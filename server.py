"""
server.py
  Socket server  → port 9999
  Flask API      → port 5001  (serves server_ui.html + /api/status + /api/drop-config)

Drop control (set from the server UI before starting a transfer):
  mode = "random"  → drop each chunk with probability `drop_rate`
  mode = "manual"  → drop exactly the sequence numbers listed in `manual_drops`
  mode = "none"    → no drops at all

The UI lets you pick the mode and configure it before the client sends a file.
Settings are preserved across transfers until changed.
"""

import socket
import struct
import random
import threading
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from utils import split_into_chunks, compute_checksum

HOST        = "127.0.0.1"
SOCKET_PORT = 9999
FLASK_PORT  = 5001

# ── Drop config (written by /api/drop-config, read by socket thread) ──────────
drop_config = {
    "mode":         "random",   # "random" | "manual" | "none"
    "drop_rate":    0.25,        # used when mode == "random"
    "manual_drops": [],          # list of seq numbers to drop when mode == "manual"
}
config_lock = threading.Lock()

# ── Transfer state ─────────────────────────────────────────────────────────────
state = {
    "status":            "WAITING",
    "client_addr":       None,
    "file_size":         0,
    "total_chunks":      0,
    "chunks_sent":       [],
    "dropped_chunks":    [],
    "retransmit_rounds": 0,
    "checksum":          None,
    "drop_config":       dict(drop_config),
    "logs":              [],
}
state_lock = threading.Lock()

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}]  {msg}"
    with state_lock:
        state["logs"].append(entry)
    print(entry)

def set_state(**kwargs):
    with state_lock:
        state.update(kwargs)


# ── Flask ──────────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=".")

@app.route("/")
def index():
    return send_from_directory(".", "server_ui.html")

@app.route("/api/status")
def api_status():
    with state_lock:
        return jsonify(dict(state))

@app.route("/api/drop-config", methods=["POST"])
def api_drop_config():
    """
    Called by the server UI control panel.
    Body JSON:
      { "mode": "random",  "drop_rate": 0.3 }
      { "mode": "manual",  "manual_drops": [0, 3, 7] }
      { "mode": "none" }
    """
    body = request.get_json(force=True)
    with config_lock:
        if "mode" in body:
            drop_config["mode"] = body["mode"]
        if "drop_rate" in body:
            drop_config["drop_rate"] = float(body["drop_rate"])
        if "manual_drops" in body:
            drop_config["manual_drops"] = [int(x) for x in body["manual_drops"]]
    with state_lock:
        state["drop_config"] = dict(drop_config)
    log(f"Drop config updated: {drop_config}")
    return jsonify({"ok": True, "config": dict(drop_config)})

@app.route("/api/reset", methods=["POST"])
def api_reset():
    with state_lock:
        state.update({
            "status": "WAITING", "client_addr": None,
            "file_size": 0, "total_chunks": 0,
            "chunks_sent": [], "dropped_chunks": [],
            "retransmit_rounds": 0, "checksum": None,
            "drop_config": dict(drop_config), "logs": [],
        })
    return jsonify({"ok": True})


# ── Socket helpers ─────────────────────────────────────────────────────────────
def recv_all(conn, length):
    data = b""
    while len(data) < length:
        pkt = conn.recv(length - len(data))
        if not pkt:
            break
        data += pkt
    return data

def should_drop(seq_no: int, round_num: int) -> bool:
    """
    Decide whether to drop a chunk.
    Only drops on round 1 — retransmits are never dropped.
    """
    if round_num > 1:
        return False
    with config_lock:
        mode = drop_config["mode"]
        if mode == "none":
            return False
        if mode == "random":
            return random.random() < drop_config["drop_rate"]
        if mode == "manual":
            return seq_no in drop_config["manual_drops"]
    return False


# ── Transfer logic ─────────────────────────────────────────────────────────────
def receive_file(conn):
    file_size = struct.unpack(">I", recv_all(conn, 4))[0]
    set_state(file_size=file_size, status="RECEIVING")
    log(f"Expecting {file_size} bytes from client")
    file_data = recv_all(conn, file_size)
    log(f"File received ({len(file_data)} bytes)")
    return file_data


def send_chunks_with_retransmit(conn, file_data):
    chunks    = split_into_chunks(file_data)
    total     = len(chunks)
    chunk_map = {seq: data for seq, data in chunks}

    conn.sendall(struct.pack(">I", total))
    set_state(total_chunks=total, status="SENDING")

    with config_lock:
        cfg = dict(drop_config)

    if cfg["mode"] == "random":
        log(f"Drop mode: RANDOM  ({int(cfg['drop_rate']*100)}% per chunk)")
    elif cfg["mode"] == "manual":
        log(f"Drop mode: MANUAL  (dropping seq: {cfg['manual_drops']})")
    else:
        log("Drop mode: NONE  (all chunks will be sent)")

    log(f"File split into {total} chunks of up to 1024 bytes")

    to_send   = list(chunks)
    random.shuffle(to_send)
    round_num = 0

    while True:
        round_num += 1
        set_state(retransmit_rounds=round_num)

        if round_num == 1:
            log(f"Round 1: sending all {total} chunks (shuffled)")
        else:
            log(f"Round {round_num}: retransmitting {len(to_send)} missing chunk(s)")

        for seq_no, data in to_send:
            if should_drop(seq_no, round_num):
                log(f"  [DROP] Chunk #{seq_no} dropped")
                with state_lock:
                    state["dropped_chunks"].append(seq_no)
                continue
            header = struct.pack(">II", seq_no, len(data))
            conn.sendall(header + data)
            with state_lock:
                state["chunks_sent"].append(seq_no)
            log(f"  Sent chunk #{seq_no} ({len(data)} B)")

        conn.sendall(b"END_")
        log(f"Round {round_num} done — waiting for ACK/NACK")

        missing_count = struct.unpack(">I", recv_all(conn, 4))[0]
        if missing_count == 0:
            log(f"ACK received — all chunks delivered in {round_num} round(s)")
            break

        missing  = [struct.unpack(">I", recv_all(conn, 4))[0] for _ in range(missing_count)]
        log(f"NACK — {missing_count} missing: {missing}")
        to_send  = [(seq, chunk_map[seq]) for seq in missing]


def send_checksum(conn, file_data):
    checksum = compute_checksum(file_data)
    conn.sendall(checksum.encode())
    set_state(checksum=checksum, status="DONE")
    log(f"Checksum sent: {checksum[:20]}...")


# ── Client handler ─────────────────────────────────────────────────────────────
def handle_client(conn, addr):
    set_state(
        status="CONNECTED", client_addr=str(addr),
        chunks_sent=[], dropped_chunks=[], retransmit_rounds=0,
        checksum=None, file_size=0, total_chunks=0,
        drop_config=dict(drop_config),
    )
    log(f"Client connected: {addr}")
    try:
        file_data = receive_file(conn)
        send_chunks_with_retransmit(conn, file_data)
        send_checksum(conn, file_data)
    except Exception as e:
        log(f"ERROR: {e}")
        set_state(status="ERROR")
    finally:
        conn.close()
        log("Connection closed")


# ── Socket server ──────────────────────────────────────────────────────────────
def run_socket_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, SOCKET_PORT))
    srv.listen(1)
    log(f"Socket server listening on {HOST}:{SOCKET_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ── Entry ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=run_socket_server, daemon=True).start()
    print(f"\n  Server UI →  http://localhost:{FLASK_PORT}\n")
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)
"""
client_web.py
  Flask web app  → port 5002  (serves client_ui.html + /api/transfer)

What's new vs the basic version:
  - Retransmission loop: after each round, client checks for missing sequence
    numbers and sends NACK back to the server with the missing list.
    Continues until all chunks are present, then sends ACK.
  - Transfer speed: measures total bytes received vs elapsed time (bytes/sec)
  - retransmit_rounds tracked in state for the UI to display
"""

import socket
import struct
import threading
import os
import time
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from utils import reassemble_chunks, compute_checksum

HOST        = "127.0.0.1"
SOCKET_PORT = 9999
FLASK_PORT  = 5002

# ── Shared state ───────────────────────────────────────────────────────────────
transfer_state = {
    "status":             "IDLE",
    "filename":           None,
    "file_size":          0,
    "total_chunks":       0,
    "chunks_received":    [],
    "retransmit_rounds":  0,
    "expected_checksum":  None,
    "actual_checksum":    None,
    "verified":           None,
    "output_file":        None,
    "transfer_speed":     None,   # bytes/sec
    "transfer_time":      None,   # seconds
    "logs":               [],
}
state_lock = threading.Lock()

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}]  {msg}"
    with state_lock:
        transfer_state["logs"].append(entry)
    print(entry)

def set_state(**kwargs):
    with state_lock:
        transfer_state.update(kwargs)


# ── Flask ──────────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=".")

@app.route("/")
def index():
    return send_from_directory(".", "client_ui.html")

@app.route("/api/transfer", methods=["POST"])
def api_transfer():
    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400
    f        = request.files["file"]
    filename = f.filename
    data     = f.read()
    t = threading.Thread(target=run_transfer, args=(data, filename), daemon=True)
    t.start()
    return jsonify({"ok": True, "filename": filename, "size": len(data)})

@app.route("/api/status")
def api_status():
    with state_lock:
        return jsonify(dict(transfer_state))

@app.route("/api/reset", methods=["POST"])
def api_reset():
    set_state(
        status="IDLE", filename=None, file_size=0,
        total_chunks=0, chunks_received=[], retransmit_rounds=0,
        expected_checksum=None, actual_checksum=None,
        verified=None, output_file=None,
        transfer_speed=None, transfer_time=None, logs=[],
    )
    return jsonify({"ok": True})


# ── Socket helpers ─────────────────────────────────────────────────────────────
def recv_all(sock, length):
    data = b""
    while len(data) < length:
        pkt = sock.recv(length - len(data))
        if not pkt:
            break
        data += pkt
    return data


# ── Core transfer ──────────────────────────────────────────────────────────────
def run_transfer(file_bytes: bytes, filename: str):
    set_state(
        status="CONNECTING", filename=filename,
        file_size=len(file_bytes), total_chunks=0,
        chunks_received=[], retransmit_rounds=0,
        expected_checksum=None, actual_checksum=None,
        verified=None, output_file=None,
        transfer_speed=None, transfer_time=None, logs=[],
    )

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, SOCKET_PORT))
        log(f"Connected to server {HOST}:{SOCKET_PORT}")

        # ── 1. Send file ───────────────────────────────────────────────────────
        set_state(status="SENDING")
        sock.sendall(struct.pack(">I", len(file_bytes)))
        sock.sendall(file_bytes)
        log(f"Sent {len(file_bytes)} bytes to server")

        # ── 2. Read total chunk count ──────────────────────────────────────────
        set_state(status="RECEIVING")
        total = struct.unpack(">I", recv_all(sock, 4))[0]
        set_state(total_chunks=total)
        log(f"Server will send {total} chunks (with drop simulation)")

        # ── 3. Retransmission loop ─────────────────────────────────────────────
        chunk_dict  = {}
        round_num   = 0
        start_time  = time.time()

        while True:
            round_num += 1
            set_state(retransmit_rounds=round_num)

            # Receive chunks until END_ signal
            while True:
                header = recv_all(sock, 4)
                if header == b"END_":
                    break
                seq_no    = struct.unpack(">I", header)[0]
                chunk_len = struct.unpack(">I", recv_all(sock, 4))[0]
                chunk     = recv_all(sock, chunk_len)
                chunk_dict[seq_no] = chunk
                with state_lock:
                    transfer_state["chunks_received"].append(seq_no)
                log(f"  Received chunk #{seq_no} ({chunk_len} B)")

            # Check which sequence numbers are missing
            all_seqs  = set(range(total))
            got_seqs  = set(chunk_dict.keys())
            missing   = sorted(all_seqs - got_seqs)

            if not missing:
                # ACK — send 0 to signal all chunks received
                sock.sendall(struct.pack(">I", 0))
                log(f"ACK sent — all {total} chunks received after {round_num} round(s)")
                break
            else:
                # NACK — send count + each missing seq number
                log(f"Round {round_num}: missing {len(missing)} chunk(s): {missing}, sending NACK")
                sock.sendall(struct.pack(">I", len(missing)))
                for seq in missing:
                    sock.sendall(struct.pack(">I", seq))

        # ── 4. Receive checksum ────────────────────────────────────────────────
        expected = recv_all(sock, 64).decode()
        elapsed  = time.time() - start_time
        speed    = int(len(file_bytes) / elapsed) if elapsed > 0 else 0
        set_state(
            expected_checksum=expected,
            transfer_time=round(elapsed, 2),
            transfer_speed=speed,
        )
        log(f"Checksum received: {expected[:20]}...")
        log(f"Transfer time: {elapsed:.2f}s  |  Speed: {speed:,} bytes/sec")
        sock.close()

        # ── 5. Reassemble ──────────────────────────────────────────────────────
        set_state(status="VERIFYING")
        log("Reassembling chunks in sequence order...")
        reassembled = reassemble_chunks(chunk_dict)

        # ── 6. Verify ──────────────────────────────────────────────────────────
        actual = compute_checksum(reassembled)
        set_state(actual_checksum=actual)

        if actual == expected:
            out_name = "received_" + filename
            os.makedirs("received", exist_ok=True)
            with open(os.path.join("received", out_name), "wb") as fout:
                fout.write(reassembled)
            set_state(status="SUCCESS", verified=True, output_file=out_name)
            log(f"VERIFIED — checksums match. Saved as {out_name}")
            log("Transfer Successful")
        else:
            set_state(status="FAILED", verified=False)
            log("CHECKSUM MISMATCH — file integrity check failed")

    except Exception as e:
        log(f"ERROR: {e}")
        set_state(status="ERROR")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n  Client UI →  http://localhost:{FLASK_PORT}\n")
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)
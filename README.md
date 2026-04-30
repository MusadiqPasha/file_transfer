# File Transfer Protocol — BigEndian Challenge 1

A complete client-server file transfer system built from scratch using raw TCP sockets. Implements chunking, sequence numbers, out-of-order delivery simulation, SHA-256 integrity verification, packet drop simulation (random and manual), and automatic retransmission via ACK/NACK — with live web dashboards for both server and client.

---

## Features

| Feature | Details |
|---|---|
| TCP socket communication | Raw `socket` module, no libraries |
| 1024-byte chunking | Fixed-size chunks with sequence numbers |
| Out-of-order simulation | Server shuffles chunks before sending |
| SHA-256 checksum | Computed server-side, verified client-side |
| Packet drop simulation | **Random** (configurable %) or **Manual** (pick exact chunks) |
| ACK / NACK retransmission | Client detects missing chunks, sends NACK with missing seq numbers, server resends — loops until all chunks arrive |
| Transfer speed + timing | Measured end-to-end including all retransmit rounds |
| Live web dashboards | Real-time monitor for both server and client |

---

## Project Structure

```
file_transfer/
├── server.py         # TCP socket server (port 9999) + Flask API (port 5001)
├── client_web.py     # Flask web app (port 5002) + TCP socket client
├── utils.py          # Shared: split_into_chunks, reassemble_chunks, compute_checksum
├── server_ui.html    # Live server dashboard + drop control panel
├── client_ui.html    # File upload dashboard + transfer progress
└── README.md
```

---

## How to Run

### 1. Install dependencies

```bash
pip install flask
```

### 2. Terminal 1 — Start the server

```bash
python server.py
```

Open **http://localhost:5001** → server dashboard loads.

### 3. Terminal 2 — Start the client

```bash
python client_web.py
```

Open **http://localhost:5002** → client dashboard loads.

### 4. (Optional) Configure packet drops before transferring

On the **server dashboard** (http://localhost:5001), use the **Packet Drop Control** panel on the left:

- **None** — no drops, clean transfer
- **Random** — drag the slider to set drop probability (0–90%), every chunk has that % chance of being dropped on round 1
- **Manual** — type specific sequence numbers (e.g. `0`, `3`, `7`), click Add → those exact chunks will be dropped

Click **Apply Settings** before the client sends a file.

### 5. Transfer a file

- Go to **http://localhost:5002**
- Drop or select any file
- Click **Send File →**
- Watch both dashboards update live

---

## Screenshots

### Client Dashboard
![Client UI](./client_ui.png)

### Server Dashboard
![Server UI](./server_ui.png)

---

## Full Protocol — With Retransmission

```
CLIENT                                  SERVER
------                                  ------
1. Send: [4B file_size][file_data]
                                        2. Receive file
                                        3. split_into_chunks() → 1024B chunks
                                        4. Build chunk_map {seq: data}
                                        5. Shuffle chunks (out-of-order sim)
                                        6. Send: [4B total_chunks]

──── ROUND 1 (and retransmit rounds) ────────────────────────────

                                        7. For each chunk:
                                           should_drop(seq, round)?
                                           → YES: skip (log [DROP])
                                           → NO:  send [4B seq][4B len][data]
                                        8. Send: b"END_"

9. Collect chunks → chunk_dict {seq: data}
10. missing = set(range(total)) - set(chunk_dict.keys())

    If missing is empty:
11a. Send: [4B: 0]  ← ACK
                                        12a. Receive 0 → break loop

    If missing chunks exist:
11b. Send: [4B: count][count × 4B seq_nos]  ← NACK
                                        12b. Receive NACK
                                        13.  Rebuild to_send from chunk_map
                                             (retransmits never dropped)
                                        → back to step 7

──── ALL CHUNKS RECEIVED ────────────────────────────────────────

                                        14. compute_checksum(original_file)
                                        15. Send: [64B SHA-256 hex string]

16. recv_all(sock, 64) → expected checksum
17. reassemble_chunks(chunk_dict)
    → sorted(keys) → b"".join(chunks)
18. compute_checksum(reassembled) → actual
19. actual == expected?
    → YES: save to received/, print "Transfer Successful"
    → NO:  print "CHECKSUM MISMATCH"
```

---

## Design Decisions

### Why TCP?
TCP guarantees delivery and byte ordering at the network layer. We still implement out-of-order handling at the **application layer** (by shuffling chunks ourselves) to demonstrate sequence-number-based reassembly — the same technique used in real streaming protocols.

### Why `recv_all()`?
TCP is a stream protocol — it does not preserve message boundaries. Sending 1024 bytes may arrive as 512 + 512 across two `recv()` calls. `recv_all()` loops until exactly the requested number of bytes has been collected.

### Why `struct.pack(">I", n)`?
Integers must be serialized to bytes to be sent over a socket. `">I"` means big-endian unsigned 32-bit integer — "network byte order", the universal convention for all major network protocols (RFC 1700). The receiver uses `struct.unpack` to reconstruct the integer.

### Why a separate `chunk_map` dictionary?
```python
chunk_map = {seq: data for seq, data in chunks}
```
In retransmit rounds the server receives a NACK with a list of missing sequence numbers and must look them up instantly. A dict gives O(1) lookup vs O(n) scan of a list.

### Why two separate state dicts with two separate locks?
`drop_config` is written by the Flask thread (UI Apply button) and read by the socket thread. `state` is written by the socket thread and read by Flask for status polls. Separate locks mean one thread writing drop config never blocks another thread reading transfer state — better concurrency and cleaner separation of concerns.

### Why retransmits are never dropped
```python
def should_drop(seq_no, round_num):
    if round_num > 1:
        return False
```
Guarantees the retransmission loop always terminates. In real networks retransmits can also fail, but for a demo system we guarantee convergence.

---

## Example Output

**Server terminal (with 25% random drop):**
```
[10:42:01]  Socket server listening on 127.0.0.1:9999
[10:42:05]  Client connected: ('127.0.0.1', 52341)
[10:42:05]  File received (3072 bytes)
[10:42:05]  Drop mode: RANDOM (25% per chunk)
[10:42:05]  File split into 3 chunks
[10:42:05]  Round 1: sending all 3 chunks (shuffled)
[10:42:05]    Sent chunk #2 (1024 B)
[10:42:05]    [DROP] Chunk #0 dropped
[10:42:05]    Sent chunk #1 (1024 B)
[10:42:05]  Round 1 done — waiting for ACK/NACK
[10:42:05]  NACK — 1 missing: [0]
[10:42:05]  Round 2: retransmitting 1 missing chunk(s)
[10:42:05]    Sent chunk #0 (1024 B)
[10:42:05]  ACK received — all chunks delivered in 2 round(s)
[10:42:05]  Checksum sent: 3b4c1d8eaf29c7...
```

**Client terminal:**
```
[10:42:05]  Connected to server 127.0.0.1:9999
[10:42:05]  Sent 3072 bytes to server
[10:42:05]  Server will send 3 chunks (with drop simulation)
[10:42:05]    Received chunk #2 (1024 B)
[10:42:05]    Received chunk #1 (1024 B)
[10:42:05]  Round 1: missing 1 chunk(s): [0], sending NACK
[10:42:05]    Received chunk #0 (1024 B)
[10:42:05]  ACK sent — all 3 chunks received after 2 round(s)
[10:42:05]  Checksum received: 3b4c1d8eaf29c7...
[10:42:05]  Reassembling chunks in sequence order...
[10:42:05]  VERIFIED — checksums match. Saved as received_sample.txt
[10:42:05]  Transfer Successful
```

---

## What Each File Does

| File | Responsibility |
|---|---|
| `utils.py` | `split_into_chunks()` — splits bytes into 1024B chunks with seq numbers. `reassemble_chunks()` — sorts by seq number and joins. `compute_checksum()` — SHA-256 hex digest. |
| `server.py` | Receives file over TCP, chunks it, simulates drops (random/manual), sends chunks shuffled, handles NACK/retransmit loop, sends checksum. Flask API exposes status + drop config to the dashboard. |
| `client_web.py` | Flask web app receives file upload from browser, runs socket client in a thread, implements NACK/retransmit loop, reassembles, verifies checksum. Flask API exposes transfer state to dashboard. |
| `server_ui.html` | Drop control panel (mode tabs, rate slider, manual chunk picker). Live stats: file size, chunks sent, dropped, retransmit rounds. Chunk grid (white = sent, red = dropped). Scrollable log. |
| `client_ui.html` | File drop zone + Send button. Progress bar. Checksum comparison panel (both boxes turn green on match). Chunk arrival visualizer (shows shuffled order). Transfer speed + time. Log. |

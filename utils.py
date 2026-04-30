import hashlib

CHUNK_SIZE = 1024  # bytes per chunk


def split_into_chunks(data: bytes) -> list[tuple[int, bytes]]:
    chunks = []
    seq = 0
    for offset in range(0, len(data), CHUNK_SIZE):
        chunk = data[offset : offset + CHUNK_SIZE]
        chunks.append((seq, chunk))
        seq += 1
    return chunks


def reassemble_chunks(chunk_dict: dict) -> bytes:
    sorted_keys = sorted(chunk_dict.keys())
    return b"".join(chunk_dict[k] for k in sorted_keys)


def compute_checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

#!/usr/bin/env python3
"""Exercise bounded-queue overload handling with deliberately stalled clients.

Run only against the local learning server. The default connection count is
larger than the server's 1,024 queued-connection limit plus its workers.
"""
import argparse
import selectors
import socket
import time


def main():
    parser = argparse.ArgumentParser(description="Test HTTP 503 overload handling")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--connections", type=int, default=1400)
    parser.add_argument("--stalled-clients", type=int, default=8,
                        help="number of initial clients that send incomplete headers")
    parser.add_argument("--read-timeout", type=float, default=12.0)
    args = parser.parse_args()
    if args.connections < 1 or args.read_timeout <= 0 or args.stalled_clients < 1:
        parser.error("connections, stalled-clients, and read-timeout must be positive")

    sockets = []
    connect_errors = 0
    status_counts = {}
    started = time.perf_counter()
    deadline = started + args.read_timeout
    try:
        for index in range(args.connections):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            try:
                sock.connect((args.host, args.port))
                if index < args.stalled_clients:
                    # These few clients occupy workers until SO_RCVTIMEO fires.
                    sock.sendall(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nX-Stall: 1")
                else:
                    sock.sendall(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                sock.setblocking(False)
                sockets.append(sock)
            except OSError:
                connect_errors += 1
                sock.close()

        print(f"Connected clients: {len(sockets)} / {args.connections}")
        print(f"Intentionally stalled clients: {min(args.stalled_clients, len(sockets))}")
        print("Collecting responses concurrently...")
        waiting = set(sockets)
        with selectors.DefaultSelector() as selector:
            for sock in waiting:
                selector.register(sock, selectors.EVENT_READ)
            while waiting and time.perf_counter() < deadline:
                remaining = max(0.0, deadline - time.perf_counter())
                for key_info, _ in selector.select(min(0.25, remaining)):
                    sock = key_info.fileobj
                    try:
                        data = sock.recv(4096)
                        first_line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
                        if first_line.startswith("HTTP/"):
                            parts = first_line.split()
                            key = f"{parts[1]} {parts[2]}" if len(parts) >= 3 else first_line
                        else:
                            key = "connection closed without HTTP response"
                    except OSError:
                        key = "socket error"
                    status_counts[key] = status_counts.get(key, 0) + 1
                    waiting.discard(sock)
                    selector.unregister(sock)
                    sock.close()

        if waiting:
            status_counts["no response before test deadline"] = len(waiting)
    finally:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass

    elapsed = time.perf_counter() - started
    print(f"Connection errors: {connect_errors}")
    print(f"Elapsed: {elapsed:.2f} s")
    print("Responses:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")
    rejected = sum(count for status, count in status_counts.items() if status.startswith("503 "))
    print(f"Observed HTTP 503 responses: {rejected}")
    if rejected == 0:
        print("No 503 observed. Check /stats and consider increasing --connections.")


if __name__ == "__main__":
    main()

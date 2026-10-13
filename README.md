# High-Performance Multithreaded HTTP Server

A C++17 learning project using POSIX TCP sockets, a fixed worker thread pool, a bounded connection queue, and a thread-safe in-memory LRU response cache.

## Features

- HTTP/1.1 `GET` requests; one request per connection (`Connection: close`)
- Fixed-size worker pool and bounded queue (1,024 queued connections)
- HTTP `503 Service Unavailable` when the queue is full
- Two-second socket receive/send timeouts to prevent stalled clients from occupying workers indefinitely
- Thread-safe LRU response cache with hit/miss counters
- `/`, `/hello`, `/time`, and `/stats` endpoints
- Atomic server counters, including active workers, queued connections, and rejected connections
- Python load tester with bounded in-flight work, status-code counts, warm-up, repeated runs, and latency percentiles
- Graceful shutdown on Ctrl+C

## Requirements

- macOS or Linux
- C++17 compiler (`clang++` or `g++`)
- Python 3 for load testing

This uses POSIX sockets and is not a Windows-native implementation.

## Build on macOS without CMake

From the project directory:

```bash
clang++ -std=c++17 -Wall -Wextra -Wpedantic -pthread src/main.cpp -o web_server
```

## Run

```bash
./web_server 8080 8
```

Arguments are port and worker count. Defaults are port `8080` and four workers. The server binds to `127.0.0.1` only.

In another terminal, try:

```bash
curl -i http://127.0.0.1:8080/
curl -i http://127.0.0.1:8080/hello
curl -i http://127.0.0.1:8080/time
curl -i http://127.0.0.1:8080/stats
```

## Benchmark

Run a single test:

```bash
python3 scripts/load_test.py --requests 10000 --concurrency 100
```

Run three timed trials with 20 warm-up requests before each set of trials:

```bash
python3 scripts/load_test.py --requests 10000 --concurrency 100 --runs 3
```

Try concurrency levels such as 50, 100, 250, and 500. Keep the worker count fixed while comparing concurrency, then restart the server with different worker counts to compare worker configurations. Run one benchmark at a time. The script counts HTTP 503 responses as non-200 results and prints status-code counts separately from connection errors.

Latency is measured from the start of each request call through receipt of its response body. The load tester keeps at most `--concurrency` futures in flight rather than pre-creating a future for every request. Results still include client-side HTTP/TCP overhead and depend on the machine and test conditions.

## Testing bounded-queue overload handling

This test deliberately opens many local connections and sends incomplete headers so workers remain occupied until their two-second receive timeout. It is intended to exceed the configured queue capacity and observe HTTP 503 responses. Run it only against your local development server:

```bash
python3 scripts/overload_test.py --connections 1400
```

Then inspect the counters:

```bash
curl -s http://127.0.0.1:8080/stats
```

The exact number of 503 responses depends on the OS socket limits and timing. If the test cannot open enough sockets, reduce the count to inspect behavior or check your process file-descriptor limit. The normal load tester treats HTTP 503 as a non-200 response and prints status-code counts.

## Testing socket timeouts

A client that connects but sends no complete HTTP request should not occupy a worker indefinitely. Socket receive/send timeouts are set to two seconds. To manually test a stalled request, run this in a separate terminal while the server is running:

```bash
python3 - <<'PY'
import socket, time
s = socket.create_connection(("127.0.0.1", 8080))
s.sendall(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n")
time.sleep(3)
try:
    print(s.recv(4096).decode(errors="replace"))
except OSError as exc:
    print("Socket closed or timed out:", exc)
s.close()
PY
```

## Endpoints

- `GET /` — project overview
- `GET /hello` — small static response (cacheable)
- `GET /time` — current server time (not cached)
- `GET /stats` — request and cache counters, active workers, queue depth, and rejected connections
- Other paths — `404 Not Found`

## Limitations

This is a learning server, not production-ready. It does not implement TLS, persistent connections, chunked request bodies, a complete HTTP parser, or production-grade graceful shutdown/overload management. Keep it on localhost while learning.

#!/usr/bin/env python3
"""Bounded-concurrency HTTP benchmark for the local C++ server."""
import argparse
import concurrent.futures
import statistics
import time
import urllib.error
import urllib.request


def percentile(values, percent):
    if not values:
        return 0.0
    ordered = sorted(values)
    # Nearest-rank percentile, a common convention for latency reporting.
    rank = max(1, int((percent / 100.0) * len(ordered) + 0.999999))
    return ordered[min(len(ordered) - 1, rank - 1)]


def make_request(url, timeout):
    start = time.perf_counter()
    request = urllib.request.Request(
        url,
        headers={"Connection": "close", "User-Agent": "cpp-server-load-test/2.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            elapsed_ms = (time.perf_counter() - start) * 1000
            return response.status, elapsed_ms, len(body), ""
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read()
        except OSError:
            body = b""
        elapsed_ms = (time.perf_counter() - start) * 1000
        return exc.code, elapsed_ms, len(body), f"HTTP {exc.code}: {exc.reason}"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return 0, elapsed_ms, 0, str(exc)


def run_benchmark(url, count, concurrency, timeout):
    latencies = []
    success_latencies = []
    status_counts = {}
    failures = 0
    bytes_received = 0
    sample_errors = []
    start = time.perf_counter()

    # Keep at most `concurrency` futures in flight instead of allocating one
    # Future per request. This makes client-side scheduling overhead smaller.
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        pending = set()
        submitted = 0
        while submitted < min(count, concurrency):
            pending.add(executor.submit(make_request, url, timeout))
            submitted += 1

        while pending:
            done, pending = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                status, elapsed_ms, size, error = future.result()
                latencies.append(elapsed_ms)
                status_counts[status] = status_counts.get(status, 0) + 1
                if status == 200:
                    success_latencies.append(elapsed_ms)
                    bytes_received += size
                else:
                    failures += 1
                    if error and len(sample_errors) < 5:
                        sample_errors.append(error)

                if submitted < count:
                    pending.add(executor.submit(make_request, url, timeout))
                    submitted += 1

    duration = time.perf_counter() - start
    return {
        "requested": count,
        "completed": len(latencies),
        "concurrency": concurrency,
        "duration": duration,
        "throughput": len(latencies) / duration if duration else 0.0,
        "successes": len(success_latencies),
        "failures": failures,
        "status_counts": status_counts,
        "bytes": bytes_received,
        "latencies": latencies,
        "successful_latencies": success_latencies,
        "errors": sample_errors,
    }


def print_result(result, url):
    print("\n=== C++ HTTP Server Load Test ===")
    print(f"URL:                 {url}")
    print(f"Total requests:      {result['requested']}")
    print(f"Concurrency:         {result['concurrency']}")
    print(f"Completed:           {result['completed']}")
    print(f"HTTP 200 responses:  {result['successes']}")
    print(f"Non-200/errors:      {result['failures']}")
    print(f"Status counts:       {dict(sorted(result['status_counts'].items()))}")
    print(f"Total time:          {result['duration']:.3f} s")
    print(f"Throughput:          {result['throughput']:.2f} responses/s")
    print(f"Successful bytes:    {result['bytes']}")
    values = result['successful_latencies']
    if values:
        print(f"Latency mean:        {statistics.mean(values):.2f} ms")
        print(f"Latency p50:         {percentile(values, 50):.2f} ms")
        print(f"Latency p95:         {percentile(values, 95):.2f} ms")
        print(f"Latency p99:         {percentile(values, 99):.2f} ms")
    if result['errors']:
        print("Sample non-200/errors:")
        for error in result['errors']:
            print(f"  - {error}")


def main():
    parser = argparse.ArgumentParser(description="Concurrent HTTP load tester")
    parser.add_argument("--url", default="http://127.0.0.1:8080/hello")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--warmup", type=int, default=20,
                        help="successful warm-up attempts before timing (default: 20; use 0 to disable)")
    parser.add_argument("--runs", type=int, default=1,
                        help="number of timed runs; each run sends --requests (default: 1)")
    args = parser.parse_args()

    if args.requests < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("--requests and --concurrency must be positive; --timeout must be > 0")
    if args.warmup < 0 or args.runs < 1:
        parser.error("--warmup must be >= 0 and --runs must be >= 1")

    if args.warmup:
        warmup = run_benchmark(args.url, args.warmup, min(args.concurrency, args.warmup), args.timeout)
        print(f"Warm-up: {warmup['successes']}/{args.warmup} HTTP 200 responses")

    results = []
    for run_number in range(1, args.runs + 1):
        result = run_benchmark(args.url, args.requests, args.concurrency, args.timeout)
        print(f"\n--- Run {run_number}/{args.runs} ---")
        print_result(result, args.url)
        results.append(result)

    if len(results) > 1:
        throughputs = [r['throughput'] for r in results]
        p95s = [percentile(r['successful_latencies'], 95) for r in results if r['successful_latencies']]
        p99s = [percentile(r['successful_latencies'], 99) for r in results if r['successful_latencies']]
        print("\n=== Across-run summary (medians) ===")
        print(f"Median throughput:   {statistics.median(throughputs):.2f} responses/s")
        if p95s:
            print(f"Median p95 latency:  {statistics.median(p95s):.2f} ms")
        if p99s:
            print(f"Median p99 latency:  {statistics.median(p99s):.2f} ms")


if __name__ == "__main__":
    main()

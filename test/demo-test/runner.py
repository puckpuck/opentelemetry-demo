# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""
OTel Demo trace-based test runner.

Usage:
    python runner.py [--tests tests/] [--services frontend,shipping] \
                     [--jaeger http://jaeger:16686/api] \
                     [--workers 5] [--stagger 2]
"""

import argparse
import concurrent.futures
import glob
import json
import os
import re
import sys
import time
from urllib.parse import quote

import requests
import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SPAN_COMPLETION_WINDOW_S = 30   # Phase B: re-poll this many seconds after first trace found
DEFAULT_WORKERS = 5
DEFAULT_STAGGER = 2             # seconds between submitting parallel tests
DEFAULT_POLL_INTERVAL = 3
DEFAULT_POLL_ATTEMPTS = 20      # Phase A: max attempts = 60s
DEFAULT_LOOKBACK_S = 30

# ---------------------------------------------------------------------------
# Environment variable expansion
# ---------------------------------------------------------------------------

_ENV_RE = re.compile(r"\$\{([^}]+)\}")


def expand_env(value):
    """Recursively expand ${VAR} in strings, dicts, and lists."""
    if isinstance(value, str):
        def _replace(m):
            var = m.group(1)
            result = os.environ.get(var)
            if result is None:
                raise KeyError(f"Environment variable not set: {var}")
            return result
        return _ENV_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# JSON path resolver (dot notation, no extra deps)
# ---------------------------------------------------------------------------

def resolve_json_path(data, path):
    """
    Resolve a simple dot-notation JSON path against a dict.
    Supports: $.field, $.nested.field, $.arr[0].field
    Returns None if any segment is missing.
    """
    # strip leading "$." or "$"
    path = path.lstrip("$").lstrip(".")
    if not path:
        return data

    parts = []
    for segment in path.split("."):
        # handle array index e.g. items[0]
        m = re.match(r"^([^\[]+)\[(\d+)\]$", segment)
        if m:
            parts.append(m.group(1))
            parts.append(int(m.group(2)))
        else:
            parts.append(segment)

    cur = data
    for part in parts:
        if cur is None:
            return None
        if isinstance(part, int):
            if isinstance(cur, list) and part < len(cur):
                cur = cur[part]
            else:
                return None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


# ---------------------------------------------------------------------------
# Jaeger helpers
# ---------------------------------------------------------------------------

def jaeger_get(jaeger_base, path, timeout=10):
    url = jaeger_base.rstrip("/") + path
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def trace_start_us(trace):
    """Return the earliest span startTime (microseconds) in a trace summary."""
    spans = trace.get("spans", [])
    if not spans:
        return 0
    return min(s.get("startTime", 0) for s in spans)


def fetch_full_trace(jaeger_base, trace_id):
    """Fetch a complete trace (all spans + processes) by ID."""
    data = jaeger_get(jaeger_base, f"/traces/{quote(trace_id)}")
    traces = data.get("data", [])
    if not traces:
        return [], {}
    t = traces[0]
    return t.get("spans", []), t.get("processes", {})


def service_of_span(span, processes):
    """Resolve a span's service name via its processID."""
    pid = span.get("processID", "")
    return processes.get(pid, {}).get("serviceName", "")


def get_tag(span, key):
    """Return the value of a Jaeger tag by key, or None."""
    for tag in span.get("tags", []):
        if tag.get("key") == key:
            return tag.get("value")
    return None


# ---------------------------------------------------------------------------
# Assertion evaluation
# ---------------------------------------------------------------------------

def evaluate_assertions(spans, processes, assertions):
    """
    Evaluate a list of span assertions against collected spans.
    Returns (all_passed: bool, results: list[dict])
    """
    results = []
    all_passed = True

    for assertion in assertions:
        name = assertion.get("name", assertion.get("operation", "?"))
        operation = assertion.get("operation", "")
        service_filter = assertion.get("service")
        min_spans = assertion.get("min_spans", 1)
        span_attributes = assertion.get("span_attributes", {})

        # Filter spans by operation (substring match)
        matching = [
            s for s in spans
            if operation in s.get("operationName", "")
        ]

        # Further filter by service name if specified
        if service_filter:
            matching = [
                s for s in matching
                if service_of_span(s, processes) == service_filter
            ]

        if len(matching) < min_spans:
            all_passed = False
            results.append({
                "name": name,
                "passed": False,
                "reason": (
                    f"Expected >= {min_spans} span(s) matching operation "
                    f"'{operation}'"
                    + (f" on service '{service_filter}'" if service_filter else "")
                    + f", found {len(matching)}"
                ),
            })
            continue

        # Check span_attributes: at least one matching span must satisfy all attrs
        if span_attributes:
            def span_matches_attrs(span):
                return all(
                    str(get_tag(span, k)) == str(v)
                    for k, v in span_attributes.items()
                )
            attr_ok = any(span_matches_attrs(s) for s in matching)
            if not attr_ok:
                all_passed = False
                # Build a helpful diagnostic showing what was found
                found_attrs = {
                    k: [get_tag(s, k) for s in matching]
                    for k in span_attributes
                }
                results.append({
                    "name": name,
                    "passed": False,
                    "reason": (
                        f"No span matching '{operation}' had all required attributes "
                        f"{span_attributes}. Found: {found_attrs}"
                    ),
                })
                continue

        results.append({"name": name, "passed": True, "reason": None})

    return all_passed, results


# ---------------------------------------------------------------------------
# Response assertions
# ---------------------------------------------------------------------------

def evaluate_response_assertions(body_text, response_assertions):
    """
    Evaluate response_assertions against the HTTP response body.
    Returns (passed: bool, error: str|None)
    """
    if not response_assertions:
        return True, None

    try:
        data = json.loads(body_text)
    except json.JSONDecodeError:
        return False, f"Response body is not valid JSON: {body_text[:200]}"

    for ra in response_assertions:
        path = ra.get("json_path")
        if not path:
            continue
        value = resolve_json_path(data, path)

        if ra.get("not_empty"):
            if not value:
                return False, f"Expected {path} to be non-empty, got: {value!r}"
        if "equals" in ra:
            if str(value) != str(ra["equals"]):
                return False, (
                    f"Expected {path} == {ra['equals']!r}, got {value!r}"
                )

    return True, None


# ---------------------------------------------------------------------------
# Core test runner
# ---------------------------------------------------------------------------

def run_test(test_path, jaeger_base):
    """
    Load and execute a single test YAML. Returns a result dict.
    """
    with open(test_path) as f:
        raw = yaml.safe_load(f)

    try:
        test = expand_env(raw)
    except KeyError as e:
        return {
            "name": raw.get("name", test_path),
            "path": test_path,
            "passed": False,
            "error": str(e),
            "assertion_results": [],
            "elapsed_ms": 0,
            "jaeger_polls": 0,
        }

    name = test.get("name", test_path)
    trigger = test.get("trigger", {})
    jaeger_cfg = test.get("jaeger_query", {})
    assertions = test.get("assertions", [])

    poll_interval = jaeger_cfg.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL)
    poll_attempts = jaeger_cfg.get("poll_max_attempts", DEFAULT_POLL_ATTEMPTS)
    lookback_s = jaeger_cfg.get("lookback_seconds", DEFAULT_LOOKBACK_S)
    jaeger_service = jaeger_cfg.get("service", "")
    jaeger_operation = jaeger_cfg.get("operation")

    # ------------------------------------------------------------------
    # Step 1: Fire HTTP trigger
    # ------------------------------------------------------------------
    method = trigger.get("method", "GET").upper()
    url = trigger.get("url", "")
    headers = trigger.get("headers", {})
    body = trigger.get("body")
    expected_status = trigger.get("expected_status", 200)
    response_assertions = trigger.get("response_assertions", [])
    timeout_s = trigger.get("timeout_seconds", 10)

    if isinstance(body, dict):
        body = json.dumps(body)
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

    t_start = time.time()
    trigger_time_us = t_start * 1_000_000

    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            data=body.encode() if isinstance(body, str) else body,
            timeout=timeout_s,
        )
    except Exception as e:
        return {
            "name": name,
            "path": test_path,
            "passed": False,
            "error": f"HTTP trigger failed: {e}",
            "assertion_results": [],
            "elapsed_ms": int((time.time() - t_start) * 1000),
            "jaeger_polls": 0,
        }

    trigger_ms = int((time.time() - t_start) * 1000)

    # ------------------------------------------------------------------
    # Step 2: Check HTTP status
    # ------------------------------------------------------------------
    if expected_status is not None and resp.status_code != expected_status:
        return {
            "name": name,
            "path": test_path,
            "passed": False,
            "error": (
                f"Expected HTTP {expected_status}, got {resp.status_code}. "
                f"Body: {resp.text[:300]}"
            ),
            "assertion_results": [],
            "elapsed_ms": trigger_ms,
            "jaeger_polls": 0,
        }

    # ------------------------------------------------------------------
    # Step 3: Response body assertions
    # ------------------------------------------------------------------
    resp_ok, resp_err = evaluate_response_assertions(resp.text, response_assertions)
    if not resp_ok:
        return {
            "name": name,
            "path": test_path,
            "passed": False,
            "error": f"Response assertion failed: {resp_err}",
            "assertion_results": [],
            "elapsed_ms": trigger_ms,
            "jaeger_polls": 0,
        }

    # ------------------------------------------------------------------
    # Step 4: Jaeger polling — Phase A (trace discovery)
    # ------------------------------------------------------------------
    if not jaeger_service:
        # No Jaeger assertions configured — HTTP-only test
        return {
            "name": name,
            "path": test_path,
            "passed": True,
            "error": None,
            "assertion_results": [],
            "elapsed_ms": trigger_ms,
            "jaeger_polls": 0,
        }

    start_us = int((t_start - lookback_s) * 1_000_000)
    search_path = (
        f"/traces?service={quote(jaeger_service)}"
        f"&limit=20&start={start_us}"
    )
    if jaeger_operation:
        search_path += f"&operation={quote(jaeger_operation)}"

    found_trace_ids = []
    phase_a_polls = 0

    for attempt in range(poll_attempts):
        phase_a_polls += 1
        try:
            data = jaeger_get(jaeger_base, search_path)
        except Exception as e:
            time.sleep(poll_interval)
            continue

        traces = data.get("data", [])
        fresh = [t for t in traces if trace_start_us(t) >= trigger_time_us]
        if fresh:
            found_trace_ids = [t["traceID"] for t in fresh]
            break
        time.sleep(poll_interval)
    else:
        return {
            "name": name,
            "path": test_path,
            "passed": False,
            "error": (
                f"No trace appeared in Jaeger for service '{jaeger_service}' "
                f"after {poll_attempts * poll_interval}s"
            ),
            "assertion_results": [],
            "elapsed_ms": int((time.time() - t_start) * 1000),
            "jaeger_polls": phase_a_polls,
        }

    # ------------------------------------------------------------------
    # Step 5: Jaeger polling — Phase B (span completion wait)
    # ------------------------------------------------------------------
    phase_b_deadline = time.time() + SPAN_COMPLETION_WINDOW_S
    last_assertion_results = []
    last_all_passed = False
    phase_b_polls = 0

    while time.time() < phase_b_deadline:
        phase_b_polls += 1
        all_spans = []
        all_processes = {}

        for tid in found_trace_ids:
            try:
                spans, processes = fetch_full_trace(jaeger_base, tid)
                all_spans.extend(spans)
                all_processes.update(processes)
            except Exception:
                pass

        last_all_passed, last_assertion_results = evaluate_assertions(
            all_spans, all_processes, assertions
        )
        if last_all_passed:
            break

        # Also check for additional traces that may have arrived
        try:
            data = jaeger_get(jaeger_base, search_path)
            traces = data.get("data", [])
            fresh = [t for t in traces if trace_start_us(t) >= trigger_time_us]
            new_ids = [t["traceID"] for t in fresh if t["traceID"] not in found_trace_ids]
            found_trace_ids.extend(new_ids)
        except Exception:
            pass

        time.sleep(poll_interval)

    total_ms = int((time.time() - t_start) * 1000)
    total_polls = phase_a_polls + phase_b_polls

    return {
        "name": name,
        "path": test_path,
        "passed": last_all_passed,
        "error": None,
        "assertion_results": last_assertion_results,
        "elapsed_ms": total_ms,
        "jaeger_polls": total_polls,
    }


# ---------------------------------------------------------------------------
# Readiness checks
# ---------------------------------------------------------------------------

def wait_for_jaeger(jaeger_base, max_wait=120):
    print(f"Waiting for Jaeger at {jaeger_base} ...", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = requests.get(f"{jaeger_base}/services", timeout=5)
            if r.status_code == 200:
                print("Jaeger is ready.", flush=True)
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Jaeger did not become available within {max_wait}s")


def wait_for_frontend(frontend_addr, max_wait=120):
    if not frontend_addr:
        return
    url = f"http://{frontend_addr}/"
    print(f"Waiting for frontend at {url} ...", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                print("Frontend is ready.", flush=True)
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Frontend did not become available within {max_wait}s")


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

PASS_MARK = "PASS"
FAIL_MARK = "FAIL"


def print_result(result):
    status = PASS_MARK if result["passed"] else FAIL_MARK
    timing = f"{result['elapsed_ms']}ms, {result['jaeger_polls']} Jaeger poll(s)"
    print(f"[{status}] {result['name']} ({timing})", flush=True)

    if result.get("error"):
        print(f"       ERROR: {result['error']}", flush=True)

    for ar in result.get("assertion_results", []):
        mark = "  v" if ar["passed"] else "  x"
        print(f"{mark} {ar['name']}", flush=True)
        if not ar["passed"] and ar.get("reason"):
            print(f"       {ar['reason']}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OTel Demo trace-based test runner")
    parser.add_argument(
        "--tests", default="tests/",
        help="Directory containing test YAML files (default: tests/)"
    )
    parser.add_argument(
        "--services",
        help="Comma-separated list of service subdirectories to run (e.g. frontend,shipping)"
    )
    parser.add_argument(
        "--jaeger",
        default=os.environ.get("JAEGER_URL", "http://jaeger:16686/api"),
        help="Jaeger HTTP API base URL"
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Max parallel test workers (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--stagger", type=float, default=DEFAULT_STAGGER,
        help=f"Seconds between starting each parallel test (default: {DEFAULT_STAGGER})"
    )
    args = parser.parse_args()

    # SERVICES_TO_TEST env var acts as --services if flag not given
    services_filter = args.services or os.environ.get("SERVICES_TO_TEST")

    # Discover test files
    base = os.path.dirname(os.path.abspath(__file__))
    tests_dir = os.path.join(base, args.tests)
    all_files = sorted(glob.glob(os.path.join(tests_dir, "**", "*.yaml"), recursive=True))

    if services_filter:
        allowed = {s.strip() for s in services_filter.split(",")}
        all_files = [
            f for f in all_files
            if any(
                os.path.basename(os.path.dirname(f)) == svc
                for svc in allowed
            )
        ]

    if not all_files:
        print("No test files found.", flush=True)
        sys.exit(1)

    print(f"Found {len(all_files)} test(s).", flush=True)

    # Readiness checks
    wait_for_jaeger(args.jaeger)
    wait_for_frontend(os.environ.get("FRONTEND_ADDR"))

    print(
        f"\nRunning {len(all_files)} test(s) with up to {args.workers} workers "
        f"({args.stagger}s stagger)...\n",
        flush=True
    )

    # Submit tests with stagger
    futures_ordered = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        for i, path in enumerate(all_files):
            if i > 0:
                time.sleep(args.stagger)
            future = executor.submit(run_test, path, args.jaeger)
            futures_ordered.append(future)

        results = [f.result() for f in futures_ordered]

    # Print results in submission order
    print("\n" + "=" * 60, flush=True)
    passed = 0
    failed = 0
    for result in results:
        print_result(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

    print("=" * 60, flush=True)
    print(f"\nResults: {passed} passed, {failed} failed", flush=True)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Multi-worker Stratum load generator for blocknet-pool.

This is intended for local performance validation and A/B benchmarking.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import socket
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Stratum share load against blocknet-pool."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Pool host.")
    parser.add_argument(
        "--stratum-port", type=int, required=True, help="Pool stratum TCP port."
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=0,
        help="Optional pool API port for sampling /api/stats (0 disables sampling).",
    )
    parser.add_argument(
        "--workers", type=int, default=64, help="Number of worker connections."
    )
    parser.add_argument(
        "--duration-secs", type=float, default=60.0, help="Run duration in seconds."
    )
    parser.add_argument(
        "--shares-per-worker-per-sec",
        type=float,
        default=5.0,
        help="Share submit rate per worker connection.",
    )
    parser.add_argument(
        "--submit-mode",
        choices=("legacy", "v2"),
        default="legacy",
        help="legacy omits claimed_hash; v2 includes claimed_hash.",
    )
    parser.add_argument(
        "--response-timeout-secs",
        type=float,
        default=15.0,
        help="Socket read timeout for stratum responses.",
    )
    parser.add_argument(
        "--login-timeout-secs",
        type=float,
        default=10.0,
        help="Timeout waiting for login + first job.",
    )
    parser.add_argument(
        "--stats-sample-interval-secs",
        type=float,
        default=1.0,
        help="Sampling interval for /api/stats when --api-port is set.",
    )
    parser.add_argument(
        "--worker-prefix",
        default="bench",
        help="Prefix for worker names.",
    )
    parser.add_argument(
        "--address-prefix",
        default="bench_addr",
        help="Prefix for miner addresses used in login.",
    )
    parser.add_argument(
        "--address-mode",
        choices=("unique", "shared"),
        default="unique",
        help="Use unique address per worker or one shared address.",
    )
    parser.add_argument(
        "--shared-address",
        default="bench_shared",
        help="Address to use when --address-mode shared.",
    )
    parser.add_argument(
        "--jitter-ms",
        type=float,
        default=25.0,
        help="Start jitter window per worker in milliseconds.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output path. Printed to stdout regardless.",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    idx = (len(sorted_values) - 1) * p
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_values[lo]
    frac = idx - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def fetch_api_stats(host: str, api_port: int, timeout: float = 2.0) -> Optional[dict]:
    if api_port <= 0:
        return None
    url = f"http://{host}:{api_port}/api/stats"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            payload = resp.read()
    except (urllib.error.URLError, TimeoutError, ValueError):
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return None


class ApiSampler(threading.Thread):
    def __init__(self, host: str, api_port: int, interval_secs: float) -> None:
        super().__init__(daemon=True)
        self.host = host
        self.api_port = api_port
        self.interval_secs = max(0.1, interval_secs)
        self.stop_event = threading.Event()
        self.samples: List[dict] = []

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            snapshot = fetch_api_stats(self.host, self.api_port)
            if snapshot is not None:
                self.samples.append(
                    {
                        "time": time.time(),
                        "validation": snapshot.get("validation", {}),
                        "pool": snapshot.get("pool", {}),
                        "chain": snapshot.get("chain", {}),
                    }
                )
            self.stop_event.wait(self.interval_secs)


@dataclass
class WorkerMetrics:
    worker_id: int
    submitted: int = 0
    responses: int = 0
    accepted: int = 0
    rejected: int = 0
    transport_errors: int = 0
    protocol_errors: int = 0
    login_failed: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    error_reasons: Dict[str, int] = field(default_factory=dict)

    def add_error_reason(self, reason: str) -> None:
        key = reason.strip() or "unknown"
        self.error_reasons[key] = self.error_reasons.get(key, 0) + 1


class Worker(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        host: str,
        port: int,
        address: str,
        worker_name: str,
        submit_mode: str,
        shares_per_sec: float,
        login_timeout_secs: float,
        response_timeout_secs: float,
        stop_deadline: float,
        jitter_ms: float,
    ) -> None:
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.address = address
        self.worker_name = worker_name
        self.submit_mode = submit_mode
        self.shares_per_sec = max(0.0, shares_per_sec)
        self.login_timeout_secs = login_timeout_secs
        self.response_timeout_secs = response_timeout_secs
        self.stop_deadline = stop_deadline
        self.jitter_ms = max(0.0, jitter_ms)
        self.metrics = WorkerMetrics(worker_id=worker_id)
        self._nonce = 0
        self._nonce_start = 0
        self._nonce_end = (1 << 64) - 1
        self._job_id: Optional[str] = None

    def run(self) -> None:
        try:
            self._run_inner()
        except Exception as exc:  # pylint: disable=broad-except
            self.metrics.transport_errors += 1
            self.metrics.add_error_reason(f"worker exception: {exc}")

    def _run_inner(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=5.0) as sock:
            sock.settimeout(self.response_timeout_secs)
            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            writer = sock.makefile("w", encoding="utf-8", newline="\n")
            try:
                self._login(reader, writer)
                self._submit_loop(reader, writer)
            finally:
                try:
                    writer.close()
                except Exception:  # pylint: disable=broad-except
                    pass
                try:
                    reader.close()
                except Exception:  # pylint: disable=broad-except
                    pass

    def _send_json(self, writer, payload: dict) -> None:
        writer.write(json.dumps(payload, separators=(",", ":")) + "\n")
        writer.flush()

    def _read_json(self, reader) -> dict:
        try:
            raw = reader.readline()
        except socket.timeout as exc:
            raise TimeoutError("stratum read timeout") from exc
        if raw == "":
            raise ConnectionError("stratum connection closed")
        line = raw.strip()
        if not line:
            return {}
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid json from pool: {line}") from exc
        if not isinstance(obj, dict):
            raise ValueError("stratum payload is not an object")
        return obj

    def _update_job(self, msg: dict) -> bool:
        method = msg.get("method")
        if method != "job":
            return False
        params = msg.get("params")
        if not isinstance(params, dict):
            return True
        job_id = params.get("job_id")
        if isinstance(job_id, str) and job_id:
            self._job_id = job_id
        nonce_start = params.get("nonce_start")
        nonce_end = params.get("nonce_end")
        if isinstance(nonce_start, int) and nonce_start >= 0:
            self._nonce_start = nonce_start
        if isinstance(nonce_end, int) and nonce_end >= self._nonce_start:
            self._nonce_end = nonce_end
        self._nonce = self._nonce_start
        return True

    def _login(self, reader, writer) -> None:
        login_id = 1
        self._send_json(
            writer,
            {
                "id": login_id,
                "method": "login",
                "params": {
                    "address": self.address,
                    "worker": self.worker_name,
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash", "share_validation_status"],
                },
            },
        )

        login_ok = False
        deadline = time.monotonic() + self.login_timeout_secs
        while time.monotonic() < deadline:
            msg = self._read_json(reader)
            if self._update_job(msg):
                if login_ok and self._job_id is not None:
                    return
                continue
            msg_id = msg.get("id")
            if msg_id != login_id:
                continue
            status = msg.get("status")
            if status == "ok":
                login_ok = True
            else:
                self.metrics.login_failed += 1
                self.metrics.add_error_reason(str(msg.get("error") or "login failed"))
                return
            if login_ok and self._job_id is not None:
                return

        if not login_ok:
            self.metrics.login_failed += 1
            self.metrics.add_error_reason("login timeout")
        elif self._job_id is None:
            self.metrics.login_failed += 1
            self.metrics.add_error_reason("job notify timeout")

    def _next_nonce(self) -> int:
        nonce = self._nonce
        if nonce >= self._nonce_end:
            self._nonce = self._nonce_start
        else:
            self._nonce += 1
        return nonce

    def _submit_loop(self, reader, writer) -> None:
        if self._job_id is None:
            return

        req_id = 2
        if self.jitter_ms > 0:
            jitter = random.random() * (self.jitter_ms / 1000.0)
            time.sleep(jitter)

        period = 0.0 if self.shares_per_sec <= 0 else 1.0 / self.shares_per_sec
        next_submit = time.monotonic()

        while time.monotonic() < self.stop_deadline:
            now = time.monotonic()
            if period > 0 and now < next_submit:
                time.sleep(min(next_submit - now, 0.01))
                continue

            if self._job_id is None:
                self.metrics.protocol_errors += 1
                self.metrics.add_error_reason("missing active job")
                break

            params = {
                "job_id": self._job_id,
                "nonce": self._next_nonce(),
            }
            if self.submit_mode == "v2":
                params["claimed_hash"] = "00" * 32

            request = {
                "id": req_id,
                "method": "submit",
                "params": params,
            }
            submit_t0 = time.perf_counter()
            self._send_json(writer, request)
            self.metrics.submitted += 1

            try:
                while True:
                    msg = self._read_json(reader)
                    if self._update_job(msg):
                        continue

                    msg_id = msg.get("id")
                    if msg_id != req_id:
                        continue

                    self.metrics.responses += 1
                    self.metrics.latencies_ms.append((time.perf_counter() - submit_t0) * 1000.0)
                    if msg.get("status") == "ok":
                        self.metrics.accepted += 1
                    else:
                        self.metrics.rejected += 1
                        self.metrics.add_error_reason(str(msg.get("error") or "unknown"))
                    break
            except TimeoutError:
                self.metrics.transport_errors += 1
                self.metrics.add_error_reason("submit timeout")
            except (ConnectionError, ValueError) as exc:
                self.metrics.transport_errors += 1
                self.metrics.add_error_reason(str(exc))
                break

            req_id += 1
            if period > 0:
                next_submit += period


def merge_error_reasons(metrics: List[WorkerMetrics]) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    for item in metrics:
        for key, value in item.error_reasons.items():
            merged[key] = merged.get(key, 0) + value
    return dict(sorted(merged.items(), key=lambda kv: (-kv[1], kv[0])))


def summarize_api_samples(
    start_snapshot: Optional[dict], end_snapshot: Optional[dict], samples: List[dict]
) -> dict:
    if start_snapshot is None and end_snapshot is None and not samples:
        return {"available": False}

    max_in_flight = 0
    max_candidate_queue_depth = 0
    max_regular_queue_depth = 0
    max_pending_provisional = 0
    max_forced_verify_addresses = 0

    for sample in samples:
        val = sample.get("validation", {})
        if not isinstance(val, dict):
            continue
        max_in_flight = max(max_in_flight, int(val.get("in_flight", 0)))
        max_candidate_queue_depth = max(
            max_candidate_queue_depth, int(val.get("candidate_queue_depth", 0))
        )
        max_regular_queue_depth = max(
            max_regular_queue_depth, int(val.get("regular_queue_depth", 0))
        )
        max_pending_provisional = max(
            max_pending_provisional, int(val.get("pending_provisional", 0))
        )
        max_forced_verify_addresses = max(
            max_forced_verify_addresses, int(val.get("forced_verify_addresses", 0))
        )

    def get_nested(snapshot: Optional[dict], *keys, default=0):
        cur = snapshot
        for key in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key)
        if cur is None:
            return default
        return cur

    delta = {
        "pool_shares_accepted": int(
            get_nested(end_snapshot, "pool", "shares_accepted", default=0)
        )
        - int(get_nested(start_snapshot, "pool", "shares_accepted", default=0)),
        "pool_shares_rejected": int(
            get_nested(end_snapshot, "pool", "shares_rejected", default=0)
        )
        - int(get_nested(start_snapshot, "pool", "shares_rejected", default=0)),
        "validation_total_shares": int(
            get_nested(end_snapshot, "validation", "total_shares", default=0)
        )
        - int(get_nested(start_snapshot, "validation", "total_shares", default=0)),
        "validation_sampled_shares": int(
            get_nested(end_snapshot, "validation", "sampled_shares", default=0)
        )
        - int(get_nested(start_snapshot, "validation", "sampled_shares", default=0)),
        "validation_invalid_samples": int(
            get_nested(end_snapshot, "validation", "invalid_samples", default=0)
        )
        - int(get_nested(start_snapshot, "validation", "invalid_samples", default=0)),
        "validation_fraud_detections": int(
            get_nested(end_snapshot, "validation", "fraud_detections", default=0)
        )
        - int(get_nested(start_snapshot, "validation", "fraud_detections", default=0)),
    }

    return {
        "available": True,
        "samples": len(samples),
        "max_in_flight": max_in_flight,
        "max_candidate_queue_depth": max_candidate_queue_depth,
        "max_regular_queue_depth": max_regular_queue_depth,
        "max_pending_provisional": max_pending_provisional,
        "max_forced_verify_addresses": max_forced_verify_addresses,
        "snapshot_start": start_snapshot or {},
        "snapshot_end": end_snapshot or {},
        "delta": delta,
    }


def main() -> int:
    args = parse_args()
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.duration_secs <= 0:
        raise SystemExit("--duration-secs must be > 0")

    start_iso = now_utc_iso()
    start_monotonic = time.monotonic()
    stop_deadline = start_monotonic + args.duration_secs

    start_snapshot = fetch_api_stats(args.host, args.api_port) if args.api_port > 0 else None
    sampler: Optional[ApiSampler] = None
    if args.api_port > 0:
        sampler = ApiSampler(args.host, args.api_port, args.stats_sample_interval_secs)
        sampler.start()

    workers: List[Worker] = []
    for i in range(args.workers):
        if args.address_mode == "shared":
            address = args.shared_address
        else:
            address = f"{args.address_prefix}_{i:04d}"
        worker_name = f"{args.worker_prefix}_{i:04d}"
        worker = Worker(
            worker_id=i,
            host=args.host,
            port=args.stratum_port,
            address=address,
            worker_name=worker_name,
            submit_mode=args.submit_mode,
            shares_per_sec=args.shares_per_worker_per_sec,
            login_timeout_secs=args.login_timeout_secs,
            response_timeout_secs=args.response_timeout_secs,
            stop_deadline=stop_deadline,
            jitter_ms=args.jitter_ms,
        )
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()

    if sampler is not None:
        sampler.stop()
        sampler.join(timeout=2.0)

    end_snapshot = fetch_api_stats(args.host, args.api_port) if args.api_port > 0 else None

    elapsed = max(0.001, time.monotonic() - start_monotonic)
    all_metrics = [worker.metrics for worker in workers]

    submitted = sum(m.submitted for m in all_metrics)
    responses = sum(m.responses for m in all_metrics)
    accepted = sum(m.accepted for m in all_metrics)
    rejected = sum(m.rejected for m in all_metrics)
    transport_errors = sum(m.transport_errors for m in all_metrics)
    protocol_errors = sum(m.protocol_errors for m in all_metrics)
    login_failed = sum(m.login_failed for m in all_metrics)
    all_latencies = sorted(lat for m in all_metrics for lat in m.latencies_ms)

    latency_summary = {
        "avg": (sum(all_latencies) / len(all_latencies)) if all_latencies else 0.0,
        "p50": percentile(all_latencies, 0.50),
        "p95": percentile(all_latencies, 0.95),
        "p99": percentile(all_latencies, 0.99),
        "max": all_latencies[-1] if all_latencies else 0.0,
    }

    error_breakdown = merge_error_reasons(all_metrics)
    api_summary = summarize_api_samples(
        start_snapshot, end_snapshot, sampler.samples if sampler is not None else []
    )

    result = {
        "started_at": start_iso,
        "finished_at": now_utc_iso(),
        "duration_secs": elapsed,
        "host": args.host,
        "stratum_port": args.stratum_port,
        "api_port": args.api_port,
        "workers": args.workers,
        "submit_mode": args.submit_mode,
        "shares_per_worker_per_sec": args.shares_per_worker_per_sec,
        "load": {
            "submitted": submitted,
            "responses": responses,
            "accepted": accepted,
            "rejected": rejected,
            "transport_errors": transport_errors,
            "protocol_errors": protocol_errors,
            "login_failed": login_failed,
            "response_tps": responses / elapsed,
            "accept_tps": accepted / elapsed,
            "submit_tps": submitted / elapsed,
            "latency_ms": latency_summary,
        },
        "error_breakdown": error_breakdown,
        "api_stats": api_summary,
    }

    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(rendered)
            f.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
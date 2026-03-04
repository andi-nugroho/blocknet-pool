#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/bench_pool_ab.sh \
    --baseline-dir <path> \
    --candidate-dir <path> \
    [--daemon-api <url>] \
    [--daemon-token <token>] \
    [--pairs <n>] \
    [--duration-secs <n>] \
    [--workers <n>] \
    [--shares-per-worker-per-sec <n>] \
    [--submit-mode legacy|v2] \
    [--validation-mode full|probabilistic] \
    [--sample-rate <0..1>] \
    [--max-verifiers <n>] \
    [--max-validation-queue <n>] \
    [--stratum-submit-v2-required true|false] \
    [--cooldown-secs <n>] \
    [--profile <cargo-profile>] \
    [--base-stratum-port <n>] \
    [--base-api-port <n>] \
    [--max-conns-per-ip <n>] \
    [--run-order baseline-first|alternate] \
    [--ready-timeout-secs <n>] \
    [--output-dir <path>]

Example:
  scripts/bench_pool_ab.sh \
    --baseline-dir ../blocknet-pool-baseline \
    --candidate-dir . \
    --daemon-api http://127.0.0.1:8332 \
    --pairs 3 \
    --duration-secs 90 \
    --workers 12 \
    --shares-per-worker-per-sec 6 \
    --submit-mode legacy \
    --validation-mode full \
    --max-verifiers 8 \
    --max-validation-queue 512 \
    --cooldown-secs 20
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

baseline_dir=""
candidate_dir=""
daemon_api="http://127.0.0.1:8332"
daemon_token=""
pairs=3
duration_secs=60
workers=64
shares_per_worker_per_sec=5
submit_mode="legacy"
validation_mode="probabilistic"
sample_rate=0.05
max_verifiers=0
max_validation_queue=1024
stratum_submit_v2_required="false"
cooldown_secs=15
profile="release"
base_stratum_port=3433
base_api_port=8180
max_conns_per_ip=16
run_order="alternate"
ready_timeout_secs=45
output_dir=""

while (($#)); do
    case "$1" in
        --baseline-dir)
            baseline_dir="${2:-}"
            shift 2
            ;;
        --candidate-dir)
            candidate_dir="${2:-}"
            shift 2
            ;;
        --daemon-api)
            daemon_api="${2:-}"
            shift 2
            ;;
        --daemon-token)
            daemon_token="${2:-}"
            shift 2
            ;;
        --pairs)
            pairs="${2:-}"
            shift 2
            ;;
        --duration-secs)
            duration_secs="${2:-}"
            shift 2
            ;;
        --workers)
            workers="${2:-}"
            shift 2
            ;;
        --shares-per-worker-per-sec)
            shares_per_worker_per_sec="${2:-}"
            shift 2
            ;;
        --submit-mode)
            submit_mode="${2:-}"
            shift 2
            ;;
        --validation-mode)
            validation_mode="${2:-}"
            shift 2
            ;;
        --sample-rate)
            sample_rate="${2:-}"
            shift 2
            ;;
        --max-verifiers)
            max_verifiers="${2:-}"
            shift 2
            ;;
        --max-validation-queue)
            max_validation_queue="${2:-}"
            shift 2
            ;;
        --stratum-submit-v2-required)
            stratum_submit_v2_required="${2:-}"
            shift 2
            ;;
        --cooldown-secs)
            cooldown_secs="${2:-}"
            shift 2
            ;;
        --profile)
            profile="${2:-}"
            shift 2
            ;;
        --base-stratum-port)
            base_stratum_port="${2:-}"
            shift 2
            ;;
        --base-api-port)
            base_api_port="${2:-}"
            shift 2
            ;;
        --max-conns-per-ip)
            max_conns_per_ip="${2:-}"
            shift 2
            ;;
        --run-order)
            run_order="${2:-}"
            shift 2
            ;;
        --ready-timeout-secs)
            ready_timeout_secs="${2:-}"
            shift 2
            ;;
        --output-dir)
            output_dir="${2:-}"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$baseline_dir" || -z "$candidate_dir" ]]; then
    echo "error: --baseline-dir and --candidate-dir are required" >&2
    usage
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 is required" >&2
    exit 1
fi

if [[ ! -x "$SCRIPT_DIR/pool_load.py" ]]; then
    echo "error: missing executable $SCRIPT_DIR/pool_load.py" >&2
    exit 1
fi

if [[ "$submit_mode" != "legacy" && "$submit_mode" != "v2" ]]; then
    echo "error: --submit-mode must be legacy or v2" >&2
    exit 1
fi

if [[ "$validation_mode" != "full" && "$validation_mode" != "probabilistic" ]]; then
    echo "error: --validation-mode must be full or probabilistic" >&2
    exit 1
fi

if [[ "$run_order" != "baseline-first" && "$run_order" != "alternate" ]]; then
    echo "error: --run-order must be baseline-first or alternate" >&2
    exit 1
fi

if ((workers > max_conns_per_ip)); then
    echo "error: workers=$workers exceeds max-conns-per-ip=$max_conns_per_ip" >&2
    echo "hint: lower --workers or pass --max-conns-per-ip to match your pool limit" >&2
    exit 1
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
if [[ -z "$output_dir" ]]; then
    output_dir="data/bench_pool_ab_${validation_mode}_${timestamp}"
fi
mkdir -p "$output_dir"

results_tsv="$output_dir/results.tsv"
summary_txt="$output_dir/summary.txt"
meta_txt="$output_dir/meta.txt"

cat >"$results_tsv" <<'EOF'
pair	leg	submitted	responses	accepted	rejected	transport_errors	protocol_errors	login_failed	submit_tps	response_tps	accept_tps	latency_avg_ms	latency_p95_ms	latency_p99_ms	latency_max_ms	api_available	api_samples	max_in_flight	max_candidate_q	max_regular_q	delta_pool_accepted	delta_pool_rejected	delta_validation_total	delta_validation_sampled	delta_validation_invalid	delta_validation_fraud
EOF

cat >"$meta_txt" <<EOF
daemon_api=$daemon_api
pairs=$pairs
duration_secs=$duration_secs
workers=$workers
shares_per_worker_per_sec=$shares_per_worker_per_sec
submit_mode=$submit_mode
validation_mode=$validation_mode
sample_rate=$sample_rate
max_verifiers=$max_verifiers
max_validation_queue=$max_validation_queue
stratum_submit_v2_required=$stratum_submit_v2_required
cooldown_secs=$cooldown_secs
profile=$profile
max_conns_per_ip=$max_conns_per_ip
run_order=$run_order
baseline_dir=$baseline_dir
candidate_dir=$candidate_dir
EOF

build_pool() {
    local repo_dir="$1"
    (
        cd "$repo_dir"
        cargo build --profile "$profile" >/dev/null
    )
}

binary_path() {
    local repo_dir="$1"
    echo "$repo_dir/target/$profile/blocknet-pool-rs"
}

wait_for_pool_ready() {
    local api_port="$1"
    local timeout_secs="$2"
    python3 - "$api_port" "$timeout_secs" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

api_port = int(sys.argv[1])
timeout_secs = float(sys.argv[2])
deadline = time.time() + timeout_secs
url = f"http://127.0.0.1:{api_port}/api/stats"

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=1.5) as resp:
            data = json.load(resp)
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        time.sleep(0.5)
        continue
    chain = data.get("chain", {})
    if chain.get("current_job_height") is not None:
        sys.exit(0)
    time.sleep(0.5)

sys.exit(1)
PY
}

stop_pool() {
    local pid="$1"
    if [[ -z "$pid" ]]; then
        return
    fi
    if kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
        for _ in {1..30}; do
            if ! kill -0 "$pid" >/dev/null 2>&1; then
                break
            fi
            sleep 0.2
        done
        if kill -0 "$pid" >/dev/null 2>&1; then
            kill -9 "$pid" >/dev/null 2>&1 || true
        fi
    fi
}

write_leg_config() {
    local source_config="$1"
    local out_config="$2"
    local stratum_port="$3"
    local api_port="$4"

    python3 - "$source_config" "$out_config" \
        "$daemon_api" "$daemon_token" \
        "$stratum_port" "$api_port" \
        "$validation_mode" "$sample_rate" "$max_verifiers" "$max_validation_queue" \
        "$stratum_submit_v2_required" <<'PY'
import json
import sys

src = sys.argv[1]
dst = sys.argv[2]
daemon_api = sys.argv[3]
daemon_token = sys.argv[4]
stratum_port = int(sys.argv[5])
api_port = int(sys.argv[6])
validation_mode = sys.argv[7]
sample_rate = float(sys.argv[8])
max_verifiers = int(sys.argv[9])
max_validation_queue = int(sys.argv[10])
stratum_submit_v2_required = sys.argv[11].strip().lower() in ("1", "true", "yes", "on")

with open(src, "r", encoding="utf-8") as f:
    cfg = json.load(f)

cfg["stratum_port"] = stratum_port
cfg["api_port"] = api_port
cfg["api_host"] = "127.0.0.1"
cfg["daemon_api"] = daemon_api
cfg["daemon_token"] = daemon_token
cfg["validation_mode"] = validation_mode
cfg["sample_rate"] = sample_rate
cfg["max_verifiers"] = max_verifiers
cfg["max_validation_queue"] = max_validation_queue
cfg["stratum_submit_v2_required"] = stratum_submit_v2_required
cfg["pool_fee_flat"] = 0.0
cfg["pool_fee_pct"] = 0.0
cfg["payout_interval"] = "24h"
cfg["min_payout_amount"] = 1000000.0
cfg["blocks_before_payout"] = 1000000
cfg["block_poll_interval"] = "2s"
cfg["job_timeout"] = "30s"

with open(dst, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2, sort_keys=False)
    f.write("\n")
PY
}

append_result_row() {
    local pair="$1"
    local leg="$2"
    local result_json="$3"
    python3 - "$pair" "$leg" "$result_json" >>"$results_tsv" <<'PY'
import json
import sys

pair = sys.argv[1]
leg = sys.argv[2]
path = sys.argv[3]

with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

load = data.get("load", {})
lat = load.get("latency_ms", {})
api = data.get("api_stats", {})
delta = api.get("delta", {}) if isinstance(api, dict) else {}

row = [
    pair,
    leg,
    str(load.get("submitted", 0)),
    str(load.get("responses", 0)),
    str(load.get("accepted", 0)),
    str(load.get("rejected", 0)),
    str(load.get("transport_errors", 0)),
    str(load.get("protocol_errors", 0)),
    str(load.get("login_failed", 0)),
    f'{float(load.get("submit_tps", 0.0)):.6f}',
    f'{float(load.get("response_tps", 0.0)):.6f}',
    f'{float(load.get("accept_tps", 0.0)):.6f}',
    f'{float(lat.get("avg", 0.0)):.6f}',
    f'{float(lat.get("p95", 0.0)):.6f}',
    f'{float(lat.get("p99", 0.0)):.6f}',
    f'{float(lat.get("max", 0.0)):.6f}',
    "1" if api.get("available") else "0",
    str(api.get("samples", 0)),
    str(api.get("max_in_flight", 0)),
    str(api.get("max_candidate_queue_depth", 0)),
    str(api.get("max_regular_queue_depth", 0)),
    str(delta.get("pool_shares_accepted", 0)),
    str(delta.get("pool_shares_rejected", 0)),
    str(delta.get("validation_total_shares", 0)),
    str(delta.get("validation_sampled_shares", 0)),
    str(delta.get("validation_invalid_samples", 0)),
    str(delta.get("validation_fraud_detections", 0)),
]

print("\t".join(row))
PY
}

run_leg() {
    local pair="$1"
    local leg="$2"
    local repo_dir="$3"
    local binary="$4"
    local stratum_port="$5"
    local api_port="$6"

    local run_dir="$output_dir/pair_${pair}_${leg}"
    mkdir -p "$run_dir"
    local cfg_source="$repo_dir/config.example.json"
    if [[ ! -f "$cfg_source" ]]; then
        cfg_source="$repo_dir/config.json"
    fi
    if [[ ! -f "$cfg_source" ]]; then
        echo "error: missing config.example.json/config.json in $repo_dir" >&2
        exit 1
    fi

    local cfg_path="$run_dir/config.json"
    local db_path="$run_dir/pool.sqlite"
    write_leg_config "$cfg_source" "$cfg_path" "$stratum_port" "$api_port"

    python3 - "$cfg_path" "$db_path" <<'PY'
import json
import sys

cfg_path = sys.argv[1]
db_path = sys.argv[2]
with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)
cfg["database_path"] = db_path
with open(cfg_path, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2, sort_keys=False)
    f.write("\n")
PY

    local log_path="$run_dir/pool.log"
    local pool_pid=""
    cleanup_pool() {
        stop_pool "$pool_pid"
    }
    trap cleanup_pool RETURN

    RUST_LOG=warn "$binary" --config "$cfg_path" >"$log_path" 2>&1 &
    pool_pid="$!"

    if ! wait_for_pool_ready "$api_port" "$ready_timeout_secs"; then
        echo "error: pool failed readiness check for pair=$pair leg=$leg (see $log_path)" >&2
        stop_pool "$pool_pid"
        exit 1
    fi

    local load_json="$run_dir/load.json"
    local load_log="$run_dir/load.log"
    "$SCRIPT_DIR/pool_load.py" \
        --host 127.0.0.1 \
        --stratum-port "$stratum_port" \
        --api-port "$api_port" \
        --workers "$workers" \
        --duration-secs "$duration_secs" \
        --shares-per-worker-per-sec "$shares_per_worker_per_sec" \
        --submit-mode "$submit_mode" \
        --output "$load_json" \
        >"$load_log" 2>&1

    stop_pool "$pool_pid"
    pool_pid=""
    trap - RETURN
    append_result_row "$pair" "$leg" "$load_json"
}

echo "[bench] building baseline: $baseline_dir"
build_pool "$baseline_dir"
echo "[bench] building candidate: $candidate_dir"
build_pool "$candidate_dir"

baseline_bin="$(binary_path "$baseline_dir")"
candidate_bin="$(binary_path "$candidate_dir")"
if [[ ! -x "$baseline_bin" ]]; then
    echo "error: baseline binary missing: $baseline_bin" >&2
    exit 1
fi
if [[ ! -x "$candidate_bin" ]]; then
    echo "error: candidate binary missing: $candidate_bin" >&2
    exit 1
fi

for ((pair=1; pair<=pairs; pair++)); do
    slot=$((pair - 1))
    baseline_stratum_port=$((base_stratum_port + slot * 20))
    baseline_api_port=$((base_api_port + slot * 20))
    candidate_stratum_port=$((baseline_stratum_port + 10))
    candidate_api_port=$((baseline_api_port + 10))

    first_leg="baseline"
    second_leg="candidate"
    if [[ "$run_order" == "alternate" ]] && ((pair % 2 == 0)); then
        first_leg="candidate"
        second_leg="baseline"
    fi

    if [[ "$first_leg" == "baseline" ]]; then
        first_dir="$baseline_dir"
        first_bin="$baseline_bin"
        first_stratum_port="$baseline_stratum_port"
        first_api_port="$baseline_api_port"
        second_dir="$candidate_dir"
        second_bin="$candidate_bin"
        second_stratum_port="$candidate_stratum_port"
        second_api_port="$candidate_api_port"
    else
        first_dir="$candidate_dir"
        first_bin="$candidate_bin"
        first_stratum_port="$candidate_stratum_port"
        first_api_port="$candidate_api_port"
        second_dir="$baseline_dir"
        second_bin="$baseline_bin"
        second_stratum_port="$baseline_stratum_port"
        second_api_port="$baseline_api_port"
    fi

    echo "[bench] pair $pair/$pairs $first_leg: ports $first_stratum_port/$first_api_port"
    run_leg "$pair" "$first_leg" "$first_dir" "$first_bin" "$first_stratum_port" "$first_api_port"

    if ((cooldown_secs > 0)); then
        echo "[bench] cooldown ${cooldown_secs}s"
        sleep "$cooldown_secs"
    fi

    echo "[bench] pair $pair/$pairs $second_leg: ports $second_stratum_port/$second_api_port"
    run_leg "$pair" "$second_leg" "$second_dir" "$second_bin" "$second_stratum_port" "$second_api_port"

    if ((pair < pairs && cooldown_secs > 0)); then
        echo "[bench] cooldown ${cooldown_secs}s"
        sleep "$cooldown_secs"
    fi
done

python3 - "$results_tsv" >"$summary_txt" <<'PY'
import csv
import math
import statistics
import sys

results_path = sys.argv[1]

rows = []
with open(results_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    rows.extend(reader)

def collect(leg, field):
    vals = []
    for row in rows:
        if row.get("leg") != leg:
            continue
        try:
            vals.append(float(row.get(field, "0") or 0.0))
        except ValueError:
            vals.append(0.0)
    return vals

def avg(vals):
    return statistics.fmean(vals) if vals else 0.0

def fmt(v):
    return f"{v:.4f}"

base_accept_tps = collect("baseline", "accept_tps")
cand_accept_tps = collect("candidate", "accept_tps")
base_resp_tps = collect("baseline", "response_tps")
cand_resp_tps = collect("candidate", "response_tps")
base_p95 = collect("baseline", "latency_p95_ms")
cand_p95 = collect("candidate", "latency_p95_ms")
base_max_reg_q = collect("baseline", "max_regular_q")
cand_max_reg_q = collect("candidate", "max_regular_q")
base_max_inflight = collect("baseline", "max_in_flight")
cand_max_inflight = collect("candidate", "max_in_flight")

base_accept_avg = avg(base_accept_tps)
cand_accept_avg = avg(cand_accept_tps)
base_resp_avg = avg(base_resp_tps)
cand_resp_avg = avg(cand_resp_tps)
base_p95_avg = avg(base_p95)
cand_p95_avg = avg(cand_p95)

def pct_delta(candidate, baseline):
    if baseline == 0.0:
        return 0.0
    return ((candidate - baseline) / baseline) * 100.0

print("Pool A/B Summary")
print(f"rows={len(rows)}")
print()
print(f"baseline.accept_tps.avg={fmt(base_accept_avg)}")
print(f"candidate.accept_tps.avg={fmt(cand_accept_avg)}")
print(f"delta.accept_tps.pct={fmt(pct_delta(cand_accept_avg, base_accept_avg))}")
print()
print(f"baseline.response_tps.avg={fmt(base_resp_avg)}")
print(f"candidate.response_tps.avg={fmt(cand_resp_avg)}")
print(f"delta.response_tps.pct={fmt(pct_delta(cand_resp_avg, base_resp_avg))}")
print()
print(f"baseline.latency_p95_ms.avg={fmt(base_p95_avg)}")
print(f"candidate.latency_p95_ms.avg={fmt(cand_p95_avg)}")
print(f"delta.latency_p95_ms.pct={fmt(pct_delta(cand_p95_avg, base_p95_avg))}")
print()
print(f"baseline.max_regular_q.avg={fmt(avg(base_max_reg_q))}")
print(f"candidate.max_regular_q.avg={fmt(avg(cand_max_reg_q))}")
print(f"baseline.max_in_flight.avg={fmt(avg(base_max_inflight))}")
print(f"candidate.max_in_flight.avg={fmt(avg(cand_max_inflight))}")
PY

echo "[bench] done"
echo "[bench] results: $results_tsv"
echo "[bench] summary: $summary_txt"
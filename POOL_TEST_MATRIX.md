# Pool Integration Test Matrix

## 0) Preflight

Run from `/media/Code/blocknet`.

```bash
# daemon patch branch
git -C blocknet rev-parse --abbrev-ref HEAD
git -C blocknet log --oneline -1

# pool + miner local changes/build sanity
cargo test --manifest-path blocknet-pool/Cargo.toml
cargo test --no-run --manifest-path seine/Cargo.toml
```

Expected:
- daemon branch is `mining-template-speed-patch` (or equivalent patched branch)
- `blocknet-pool` tests pass
- `seine` compiles/tests build

---

## 1) Baseline E2E (pool + daemon + miner)

### Config (pool `config.json`)

Use baseline values:

```json
{
  "validation_mode": "probabilistic",
  "max_verifiers": 2,
  "max_validation_queue": 2048,
  "sample_rate": 0.05,
  "warmup_shares": 50,
  "min_sample_every": 20,
  "invalid_sample_threshold": 0.01,
  "invalid_sample_min": 50,
  "forced_verify_duration": "24h",
  "quarantine_duration": "1h",
  "max_quarantine_duration": "168h",
  "provisional_share_delay": "15m",
  "max_provisional_shares": 200,
  "stratum_submit_v2_required": false
}
```

### Start stack

```bash
# terminal 1: daemon (API on)
cd /media/Code/blocknet/blocknet
./blocknet --daemon --api 127.0.0.1:8332 --data ./data

# terminal 2: pool
cd /media/Code/blocknet/blocknet-pool
cargo run --release -- --config config.json

# terminal 3: miner (pool mode)
cd /media/Code/blocknet/seine
cargo run --release -- \
  --mode pool \
  --address <YOUR_BLOCKNET_ADDRESS> \
  --pool-url stratum+tcp://127.0.0.1:3333 \
  --pool-worker rig-01
```

### Pass/Fail signals

Pass:
- Pool log shows miner login and accepted shares.
- `curl -s http://127.0.0.1:8080/api/stats` shows accepted shares increasing.
- No frequent `validation timeout` / `server busy, retry`.

Fail:
- `shares_accepted` remains flat while miner is active.
- Repeated stratum errors for valid miner setup.

---

## 2) v2 Submit Requirement Gate

Goal: verify strict mode rejects submit payloads without `claimed_hash`.

### Set

```json
{
  "stratum_submit_v2_required": true,
  "validation_mode": "probabilistic"
}
```

### Probe script (one-shot)

Run while pool is up:

```bash
python3 - <<'PY'
import json, socket, time

s = socket.create_connection(("127.0.0.1", 3333), timeout=10)
f = s.makefile("rw", buffering=1)

# login
f.write(json.dumps({"id":1,"method":"login","params":{"address":"TSTADDR","worker":"probe"}})+"\n")

job_id = None
deadline = time.time() + 10
while time.time() < deadline:
    line = f.readline()
    if not line:
        break
    msg = json.loads(line)
    if msg.get("method") == "job":
        params = msg.get("params", {})
        job_id = params.get("job_id")
        break

if not job_id:
    print("FAIL: no job received")
    raise SystemExit(1)

# submit WITHOUT claimed_hash
f.write(json.dumps({"id":2,"method":"submit","params":{"job_id":job_id,"nonce":1}})+"\n")
line = f.readline().strip()
print(line)
PY
```

Pass:
- Response contains error `claimed hash required`.

Fail:
- Submit accepted or rejected for unrelated reason (with valid job id).

---

## 3) Fraud Escalation + Persistent Quarantine

Goal: force full verification and trigger mismatch fraud path, then confirm persistence.

### Set

```json
{
  "validation_mode": "full",
  "stratum_submit_v2_required": true,
  "quarantine_duration": "5m",
  "max_quarantine_duration": "30m",
  "forced_verify_duration": "10m"
}
```

### Trigger

- Use a test client/miner to submit valid `job_id` + nonce with intentionally wrong `claimed_hash`.

Pass:
- Pool logs include `invalid share proof`.
- Pool logs include `risk escalation for ...` and quarantined/force-verify timestamps.
- Re-login from same address returns `address quarantined`.
- Restart pool, repeat login check: still `address quarantined` (persistence confirmed).

Fail:
- No escalation after repeated proof mismatches.
- Quarantine disappears after pool restart.

---

## 4) Validation Queue Saturation / Backpressure

Goal: verify the node remains stable under parallel submit pressure.

### Set

```json
{
  "validation_mode": "probabilistic",
  "max_verifiers": 1,
  "max_validation_queue": 32,
  "sample_rate": 0.05
}
```

### Run

- Start multiple miners/workers (or repeated submit probes) in parallel for 5-10 minutes.

Monitor:

```bash
watch -n 1 'curl -s http://127.0.0.1:8080/api/stats | jq'
```

Pass:
- Process stays alive and responsive.
- Candidate queue remains prioritized (candidate submissions not starved).
- Backpressure appears as controlled rejects (`server busy, retry`) instead of crashes.

Fail:
- OOM/restarts, persistent deadlock, or unbounded queue growth symptoms.

---

## 5) Compact Submit Path (daemon API + pool behavior)

Goal: verify compact API path and pool fallback behavior.

### Direct daemon compact API check

```bash
TOKEN=$(cat /media/Code/blocknet/blocknet/data/api.cookie 2>/dev/null || true)
AUTH=()
[ -n "$TOKEN" ] && AUTH=(-H "Authorization: Bearer $TOKEN")

BT=$(curl -s "${AUTH[@]}" http://127.0.0.1:8332/api/mining/blocktemplate)
TID=$(echo "$BT" | jq -r .template_id)

echo "template_id=$TID"
curl -s "${AUTH[@]}" -H 'content-type: application/json' \
  -d "{\"template_id\":\"$TID\",\"nonce\":0}" \
  http://127.0.0.1:8332/api/mining/submitblock
```

Pass:
- `blocktemplate` response includes non-empty `template_id`.
- `submitblock` accepts compact payload shape (no schema error like missing/unknown fields).

### Pool-side compact attempt

Pass (when block candidate happens):
- Pool logs compact path silently on success.
- If compact path fails, log shows:
  - `compact submit failed, falling back to full block payload`

Fail:
- Compact payload rejected as malformed when `template_id` is valid.

---

## 6) Provisional Share Gating Signals

Goal: verify provisional shares are tracked and age out to eligibility.

### Set

```json
{
  "validation_mode": "probabilistic",
  "sample_rate": 0.05,
  "provisional_share_delay": "60s",
  "max_provisional_shares": 50
}
```

### Observe

```bash
watch -n 2 'curl -s http://127.0.0.1:8080/api/stats | jq'
```

Pass:
- `validation_pending_provisional` rises while shares arrive, then drops as delay passes.
- No abrupt rewarding behavior from unverified bursts (monitor balances/payout logs).

Fail:
- Pending provisional never decays, or all shares treated as instantly verified despite low sample rate.

---

## 7) Final Exit Criteria (ready for staged production)

Mark green only if all are true:

- Baseline E2E stable for >= 30 min.
- v2 requirement gate behaves exactly as configured.
- Fraud escalation + quarantine persists across restart.
- Saturation test shows controlled backpressure, no crash.
- Compact submit API path validated.
- Provisional metrics behave as expected over time window.
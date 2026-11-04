#!/usr/bin/env bash
# Start a fresh local Anvil; existing endpoints are not accepted.
set -euo pipefail
if [[ $# -ne 0 ]]; then
    echo "Usage: ANVIL=/path/to/anvil ./tests/runners/test-local-protocol.sh" >&2
    exit 2
fi
task_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
task_anvil="${ANVIL:-anvil}"
task_cargo="${CARGO:-cargo}"
task_dir="$(mktemp -d "${TMPDIR:-/tmp}/barterlend-local-protocol.XXXXXX")"
task_pid=""
task_cleanup() {
    if [[ -n "$task_pid" ]]; then
        kill "$task_pid" 2>/dev/null || true
        wait "$task_pid" 2>/dev/null || true
    fi
    rm -rf "$task_dir"
}
trap task_cleanup EXIT
"$task_anvil" --version | python3 -c 'import sys; s=sys.stdin.read(); sys.exit(0 if "Version: 1.8.4" in s else "Anvil 1.8.4 required")'
python3 - "$task_repo/tests/fixtures/protocol-v5" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / 'GENERATED.json').read_text())
for entry in manifest['files']:
    content = (root / 'generated' / entry['path']).read_bytes()
    if hashlib.sha256(content).hexdigest() != entry['sha256']:
        raise SystemExit(f"Fixture changed: {entry['path']}")
print('Verified protocol fixture hashes')
PY
task_port="$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    print(s.getsockname()[1])
PY
)"
task_rpc="http://127.0.0.1:$task_port"
"$task_anvil" --host 127.0.0.1 --port "$task_port" --chain-id 31337 \
    --network monad --timestamp 1 --silent \
    --load-state "$task_repo/tests/fixtures/protocol-v5/generated/anvil-state.json" \
    >"$task_dir/anvil.log" 2>&1 &
task_pid="$!"
python3 - "$task_rpc" "$task_dir/anvil.log" <<'PY'
import json, pathlib, sys, time, urllib.request
rpc, log = sys.argv[1:]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for _ in range(50):
    try:
        request = urllib.request.Request(rpc, json.dumps({
            'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]
        }).encode(), {'Content-Type':'application/json'})
        result = json.load(opener.open(request, timeout=0.2))
        if result['result'] != '0x7a69':
            raise SystemExit('local chain identity mismatch')
        break
    except (OSError, KeyError):
        time.sleep(0.1)
else:
    raise SystemExit('Local Anvil did not start:\n' + pathlib.Path(log).read_text())
PY
cd "$task_repo"
task_pool="$(python3 - "$task_repo/tests/fixtures/protocol-v5/generated/addresses.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['pool'])
PY
)"
# Exercise the public keyless consumer before mutations. Calls cannot mine blocks.
BL_STATE_RPC_URL="$task_rpc" BL_STATE_CHAIN_ID=31337 BL_STATE_POOL="$task_pool" \
    BL_STATE_DEPLOYMENT_BLOCK=0 BL_STATE_COMMITMENT=latest BL_STATE_POLLS=1 \
    BL_STATE_CHECKPOINT="$task_dir/keyless-checkpoint.json" \
    "$task_cargo" run --offline --locked --example watch_positions >"$task_dir/keyless.json"
python3 - "$task_dir/keyless.json" <<'PY'
import json, sys
result = json.load(open(sys.argv[1]))
if result['kind'] != 'bootstrapped':
    raise SystemExit('keyless example did not finish bootstrap')
if result['detail']['identity']['chain_id'] != 31337:
    raise SystemExit('keyless example chain identity mismatch')
if result['detail']['as_of']['number'] != 0:
    raise SystemExit('keyless example did not observe the initial local block')
if result['detail']['positions'] != []:
    raise SystemExit('keyless example reported unexpected local positions')
print('PASS: keyless example read the real local pool and saved its checkpoint')
PY
BARTERLEND_LOCAL_RPC="$task_rpc" "$task_cargo" test --offline --locked \
    --test local_protocol -- --ignored --nocapture

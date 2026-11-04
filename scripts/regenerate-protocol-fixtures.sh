#!/usr/bin/env bash
# Regenerate golden fixtures from the external pinned protocol bundle.
set -euo pipefail

task_mode="check"
if [[ "${1:-}" == "--update" ]]; then
    task_mode="update"
elif [[ $# -ne 0 ]]; then
    echo "Usage: FORGE=forge SOLC=solc-0.8.31 $0 [--update]" >&2
    exit 2
fi

task_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
task_fixture="$task_repo/tests/fixtures/protocol-v5"
task_forge="${FORGE:-forge}"
task_solc="${SOLC:-solc}"
task_dir="$(mktemp -d "${TMPDIR:-/tmp}/barterlend-state-fixtures.XXXXXX")"
trap 'rm -rf "$task_dir"' EXIT

"$task_forge" --version | python3 -c 'import sys; s=sys.stdin.read(); sys.exit(0 if "Version: 1.8.4" in s else "Foundry 1.8.4 required")'
"$task_solc" --version | python3 -c 'import sys; s=sys.stdin.read(); sys.exit(0 if "Version: 0.8.31+" in s else "Solc 0.8.31 required")'
python3 - "$task_fixture" <<'PY'
import hashlib, json, pathlib, sys
fixture = pathlib.Path(sys.argv[1])
manifest = json.loads((fixture / 'INPUTS.json').read_text())
for entry in manifest['files']:
    path = fixture / 'solidity' / entry['path']
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != entry['sha256']:
        raise SystemExit(f"Pinned Solidity input changed: {entry['path']}")
print(f"Verified {len(manifest['files'])} pinned Solidity inputs")
PY

cp -RL "$task_fixture/solidity" "$task_dir/solidity"
mkdir "$task_dir/generated"
"$task_forge" test --root "$task_dir/solidity" --network monad --use "$task_solc" \
    --offline --match-contract ProtocolStateFixtures -vv

# Derive bindings from the same compiler inputs as the executable fixtures.
python3 - "$task_repo" "$task_dir/solidity/out" "$task_mode" <<'PY'
import hashlib, json, pathlib, re, sys
repo, artifacts = map(pathlib.Path, sys.argv[1:3])
mode = sys.argv[3]
inputs = json.loads((repo / 'tests/fixtures/protocol-v5/INPUTS.json').read_text())
sources = {'CreditPool': 'CreditPool.sol', 'ICreditPool': 'ICreditPool.sol',
           'ICreditPoolEvents': 'ICreditPool.sol', 'IPriceOracle': 'IPriceOracle.sol',
           'ICreditPoolFactory': 'ICreditPoolFactory.sol'}
provenance = []
for name, source in sources.items():
    artifact = json.loads((artifacts / source / (name + '.json')).read_text())
    data = (json.dumps(artifact['abi'], indent=2) + '\n').encode()
    path = repo / 'abi' / (name + '.v5.json')
    metadata = artifact['metadata']
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    settings = metadata['settings']
    if (settings.get('viaIR', False) != inputs['compiler_settings']['via_ir']
            or settings['evmVersion'] != inputs['compiler_settings']['evm_version']
            or settings['optimizer']['enabled'] != inputs['compiler_settings']['optimizer']
            or settings['optimizer']['runs'] != inputs['compiler_settings']['optimizer_runs']):
        raise SystemExit(f'ABI compiler settings mismatch: {name}')
    if mode == 'update':
        path.write_bytes(data)
    elif path.read_bytes() != data:
        raise SystemExit(f'ABI differs: {name}; inspect before --update')
    provenance.append({'name': name, 'source_commit': inputs['source_head'],
        'compiler': metadata['compiler']['version'], 'via_ir': settings.get('viaIR', False),
        'evm_version': settings['evmVersion'], 'execution_network': 'monad',
        'sha256': hashlib.sha256(data).hexdigest()})
path = repo / 'abi/PROVENANCE.json'
binding_path = repo / 'src/abi.rs'
binding = binding_path.read_text()
pattern = r'(pub const ABI_FINGERPRINT:[\s\S]*?b256!\(\s*")[0-9a-f]{64}("\s*\);)'
updated, count = re.subn(pattern, lambda match: match[1] + provenance[0]['sha256'] + match[2], binding)
if count != 1:
    raise SystemExit('Expected one ABI_FINGERPRINT declaration')
if mode == 'update':
    path.write_text(json.dumps(provenance, indent=2) + '\n')
    binding_path.write_text(updated)
elif json.loads(path.read_text()) != provenance or binding != updated:
    raise SystemExit('ABI provenance/fingerprint differs; inspect before --update')
print(f'Verified {len(sources)} generated ABIs and checkpoint fingerprint')
PY

python3 - "$task_fixture" "$task_dir/generated" "$task_mode" <<'PY'
import hashlib, json, pathlib, shutil, sys
fixture, generated = map(pathlib.Path, sys.argv[1:3])
mode = sys.argv[3]
state = json.loads((fixture / 'anvil-block-template.json').read_text())
state['accounts'] = json.loads((generated / 'empty-pool-state.json').read_text())
addresses = json.loads((generated / 'addresses.json').read_text())
state['block']['timestamp'] = hex(addresses['timestamp'])
for account in state['accounts'].values():
    if isinstance(account.get('nonce'), str):
        account['nonce'] = int(account['nonce'], 16)
(generated / 'anvil-state.json').write_text(json.dumps(state, sort_keys=True, separators=(',', ':')) + '\n')
expected_dir = fixture / 'generated'
names = {p.name for p in generated.glob('*.json')}
expected_names = {p.name for p in expected_dir.glob('*.json')}
if mode == 'check':
    if names != expected_names:
        raise SystemExit(f"Golden file inventory differs: generated={sorted(names)}, expected={sorted(expected_names)}")
    for name in sorted(names):
        actual = json.loads((generated / name).read_text())
        expected = json.loads((expected_dir / name).read_text())
        if actual != expected:
            raise SystemExit(f"Golden fixture differs: {name}; inspect before --update")
    print(f"PASS: {len(names)} generated JSON fixtures equal recorded goldens")
else:
    expected_dir.mkdir(parents=True, exist_ok=True)
    for path in expected_dir.glob('*.json'):
        if path.name not in names:
            path.unlink()
    for path in generated.glob('*.json'):
        shutil.copyfile(path, expected_dir / path.name)
    manifest = {'files': [
        {'path': p.name, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': p.stat().st_size}
        for p in sorted(expected_dir.glob('*.json'))
    ]}
    (fixture / 'GENERATED.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(f"Updated {len(names)} goldens; review their diff and GENERATED.json")
PY

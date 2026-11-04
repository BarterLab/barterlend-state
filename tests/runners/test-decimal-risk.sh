#!/usr/bin/env bash
# Fresh Solidity getter results for actual token decimals; immutable fixture inputs stay untouched.
set -euo pipefail
task_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
task_fixture="$task_repo/tests/fixtures/protocol-v5"
task_forge="${FORGE:-forge}"
task_solc="${SOLC:-solc}"
task_output="${1:?Usage: FORGE=forge SOLC=solc-0.8.31 bash tests/runners/test-decimal-risk.sh /absolute/new-evidence-directory}"
[[ "$task_output" = /* ]] || { echo 'Evidence directory must be absolute' >&2; exit 2; }
mkdir "$task_output"
task_dir="$(mktemp -d "${TMPDIR:-/tmp}/barterlend-state-decimals.XXXXXX")"
trap 'rm -rf "$task_dir"' EXIT
"$task_forge" --version | python3 -c 'import sys; sys.exit(0 if "Version: 1.8.4" in sys.stdin.read() else "Foundry 1.8.4 required")'
"$task_solc" --version | python3 -c 'import sys; sys.exit(0 if "Version: 0.8.31+" in sys.stdin.read() else "Solc 0.8.31 required")'
python3 - "$task_repo" "$task_fixture" "$task_output" <<'PY'
import hashlib, json, pathlib, sys
repo, root, output = map(pathlib.Path, sys.argv[1:])
manifest = json.loads((root / 'INPUTS.json').read_text())
paths = [root / 'INPUTS.json', root / 'GENERATED.json'] + sorted((root / 'generated').glob('*.json'))
for entry in manifest['files']:
    path = root / 'solidity' / entry['path']
    if hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
        raise SystemExit(f"Pinned Solidity input changed: {entry['path']}")
    paths.append(path)
paths += sorted((repo / 'src').glob('*.rs'))
paths += [repo / 'tests/risk_fixtures.rs', repo / 'tests/solidity/DecimalsRiskDifferential.t.sol', repo / 'tests/runners/test-decimal-risk.sh']
hashes = {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}
(output / 'source-input-hashes-before.json').write_text(json.dumps(hashes, indent=2) + '\n')
print(f"Verified {len(manifest['files'])} unchanged pinned Solidity inputs")
PY
cp -RL "$task_fixture/solidity" "$task_dir/solidity"
mkdir "$task_dir/generated"
cp "$task_repo/tests/solidity/DecimalsRiskDifferential.t.sol" "$task_dir/solidity/test/generator/DecimalsRiskDifferential.t.sol"
"$task_forge" test --root "$task_dir/solidity" --network monad --use "$task_solc" --offline \
    --match-contract '^DecimalsRiskDifferential$' --match-test '^test_generate_decimal_risk_vectors' -vv \
    2>&1 | tee "$task_output/solidity-generator.log"
cp "$task_dir/generated/decimal-risk-vectors.json" "$task_output/decimal-risk-vectors.json"
BARTERLEND_DECIMAL_RISK_VECTORS="$task_output/decimal-risk-vectors.json" cargo +"${BARTERLEND_RUST_TOOLCHAIN:-1.98.0}" test \
    --manifest-path "$task_repo/Cargo.toml" --offline --locked --test risk_fixtures \
    risk_matches_fresh_decimal_solidity_getters -- --ignored --exact --nocapture \
    2>&1 | tee "$task_output/rust-differential.log"
python3 - "$task_repo" "$task_output" <<'PY'
import hashlib, json, pathlib, re, sys
repo, root = map(pathlib.Path, sys.argv[1:]); path = root / 'decimal-risk-vectors.json'
vectors = json.loads(path.read_text())
if len(vectors) != 24:
    raise SystemExit(f"Expected 24 fresh vectors, got {len(vectors)}")
pairs = sorted({(v['source_decimals'], v['target_decimals']) for v in vectors})
if pairs != [(6, 8), (6, 18), (8, 6), (18, 6)]:
    raise SystemExit(f"Unexpected actual decimal pairs: {pairs}")
rust_log = (root / 'rust-differential.log').read_text()
if len(re.findall(r'^test risk_matches_fresh_decimal_solidity_getters \.\.\. ok$', rust_log, re.M)) != 1 \
        or len(re.findall(r'^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out;', rust_log, re.M)) != 1 \
        or len(re.findall(r'^compared 24 fresh actual Solidity getter vectors$', rust_log, re.M)) != 1:
    raise SystemExit('Expected exactly one successful Rust differential test and explicit 24-record comparison')
solidity_log = (root / 'solidity-generator.log').read_text()
if len(re.findall(r'^\[PASS\] test_generate_decimal_risk_vectors\(\)', solidity_log, re.M)) != 1 \
        or not re.search(r'1 tests passed, 0 failed, 0 skipped \(1 total tests\)', solidity_log):
    raise SystemExit('Expected exactly one successful Solidity generator test')
before = json.loads((root / 'source-input-hashes-before.json').read_text())
after = {name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in before}
(root / 'source-input-hashes-after.json').write_text(json.dumps(after, indent=2) + '\n')
if before != after:
    raise SystemExit('SDK source, generator or immutable fixture hashes changed during differential run')
report = {'status': 'passed', 'network': 'none', 'solidity_getter_vectors': len(vectors),
    'actual_source_target_decimals': pairs, 'vector_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    'method': 'Pinned CreditPool v5 and facets, actual ERC20 decimals; test setter only seeds raw positions; independently compared by Rust evaluate_risk.',
    'immutable_fixtures_modified': False, 'rust_differential_tests_passed': 1, 'solidity_generator_tests_passed': 1,
    'explicit_records_compared': 24, 'source_and_inputs_unchanged_before_after': True, 'hashes_verified': len(before),
    'python_checks': 'explicit exceptions/exit codes; unaffected by PYTHONOPTIMIZE'}
(root / 'acceptance.json').write_text(json.dumps(report, indent=2) + '\n')
print(f"PASS: {len(vectors)} fresh Solidity getter vectors compared with Rust, four actual token-decimal orientations")
PY

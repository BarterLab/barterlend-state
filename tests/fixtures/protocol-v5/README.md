# CreditPool v5 fixtures

These fixtures compare SDK event decoding and risk calculations with the CreditPool Solidity implementation. The generator uses `CreditPoolTestSuite` with test tokens and a test price oracle.

`INPUTS.json` records the protocol revision, dependency revisions, compiler settings, and source hashes. `solidity/lib/` contains pinned third-party dependencies and their license notices. Protocol and generator sources are supplied as an external bundle; ordinary Rust and seeded Anvil tests use the included bytecode. `GENERATED.json` records the hashes of the files in `generated/`.

## Regenerate

Use Foundry 1.8.4, solc 0.8.31, and Python 3. The compiler settings are no IR, optimizer enabled with 200 runs, and EVM Osaka. The source revision is pinned in `INPUTS.json`; these compiler settings qualify the local fixtures, not an independently deployed bytecode build.

Run from the repository root:

```bash
python3 scripts/with-protocol-fixtures.py --source /external/sdk-protocol-v5 -- \
  env FORGE=/path/to/forge SOLC=/path/to/solc-0.8.31 \
  bash scripts/regenerate-protocol-fixtures.sh
```

The external bundle contains `src/`, `test/` and `foundry.toml` matching `INPUTS.json`. The wrapper temporarily mounts it and removes its mounts on exit. The script checks input hashes, builds in a temporary directory, and compares the generated JSON and ABIs with the recorded files. Use `--update` to replace fixtures, ABI provenance and the checkpoint fingerprint after reviewing a protocol change. For source changes, review and update `INPUTS.json` first.

## Event fixtures

| File | Case |
|---|---|
| `provided-borrow.json` | Debt created through `Provided`, without `Borrowed`. |
| `provided-mixed.json` | A `Provided` event includes a reloan lender and a new borrower. |
| `provided-all-arrays.json` | Nonempty fills, invalidations, and reconciliations; a collateral-capped borrower receives a partial fill before its remainder is invalidated. |
| `borrowed.json` | Direct borrowing with the ending APR. |
| `reloaned.json` | Exchange of a lender claim with the ending APR, without new borrower debt. |
| `underlying-index.json` | Underlying yield changes the index without changing borrower amounts. |
| `refinanced.json` | Debt and collateral moved between markets; five-argument `DebtRefinanced`. |
| `collateral.json` | Deposit, move, and withdrawal in market `0` and an arbitrary market. |
| `repaid-other-payer.json` | Repayment by someone other than the borrower. |
| `offset.json` | Debt repaid using loan tokens. |
| `liquidated-bad-debt.json` | Collateral-limited liquidation and bad debt. |
| `admin.json` | Global settings change while existing market settings remain cached. |
| `sweep.json` | A `Borrowed` beneficiary has no debt; `Provided` identifies the actual borrower. |

Event fixtures contain logs and expected final positions read from contract getters. Amounts are decimal strings; bytes use `0x` hex. Recorded Forge logs omit RPC block hashes, transaction hashes, and log indices; Anvil supplies those in integration tests.

`risk-vectors.json` records Solidity health factors and liquidation eligibility, including HF = 1, grace-period boundaries, stale prices, and rounding. Call-status fields distinguish successful getters from reverts.

## Run the integration test

```bash
ANVIL=/path/to/anvil ./tests/runners/test-local-protocol.sh
```

The script checks fixture hashes, starts Anvil 1.8.4 on chain 31337, and runs the keyless watcher and protocol integration test. It stops Anvil when finished.

`generated/anvil-state.json` is the Anvil seed derived from `empty-pool-state.json` and `anvil-block-template.json`. It contains a configured pool with no open debt or collateral positions. `addresses.json` identifies the contracts and funded test accounts used to exercise real protocol operations.

Python script checks live in `tests/python/`; run `python3 -m unittest discover -s tests/python`. Shell integration runners live in `tests/runners/`. The decimal-risk generator also requires the external source wrapper.

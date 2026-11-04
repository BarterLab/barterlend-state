# BarterLend State SDK

A read-only Rust library for tracking borrower positions in BarterLend v5. It discovers changes, reads balances and configuration at a common block, and calculates liquidation eligibility. Your bot handles sizing, swaps, profitability, signing and execution.

## Install

Requires Rust 1.98 or newer. For a sibling checkout:

```toml
[dependencies]
barterlend-state = { path = "../barterlend-state" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

For a Git dependency, pin a compatible full commit from [BarterLab/barterlend-state](https://github.com/BarterLab/barterlend-state). The bindings target protocol revision `81d2b8b93013f3b9dd56655f818b0586437c2279`. Earlier v5 deployments have different event ABIs; check [ABI provenance](abi/PROVENANCE.json), not only `version()`.

## Choose an engine

Create one engine per pool. Share one `Arc<HttpReader>` across engines to share the RPC budget.

| Engine | Coverage and startup |
| --- | --- |
| `StateEngine` | Discovers positions from history, including collateral-only positions in arbitrary markets. Requires complete logs and headers from the configured deployment block. |
| `CurrentStateEngine` | Enumerates current contract registries for all debt positions. Rebuilds after a long gap without replaying the missed history. Arbitrary-market collateral-only positions are outside its coverage. |

Both engines follow events after discovery and refresh oracle and time-dependent risk. Current-state discovery still scales with the number of registered users and scheduled markets. [Engine configuration and recovery](docs/SDK_API.md).

## Discover pools

For a reviewed CREATE-based CreditPoolFactory v5, use `HttpReader::factory_page`. It reads the factory nonce and checks derived addresses with `isPool`, without replaying historical logs:

```rust,no_run
use barterlend_state::{Commitment, HttpReader, StateReader, Result, factory::FactorySpec};

async fn first_page(reader: &HttpReader, factory: &FactorySpec) -> Result<()> {
    let at = reader.head(Commitment::Finalized).await?;
    let page = reader.factory_page(factory, 1, 32, &at).await?;
    for pool in page.pools { println!("{pool}"); }
    // Continue from page.next_nonce; caught up when it equals page.account_nonce.
    Ok(())
}
```

Pin the factory address, runtime code hash and predecessor in `FactorySpec`, and verify the reader's chain identity. Each page checks canonicality, code and version. Advance the cursor only after success; keep polling from it for newly created pools. Enumerate older factories separately. Pages contain at most 64 addresses. CREATE2 factories and proxies are not supported by this enumeration method. Discovering a pool does not authorize liquidation or establish token safety.

## Watch positions

Supply the pool address, protocol version and deployment block in `PoolSpec`, and the chain ID and genesis hash in `ChainIdentity`.

```rust,no_run
use std::{sync::Arc, time::Duration};
use barterlend_state::{
    ChainIdentity, EngineConfig, HttpReader, PoolSpec, Result,
    RiskStatus, StateEngine, SyncStepOutcome, rpc::RpcConfig,
};

async fn watch_pool(url: &str, chain: ChainIdentity, pool: PoolSpec) -> Result<()> {
    // One shared quota for in-flight and recently completed requests.
    let reader = Arc::new(HttpReader::new(url, RpcConfig::default())?
        .with_completion_rate_limit(15)?);
    let config = EngineConfig {
        expected_identity: Some(chain),
        ..EngineConfig::default()
    };
    let mut engine = StateEngine::new(reader, pool, config)?;

    loop {
        if let SyncStepOutcome::Complete(_) = engine.sync_step().await? {
            let snapshot = engine.snapshot().expect("completed snapshot");
            for position in snapshot.positions.values() {
                if matches!(&position.risk, RiskStatus::Ready(risk) if risk.execution_ready) {
                    println!("borrower={}, market={}, debt={}, collateral={}",
                        position.key.borrower, position.key.market,
                        position.amounts.debt, position.amounts.collateral);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

The example shares a 15-request budget, counting requests in flight and completed during the last second. This conservative limit prevents delayed requests from releasing a burst; throughput decreases with latency. Configure it before cloning the reader. It is optional: `RpcConfig::default()` only spaces request starts by 20 ms (about 50/s). Polling does not require processing each block separately: missed blocks are consolidated into a new snapshot. The default commitment is `Finalized`.

`Progress` means synchronization is unfinished. `Complete` makes a completed snapshot available. Production consumers should retry transient errors with backoff and reject stale snapshots; the minimal example returns errors to its caller.

## Use the snapshot

Positions are keyed by `(pool, market, borrower)` and retained while debt or collateral is nonzero. Each snapshot includes its block (`as_of`), oracle and effective market configuration.

- Debt and collateral are token base units.
- The oracle's `numerator / denominator` converts collateral base units to debt-asset base units; decimals are already included.
- Health factor, LTV and discount use a scale of `1e18`.
- `RiskStatus::Unavailable` preserves raw balances but means risk could not be calculated.
- `execution_ready` does not establish profitability or authorize a transaction. Refresh the chosen position, check size and price constraints, and simulate before sending.

An empty inactive market may be removed with `StateUpdate::MarketRemoved` and rediscovered after a new event. After a `Reset`, invalidate unsigned plans from the old epoch. `snapshot()` can still contain an older snapshot after a failed sync.

## Persist and restore

For `StateEngine`, use `FileStore` for a completed `Checkpoint` and `DiscoveryFileStore` for unfinished discovery. Save progress after each step, including completed work from a failed step. On completion, save the checkpoint before removing the discovery file.

For `CurrentStateEngine`, use `CurrentFileStore`: its envelope contains completed state and census progress. The two engine formats are not interchangeable. Use separate files per pool and synchronize restored state before trading. These files are derived caches, not transaction journals.

The [watcher example](examples/watch_positions.rs) includes polling and persistence:

```sh
cargo run --example watch_positions
```

Set `BL_STATE_RPC_URL`, `BL_STATE_CHAIN_ID`, `BL_STATE_GENESIS_HASH`, `BL_STATE_POOL` and `BL_STATE_DEPLOYMENT_BLOCK`. `BL_STATE_POLLS=0` runs continuously. The RPC must provide the history required by the selected engine.

## Reference

- [API guide](docs/SDK_API.md): engines, RPC limits, optional Multicall3, persistence and errors.
- [Contract ABIs](abi/README.md): on-demand pool and orderbook reads.
- [Protocol fixtures](tests/fixtures/protocol-v5/README.md): regeneration and local integration tests.

## License

Licensed under the [MIT License](LICENSE). Third-party components retain their own licenses.

# API guide

## Types and units

| Type | Purpose |
| --- | --- |
| `ChainIdentity` | Chain ID and genesis hash. |
| `PoolSpec` | Pool address, protocol version and history start block. |
| `BlockRef` | Block number, hash, timestamp and commitment. |
| `StateSnapshot` | Positions, configuration and oracle for one pool at `as_of`. |
| `PositionKey` | Pool, market and borrower. |
| `PositionAmounts` | Debt and collateral in token base units (`u128`). |
| `RiskStatus` | Calculated risk and eligibility, or an unavailable reason. |
| `Checkpoint` / `DiscoveryCheckpoint` | Completed historical state / unfinished history and getter progress. |
| `CurrentSnapshot` / `CurrentCheckpoint` | Debt-complete state / current-state recovery envelope with explicit coverage. |

`Price.numerator / denominator` converts collateral raw units to asset raw units; do not apply token decimals again. HF, LTV and discount use `1e18`. JSON encodes `u128` amounts as decimal strings and `U256` values as hex strings; market IDs, blocks and timestamps are `u64` numbers. Avoid floating-point parsing for financial values.

Health liquidation requires HF < 1. Maturity eligibility starts at `market + grace`; oracle data is stale at `updated_at + stale_window`. The SDK follows the supported protocol's acceptance of future oracle timestamps. Execution policy can additionally reject `updated_at > as_of.timestamp`. `execution_ready` does not check liquidation size, funding, route, gas or profit.

## Historical synchronization

`StateEngine` tracks one pool. Its mutable methods must be called sequentially. Multiple engines can share an `Arc<HttpReader>`.

| Method | Purpose |
| --- | --- |
| `sync_step()` | Polling and resumable discovery. Returns `Progress` or `Complete(SyncOutcome)`. |
| `sync()` | Runs up to `max_sync_steps` within the operation timeout. |
| `bootstrap()` | Rebuilds from the history start at a new epoch; repeated calls resume unfinished work. |
| `bootstrap_at(block)` / `sync_to(block)` | Explicit-block reads; do not retarget or save partial discovery. |
| `snapshot()` / `snapshot_shared()` | Borrow/share the last completed snapshot. It may be old after a failed sync. |

Set `deployment_block` to the creation block or earlier. Discovery requires complete historical logs and headers. Events identify affected positions; amounts come from getters. Configuration, oracle and balances are read at a common target. Polling also updates risk when prices or maturity change without a lending event.

Completed outcomes are `Bootstrapped`, `Advanced`, `Unchanged` and `Reset`. A reset replaces the snapshot and changes its epoch; invalidate unsigned plans from the old epoch. Updates describe the final state of a block range, not a transaction-by-transaction replay. Pools may have different snapshot blocks.

Both engines retain active markets and markets with nonzero debt or collateral, including matured debt. Empty inactive markets emit `StateUpdate::MarketRemoved`; later events can rediscover them. Consumers applying incremental updates must handle removals.

## Current-state recovery

`CurrentStateEngine` discovers `CurrentCoverage::AllDebtPositionsV5` from current contract registries. For the supported v5 implementation, `getMarketsConfig` identifies matured scheduled markets and `getActiveMarkets` identifies future active markets. The engine pages their user registries, reads balances, and catches the bounded event tail generated during enumeration.

Isolated collateral-only positions at arbitrary unscheduled maturities can be absent. Use `StateEngine` when that broader coverage is required.

```rust,no_run
use std::sync::Arc;
use barterlend_state::{CurrentStateEngine, CurrentStateConfig, CurrentFileStore,
    EngineConfig, HttpReader, PoolSpec, Result, SyncStepOutcome};

async fn current(reader: Arc<HttpReader>, pool: PoolSpec, config: EngineConfig) -> Result<()> {
    let policy = CurrentStateConfig { max_event_catchup_blocks: 1000, max_calendar_markets: 20_000 };
    let store = CurrentFileStore::new("pool.current.json");
    let mut engine = match store.load(config.expected_identity.as_ref(), Some(&pool))? {
        Some(saved) => CurrentStateEngine::restore(reader, pool, config, policy, saved)?,
        None => CurrentStateEngine::new(reader, pool, config, policy)?,
    };
    let step = engine.sync_step().await;
    // In a service, run checkpoint construction and file I/O on a bounded blocking worker.
    if let Some(builder) = engine.checkpoint_builder() { store.save(&builder.build()?)?; }
    if matches!(step?.outcome, SyncStepOutcome::Complete(_)) {
        let snapshot = engine.snapshot_shared().expect("completed state");
        println!("coverage={:?}, positions={}", snapshot.coverage, snapshot.state.positions.len());
    }
    Ok(())
}
```

Continue calling `sync_step()` on `Progress`. Short gaps use events; gaps exceeding `max_event_catchup_blocks` and reorgs trigger a new census. Partial progress resumes only while its anchor remains usable. Completed historical state can be imported with `from_full_checkpoint`; historical discovery progress is not imported.

`max_calendar_markets` bounds the in-memory calendar batch, also limited by 64 entries and `read_batch_size`. It is not a lifetime count of maturities. Closed users are read page by page without accumulating their empty positions; `max_positions` counts retained nonempty positions. Discovery time still depends on scheduled slots and registered users, including closed ones. A resource limit produces an error, never a successful truncated snapshot.

A fresh census needs genesis/deployment headers, execution state at its current anchors and its concurrent event tail. It does not replay all logs or require execution state from the offline interval. Keep the explicit coverage envelope: stripping it does not turn current state into a full historical snapshot.

## Configuration and RPC

`EngineConfig` controls commitment, batches, reconciliation, timeouts and retained-state limits. `RpcConfig` controls transport rate, concurrency, retries, request timeouts and response caps.

- Default commitment: `Finalized`; `Safe` and `Latest` are available.
- Default request interval: 20 ms, about 50 requests/s. For at most 15/s, use `Duration::from_millis(67)` and one shared reader. Separate clients do not share a quota automatically.
- For a conservative window, call `with_completion_rate_limit(15)` before cloning the client or deriving lanes. It counts in-flight requests plus requests completed during the last second, including external request permits. This replaces interval pacing, while keeping concurrency limits. Slow responses reduce throughput. This is optional and does not change `RpcConfig` defaults.
- Match `log_chunk_size` to `RpcConfig.max_log_range` and your provider's limits.
- `history_chunks_per_step` defaults to 8 and `getter_batches_per_step` to 64. `operation_timeout` bounds a step; convenience methods share one timeout across their steps.
- Interrupted getter batches retry at a smaller size, down to one key. Completed batches are retained at their pinned target.
- `NumberBracket` is the default read mode. `Eip1898` and `RawHash` can pin calls by hash when supported.

`HttpReader::with_endpoints()` configures endpoints; `rpc().select_endpoint(index)` selects one explicitly. Clones share endpoint selection and admission limits. `control_reader()` can reserve concurrency within the same total budget: for example, 8 total slots split into 6 bulk and 2 control slots. Retries consume new rate slots; endpoint changes do not reset the budget.

An external transaction transport can acquire `rpc().acquire_request_permit()` before each attempt and hold it until the response is consumed. This shares the quota with SDK reads without giving the SDK signing or broadcast responsibilities.

### Event-driven refresh

`event_driven_refresh=true` reuses immutable pool/token metadata and event-covered settings after validation in the engine session. It requires the supported non-upgradeable v5 implementation and complete logs. Administrative events refresh effective configuration; new markets are read before use. Oracle data is still refreshed at each new snapshot, and risk follows chain time. The default is `false`.

`reconciliation_interval` spaces defensive position/user reads by chain time. Zero preserves per-sync scans. Counts are controlled by `reconcile_positions_per_sync` and `reconcile_user_pages_per_sync`. A consumer can use 60 seconds, two positions and zero user pages without disabling initial discovery. After restart, metadata/configuration is validated again.

Missed blocks are consolidated into one target snapshot. Risk is reused only when amounts, settings, oracle status and timestamp are identical. Snapshot publication and serialization still scale with state size. Tune against measured provider limits; batching alone does not guarantee a latency bound.

## Pool and orderbook reads

Use `HttpReader::contract_call()` with a binding from `abi::CreditPool` and an explicit `BlockRef`. The SDK does not maintain a full lender/orderbook projection.

Typed position and risk getters use the pool's native `multicall(bytes[])`, with canonical checks before and after the call, including errors. Default position batches contain at most 32 positions. Invalid/reverted responses fail the read; generic `read_bundle()` keeps individual-call behavior.

`read_pool_views(pool, reads, at)` accepts 1–64 ordered `PinnedRead::Call` entries for one pool. It supports pool metadata, position/loan/balance getters, effective risk settings, active markets, users, `getProvideOrderbook` and `previewBorrow`. The selector and canonical ABI encoding are validated. Foreign targets, bytecode reads, nested multicalls and mutations are rejected. A reverted or malformed member fails the bundle. No broadcast, sender override or per-call fallback is performed.

`Borrowed` and `Reloaned` contain `endAPR`. `UnderlyingPoolIndexUpdated` is decoded without invalidating borrower amounts: the underlying index affects lender liquidity. Orderbook consumers should refresh their own views when it changes.

## Optional read optimizations

### Multicall3

`HttpReader::with_multicall3(Multicall3Config::canonical())` enables cross-contract aggregation for `read_views` and supported SDK metadata/oracle reads. It is disabled by default. `contract_call()` and generic `read_bundle()` retain their own behavior.

`read_views` accepts at most 64 ordered reads. Multiple view calls are aggregated in bounded chunks; individual calls and code reads remain direct where needed. Each aggregate validates the runtime hash at its anchor, complete ABI and subcall results. A failed aggregate has no silent individual-call fallback. RPC rate, timeout and response limits still apply.

Only supply sender-independent view getters. Multicall3 uses `CALL`, changing `msg.sender`; a simulated mutation could affect later calls within the same `eth_call`. This API sends no transactions, approvals or value.

Configure the address, runtime hash, `max_calls` (1–64), gas limit and calldata cap with `Multicall3Config`. Its canonical constructor pins a particular deployment; verify compatibility on your chain. The corresponding [runtime fixture](../tests/fixtures/multicall3-runtime.hex) and [license](../tests/fixtures/multicall3-LICENSE) are included for local tests. The 3,808-byte runtime comes from the signed deployment data in [mds1/multicall3 at b667d67](https://github.com/mds1/multicall3/tree/b667d67ecfa5361a81e8f110234ce242613b0012); its decoded-byte SHA-256 is `2756d7c52baee85cacb504f6ee1df7aad6809ac8d94a4a111d76991f90d36d6e`.

### ReadOptimizationConfig

Configure Multicall3 before enabling `batch_log_hashes`.

| Option | Behavior |
| --- | --- |
| `coalesce_pending_checks` | Shares identical header/identity/code requests only while waiting for transport admission. Already dispatched results and errors are not reused by a later closing check. |
| `batch_log_hashes` | Uses pinned Multicall3 `getBlockHash` for recent event witnesses. The current block and witnesses outside the 256-block window retain direct header checks. Small groups remain direct. |
| `position_batch_size` | Allows 1–512 positions in a native pool batch. Non-default sizes require explicit gas/calldata limits; response limits still apply. |

Defaults retain direct checks and batches of 32 positions. Failed reads do not automatically increase limits or downgrade verification. Reduce batch size/gas if the provider cannot serve the configured request.

### Verification scopes and events

`HttpReader::verified_read(future)` shares repeated verification reads inside one operation and performs fresh closing checks. Return staged data and publish it only after both the outer verification and inner calculation succeed. Do not perform irreversible effects inside the future. Nested scopes are rejected; cancellation discards the scope.

`CurrentStateEngine` uses bounded verification scopes. Failed closing checks, cancellation or endpoint changes discard that step's staged progress/publication. The preceding completed step remains available. Historical `StateEngine` keeps its verified-prefix progress semantics.

`filtered_logs(address, from, to, topics)` supports at most four positional topics (`None` is a wildcard). It validates address/range/topics and retains transport limits. It does not establish canonicality by itself: verify log block witnesses and cursor/anchor consistency before committing derived state. `StateReader::logs` remains address-only.

`StateReader::verify_log_blocks()` checks fresh headers by default. `HttpReader` can reuse bounded immutable parent-chain witnesses, but acceptance always requires a fresh canonical anchor. Arbitrary header reads are not a TTL cache of canonicality. Implement `StateReader` for another data source; optional market-schedule support is needed by the current-state engine.

## Persistence and errors

For historical discovery, use `FileStore` and `DiscoveryFileStore`. Save progress after each step, including an error with completed batches. Save the matching completed base checkpoint before discovery progress; on completion, save the final checkpoint before removing discovery. `restore_discovery()` returns `false` for valid obsolete progress.

For current-state recovery, use `CurrentFileStore`, which stores state and census progress together. Synchronize restored data before using it for decisions. A reorg discards orphaned results. Historical discovery can keep its verified log prefix when getter state is pruned and retarget the remaining reads.

ABI fingerprints and coverage are validated on restore. An incompatible derived cache must be rebuilt with the selected engine; do not edit its fingerprint to force acceptance. Historical discovery schema 1 remains readable and new saves use schema 2. Unsupported unfinished current census layouts restart that census; completed supported snapshots remain usable.

Use `snapshot_shared()` to share immutable state without cloning the position map. Checkpoint builders and `reconciliation_cursor()` allow construction/serialization on a bounded blocking worker. Capture and persist the same base snapshot used by a discovery builder. File-size limits constrain serialized bytes, not total process memory.

Stores use writer locks, bounded serialization and atomic replacement on a local filesystem. Keep lock files: OS locks are released on process exit. The reserved `.<checkpoint filename>.write.tmp` file is reused safely after a crash; symlinks, hardlinks and nonregular entries are rejected. Do not use that path for application data. `quarantine_invalid()` preserves damaged derived caches for inspection; it does not repair I/O or size-limit failures. Obsolete quarantine files require a separate retention policy.

| Error | Handling |
| --- | --- |
| `Transport`, transient `Rpc` | Retry with backoff and preserve progress. |
| `ResourceLimit` | Inspect the exhausted limit; resume progress where supported. |
| `MissingBlock`, `NonCanonical` | Synchronize against an available canonical target. |
| `IncompleteHistory` | Use an RPC that retains the required history. |
| `IdentityMismatch`, `UnsupportedVersion` | Check chain, pool and ABI configuration. |
| `Decode`, `Arithmetic`, `InvalidConfig` | Check response encoding, amounts and configuration. |
| `Checkpoint`, `Json`, `Io` | Inspect storage; restore or rebuild the derived cache as appropriate. |

Failed synchronization never publishes partial amounts. Track the last successful sync and enforce freshness. Provider errors may contain details requiring redaction. Keep financial transaction journals separate from all SDK caches.

## Factory discovery

`factory::FactorySpec` pins a CREATE-based v5 factory by address, runtime code hash and predecessor. `HttpReader::factory_page(spec, next_nonce, page_size, at)` returns registered pool addresses, the next cursor and the factory nonce at the anchor. Start at nonce 1; pages are limited to 64 candidates. All calls share the reader's RPC budget and optional Multicall3 configuration.

Check chain identity before discovery. Persist or publish cursor progress only after a successful page. A changed code/version/predecessor, regressed nonce, malformed response or changed canonical block returns an error. The helper does not follow predecessors, authorize pools, or enumerate CREATE2/proxy deployments. Keep any consumer's pool count and polling interval bounded.

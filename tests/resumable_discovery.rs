use std::{collections::BTreeMap, sync::{Arc, Mutex}};

use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::SolEvent;
use async_trait::async_trait;
use barterlend_state::{*, abi::CreditPool, risk::UNIT, storage::{FileStore, DiscoveryFileStore}};

struct Data {
    identity: ChainIdentity,
    blocks: BTreeMap<u64, BlockRef>,
    states: BTreeMap<u64, BTreeMap<PositionKey, PositionAmounts>>,
    logs: Vec<RawLog>,
    users: BTreeMap<u64, Vec<Address>>,
    calls: Vec<(String, u64)>,
    globals: PoolRiskConfig,
    market_config: MarketRiskConfig,
    oracle: OracleStatus,
    fail_positions: bool,
    change_target_during_read: bool,
    range_limit: u64,
    minimum_state_block: u64,
    advance_head_on_pruned: bool,
    head: u64,
    delayed_log: Option<u64>,
    advance_on_first_log: Option<u64>,
    prune_every_position_read: bool,
    log_delay_ms: u64,
    position_delay_ms: u64,
    config_delay_ms: u64,
    position_batches: Vec<(u64, Vec<PositionKey>)>,
}

struct MockReader(Mutex<Data>);

fn pool() -> Address { Address::repeat_byte(1) }
fn key(account: u8, market: u64) -> PositionKey {
    PositionKey { pool: pool(), market, borrower: Address::repeat_byte(account) }
}
fn block(number: u64) -> BlockRef {
    BlockRef { number, hash: B256::repeat_byte((number + 1) as u8),
        parent_hash: B256::repeat_byte(number as u8), timestamp: number * 100,
        commitment: Commitment::Finalized }
}
fn borrowed(position: PositionKey, number: u64, index: u64) -> RawLog {
    let data = CreditPool::Borrowed { account: position.borrower, market: position.market,
        assetsBorrowed: 1, debtIncurred: 1, endAPR: U256::ZERO }.encode_log_data();
    RawLog { address: pool(), topics: data.topics().to_vec(), data: data.data,
        block_number: number, block_hash: block(number).hash,
        transaction_hash: B256::repeat_byte((index + 101) as u8), log_index: index, removed: false }
}
fn spec() -> PoolSpec {
    PoolSpec { address: pool(), deployment_block: 0, expected_version: 5,
        deployment_block_hash: Some(block(0).hash) }
}
fn config() -> EngineConfig {
    EngineConfig { reconcile_positions_per_sync: 0, reconcile_user_pages_per_sync: 0,
        log_chunk_size: 10, ..Default::default() }
}

impl MockReader {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(Data {
            identity: ChainIdentity { chain_id: 143, genesis_hash: block(0).hash },
            blocks: (0..=40).map(|number| (number, block(number))).collect(),
            states: BTreeMap::new(), logs: Vec::new(), users: BTreeMap::new(), calls: Vec::new(),
            globals: PoolRiskConfig { liquidation_ltv: U256::from(UNIT * 8 / 10), stale_window: 50, min_debt_remaining: 10 },
            market_config: MarketRiskConfig { grace_period: 10, discount_min: U256::from(UNIT / 100), discount_max: U256::from(UNIT / 5) },
            oracle: OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(100_000) }),
            fail_positions: false, change_target_during_read: false, range_limit: 100,
            minimum_state_block: 0, advance_head_on_pruned: false, head: 40, delayed_log: None, advance_on_first_log: None, prune_every_position_read: false, log_delay_ms: 0,
            position_delay_ms: 0, config_delay_ms: 0, position_batches: Vec::new(),
        })))
    }
    fn set(&self, number: u64, position: PositionKey, debt: u128, collateral: u128) {
        let mut data = self.0.lock().unwrap();
        let mut state = data.states.range(..=number).next_back().map(|(_, state)| state.clone()).unwrap_or_default();
        state.insert(position, PositionAmounts { debt, collateral });
        data.states.insert(number, state);
    }
    fn event(&self, position: PositionKey, number: u64) {
        let mut data = self.0.lock().unwrap();
        let index = data.logs.iter().filter(|log| log.block_number == number).count() as u64;
        data.logs.push(borrowed(position, number, index));
    }
}

#[async_trait]
impl StateReader for MockReader {
    async fn identity(&self) -> Result<ChainIdentity> { Ok(self.0.lock().unwrap().identity) }
    async fn head(&self, commitment: Commitment) -> Result<BlockRef> {
        let data = self.0.lock().unwrap(); let mut result = data.blocks[&data.head]; result.commitment = commitment; Ok(result)
    }
    async fn block(&self, number: u64, commitment: Commitment) -> Result<Option<BlockRef>> {
        Ok(self.0.lock().unwrap().blocks.get(&number).copied().map(|mut result| { result.commitment = commitment; result }))
    }
    async fn describe_pool(&self, spec: &PoolSpec, at: &BlockRef) -> Result<PoolDescriptor> {
        self.0.lock().unwrap().calls.push(("descriptor".into(), at.number));
        Ok(PoolDescriptor { address: spec.address, version: 5,
            asset: TokenDescriptor { address: Address::repeat_byte(3), decimals: 6 },
            collateral: TokenDescriptor { address: Address::repeat_byte(4), decimals: 18 },
            oracle: Address::repeat_byte(5), code_hash: B256::repeat_byte(6) })
    }
    async fn read_positions(&self, _: Address, keys: &[PositionKey], at: &BlockRef) -> Result<BTreeMap<PositionKey, PositionAmounts>> {
        let delay = { let mut data = self.0.lock().unwrap();
            data.position_batches.push((at.number, keys.to_vec())); data.position_delay_ms * keys.len() as u64 };
        if delay != 0 { tokio::time::sleep(std::time::Duration::from_millis(delay)).await; }
        let mut data = self.0.lock().unwrap();
        data.calls.push(("positions".into(), at.number));
        if data.fail_positions { return Err(Error::Transport("primitive unavailable".into())); }
        if data.prune_every_position_read { data.head = at.number + 1; data.minimum_state_block = data.head; }
        if at.number < data.minimum_state_block {
            if data.advance_head_on_pruned { data.head = data.minimum_state_block; data.advance_head_on_pruned = false; }
            return Err(Error::MissingBlock(at.number));
        }
        let state = data.states.range(..=at.number).next_back().map(|(_, state)| state.clone()).unwrap_or_default();
        if data.change_target_during_read {
            data.blocks.get_mut(&at.number).unwrap().hash = B256::repeat_byte(200);
        }
        Ok(keys.iter().map(|key| (*key, state.get(key).copied().unwrap_or_default())).collect())
    }
    async fn read_globals(&self, _: Address, at: &BlockRef) -> Result<PoolRiskConfig> {
        let mut data = self.0.lock().unwrap(); data.calls.push(("globals".into(), at.number)); Ok(data.globals)
    }
    async fn read_market_configs(&self, _: Address, markets: &[u64], at: &BlockRef) -> Result<BTreeMap<u64, MarketRiskConfig>> {
        let delay = self.0.lock().unwrap().config_delay_ms;
        if delay != 0 { tokio::time::sleep(std::time::Duration::from_millis(delay)).await; }
        let mut data = self.0.lock().unwrap(); data.calls.push(("configs".into(), at.number));
        Ok(markets.iter().map(|&market| (market, data.market_config)).collect())
    }
    async fn read_oracle(&self, _: &PoolDescriptor, at: &BlockRef) -> Result<OracleStatus> {
        let mut data = self.0.lock().unwrap(); data.calls.push(("oracle".into(), at.number)); Ok(data.oracle.clone())
    }
    async fn logs(&self, _: Address, from: u64, to: u64) -> Result<Vec<RawLog>> {
        let (answer, delayed) = {
            let mut data = self.0.lock().unwrap(); data.calls.push(("logs".into(), from));
            if to - from + 1 > data.range_limit {
                return Err(Error::Rpc { code: -32005, message: "block range too wide".into() });
            }
            let answer = data.logs.iter().filter(|log| log.block_number >= from && log.block_number <= to).cloned().rev().collect();
            if from == 0 {
                if let Some(head) = data.advance_on_first_log.take() { data.head = head; data.minimum_state_block = head; }
            }
            (answer, if data.delayed_log == Some(from) { 80 } else { data.log_delay_ms })
        };
        if delayed != 0 { tokio::time::sleep(std::time::Duration::from_millis(delayed)).await; }
        Ok(answer)
    }
    async fn market_users(&self, _: Address, market: u64, start: u64, limit: u64, at: &BlockRef) -> Result<Vec<Address>> {
        let mut data = self.0.lock().unwrap(); data.calls.push((format!("users:{market}:{start}"), at.number));
        Ok(data.users.get(&market).into_iter().flatten().skip(start as usize).take(limit as usize).copied().collect())
    }
    async fn active_markets(&self, _: Address, _: &BlockRef) -> Result<Vec<u64>> { Ok(vec![1000]) }
}


fn step_config() -> EngineConfig {
    EngineConfig { log_chunk_size: 1, history_chunks_per_step: 2,
        reconcile_positions_per_sync: 0, reconcile_user_pages_per_sync: 0,
        ..Default::default() }
}

#[tokio::test]
async fn convenience_sync_uses_fresh_post_census_target_without_replaying_floor() {
    let reader = MockReader::new(); let early = key(7, 1000); let tail = key(8, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.advance_on_first_log = Some(5); }
    reader.set(1, early, 100, 200); reader.event(early, 1);
    reader.set(4, tail, 50, 80); reader.event(tail, 4);
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap();
    let SyncOutcome::Bootstrapped(snapshot) = engine.sync().await.unwrap() else { panic!("expected complete bootstrap"); };
    assert_eq!(snapshot.as_of, block(5)); assert_eq!(snapshot.positions.len(), 2);
    assert!(engine.discovery_checkpoint().unwrap().is_none());
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
    assert!(data.calls.iter().filter(|(kind, _)| matches!(kind.as_str(), "descriptor" | "positions" | "globals" | "configs" | "oracle"))
        .all(|(_, at)| *at == 5));
}

#[tokio::test]
async fn convenience_step_budget_retains_census_across_repeated_calls() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut options = step_config(); options.max_sync_steps = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    for through in [1, 3] {
        assert!(matches!(engine.sync().await, Err(Error::ResourceLimit(message)) if message.contains("max_sync_steps")));
        assert_eq!(engine.discovery_checkpoint().unwrap().unwrap().through.unwrap().number, through);
        assert!(engine.snapshot().is_none());
    }
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Bootstrapped(_)));
    let starts: Vec<_> = reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, n)| *n).collect();
    assert_eq!(starts, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn convenience_timeout_is_total_across_steps_and_retains_completed_chunks() {
    let reader = MockReader::new();
    { let mut data = reader.0.lock().unwrap(); data.head = 3; data.log_delay_ms = 80; }
    let mut options = step_config(); options.history_chunks_per_step = 1;
    options.operation_timeout = std::time::Duration::from_millis(200);
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(matches!(engine.sync().await, Err(Error::Transport(message)) if message.contains("timed out")));
    assert_eq!(engine.discovery_checkpoint().unwrap().unwrap().through.unwrap(), block(1));
    assert!(engine.snapshot().is_none());
    reader.0.lock().unwrap().log_delay_ms = 0;
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Bootstrapped(_)));
    let starts: Vec<_> = reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, n)| *n).collect();
    assert_eq!(starts, vec![0, 1, 2, 2, 3]);
}

#[tokio::test]
async fn bootstrap_rebuilds_same_head_at_new_epoch_and_resumes_unfinished_rebuild() {
    let reader = MockReader::new(); let position = key(7, 1000); reader.0.lock().unwrap().head = 4;
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut options = step_config(); options.max_sync_steps = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    engine.bootstrap_at(block(4)).await.unwrap(); let before = engine.checkpoint().unwrap();
    reader.0.lock().unwrap().calls.clear();
    for through in [1, 3] {
        assert!(matches!(engine.bootstrap().await, Err(Error::ResourceLimit(_))));
        let progress = engine.discovery_checkpoint().unwrap().unwrap();
        assert!(progress.rebuilding); assert_eq!(progress.epoch, 2);
        assert_eq!(progress.through.unwrap().number, through);
        assert_eq!(engine.checkpoint().unwrap(), before);
    }
    let snapshot = engine.bootstrap().await.unwrap();
    assert_eq!(snapshot.epoch, 2); assert_eq!(snapshot.as_of, block(4));
    assert_eq!(snapshot.positions[&position].amounts.debt, 100);
    let starts: Vec<_> = reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, n)| *n).collect();
    assert_eq!(starts, vec![0, 1, 2, 3, 4]);
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Unchanged { epoch: 2, .. }));
}

#[tokio::test]
async fn bootstrap_replaces_incomplete_warm_catchup_with_new_discovery_generation() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut options = step_config(); options.history_chunks_per_step = 1; options.max_sync_steps = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    assert!(matches!(engine.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
    let old = engine.discovery_checkpoint().unwrap().unwrap(); assert_eq!(old.through.unwrap(), block(2));
    let directory = tempfile::tempdir().unwrap(); let store = DiscoveryFileStore::new(directory.path().join("progress.json"));
    store.save(&old).unwrap();
    assert!(matches!(engine.bootstrap().await, Err(Error::ResourceLimit(_))));
    let new = engine.discovery_checkpoint().unwrap().unwrap();
    assert!(new.rebuilding); assert_eq!(new.generation, old.generation + 1);
    assert_eq!(new.from_block, 0); assert_eq!(new.through.unwrap(), block(0));
    store.save(&new).unwrap();
    assert_eq!(store.load(None, None).unwrap().unwrap(), new);
    assert_eq!(engine.snapshot().unwrap().epoch, 1);
}

#[tokio::test]
async fn explicit_target_pruning_fails_without_implicit_head_selection() {
    let reader = MockReader::new(); let position = key(7, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.minimum_state_block = 3; data.advance_head_on_pruned = true; }
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    assert!(matches!(engine.bootstrap_at(block(2)).await, Err(Error::MissingBlock(2))));
    assert!(engine.snapshot().is_none()); assert!(engine.discovery_checkpoint().unwrap().is_none());
    assert!(!reader.0.lock().unwrap().calls.iter().any(|(kind, at)| kind == "positions" && *at == 3));
    reader.0.lock().unwrap().minimum_state_block = 0;
    engine.bootstrap_at(block(1)).await.unwrap(); let before = engine.checkpoint().unwrap();
    reader.event(position, 2);
    reader.0.lock().unwrap().minimum_state_block = 3;
    assert!(matches!(engine.sync_to(block(2)).await, Err(Error::MissingBlock(2))));
    assert_eq!(engine.checkpoint().unwrap(), before);
    assert!(engine.discovery_checkpoint().unwrap().is_none());
}

#[tokio::test]
async fn latest_head_retreat_to_saved_cursor_clears_empty_catchup_as_unchanged() {
    let reader = MockReader::new();
    let mut options = step_config(); options.commitment = Commitment::Latest;
    options.operation_timeout = std::time::Duration::from_millis(20);
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    let mut saved_block = block(1); saved_block.commitment = Commitment::Latest;
    engine.bootstrap_at(saved_block).await.unwrap(); let before = engine.checkpoint().unwrap();
    { let mut data = reader.0.lock().unwrap(); data.head = 3; data.delayed_log = Some(2); }
    assert!(matches!(engine.sync_step().await, Err(Error::Transport(_))));
    assert!(engine.discovery_checkpoint().unwrap().unwrap().through.is_none());
    { let mut data = reader.0.lock().unwrap(); data.head = 1; data.blocks.remove(&3); data.delayed_log = None; }
    assert_eq!(engine.sync().await.unwrap(), SyncOutcome::Unchanged { epoch: 1, as_of: saved_block });
    assert_eq!(engine.checkpoint().unwrap(), before);
    assert!(engine.discovery_checkpoint().unwrap().is_none());
}

#[tokio::test]
async fn reorg_during_forced_bootstrap_preserves_the_full_rebuild_request() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut options = step_config(); options.max_sync_steps = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    engine.bootstrap_at(block(4)).await.unwrap();
    assert!(matches!(engine.bootstrap().await, Err(Error::ResourceLimit(_))));
    reader.0.lock().unwrap().blocks.get_mut(&1).unwrap().hash = B256::repeat_byte(199);
    assert!(matches!(engine.bootstrap().await, Err(Error::ResourceLimit(_))));
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    assert!(progress.rebuilding); assert_eq!(progress.generation, 2); assert_eq!(progress.epoch, 2);
    assert!(matches!(engine.bootstrap().await, Err(Error::ResourceLimit(_))));
    assert_eq!(engine.bootstrap().await.unwrap().epoch, 2);
}

async fn finish(engine: &mut StateEngine<MockReader>) -> SyncOutcome {
    for _ in 0..100 {
        match engine.sync_step().await.unwrap() {
            SyncStepOutcome::Complete(outcome) => return outcome,
            SyncStepOutcome::Progress(_) => {}
        }
    }
    panic!("bounded steps did not finish after all history was available");
}

#[tokio::test]
async fn bounded_cold_census_never_publishes_partial_state_and_resumes_from_disk() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.0.lock().unwrap().head = 4;
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap();
    let SyncStepOutcome::Progress(status) = engine.sync_step().await.unwrap() else { panic!("expected progress"); };
    assert_eq!(status.through.unwrap().number, 1);
    assert!(engine.snapshot().is_none()); assert!(engine.checkpoint().is_err());
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "logs").count(), 2);
    assert!(!reader.0.lock().unwrap().calls.iter().any(|(kind, _)| kind == "positions"));
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("discovery.json");
    let store = DiscoveryFileStore::new(&path);
    let progress = engine.discovery_checkpoint().unwrap().unwrap(); store.save(&progress).unwrap();
    assert!(FileStore::new(&path).load(None, None).is_err(), "progress must not parse as Ready state");
    #[cfg(unix)] { use std::os::unix::fs::PermissionsExt; assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600); }
    let mut restarted = StateEngine::new(reader.clone(), spec(), step_config()).unwrap();
    assert!(restarted.restore_discovery(store.load(Some(&progress.identity), Some(&spec())).unwrap().unwrap()).unwrap());
    assert!(matches!(finish(&mut restarted).await, SyncOutcome::Bootstrapped(_)));
    assert_eq!(restarted.snapshot().unwrap().positions[&position].amounts.debt, 100);
    let starts: Vec<_> = reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, n)| *n).collect();
    assert_eq!(starts, vec![0,1,2,3,4]);
    store.remove().unwrap(); assert!(store.load(None, None).unwrap().is_none());
    assert!(directory.path().join("discovery.json.lock").exists());
}

#[tokio::test]
async fn fresh_target_after_long_census_discovers_tail_without_replaying_floor() {
    let reader = MockReader::new(); let early = key(7, 1000); let tail = key(8, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.advance_on_first_log = Some(5); }
    reader.set(1, early, 100, 200); reader.event(early, 1);
    reader.set(4, tail, 50, 80); reader.event(tail, 4);
    let options = EngineConfig { history_chunks_per_step: 8, ..config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(matches!(finish(&mut engine).await, SyncOutcome::Bootstrapped(_)));
    let snapshot = engine.snapshot().unwrap(); assert_eq!(snapshot.as_of, block(5));
    assert_eq!(snapshot.positions.len(), 2);
    assert_eq!(snapshot.positions[&early].amounts.debt, 100);
    assert_eq!(snapshot.positions[&tail].amounts.debt, 50);
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
    assert!(data.calls.iter().filter(|(kind, _)| matches!(kind.as_str(), "descriptor" | "positions" | "globals" | "configs" | "oracle"))
        .all(|(_, at)| *at == 5), "no getter uses the expired pre-census target");
}

#[tokio::test]
async fn pruning_during_getters_repeats_all_getters_on_new_target_and_only_new_tail() {
    let reader = MockReader::new(); let position = key(7, 1000); let extra = key(8, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.minimum_state_block = 3; data.advance_head_on_pruned = true; }
    reader.set(1, position, 100, 200); reader.event(position, 1);
    reader.set(3, extra, 30, 90); reader.event(extra, 3);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    assert!(matches!(finish(&mut engine).await, SyncOutcome::Bootstrapped(_)));
    let snapshot = engine.snapshot().unwrap(); assert_eq!(snapshot.as_of, block(3)); assert_eq!(snapshot.positions.len(), 2);
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
    // Position reads now precede config selection. The pruned amount read
    // must restart every getter already attempted at the old anchor.
    for kind in ["descriptor", "positions"] {
        assert!(data.calls.iter().any(|(name, at)| name == kind && *at == 2));
    }
    for kind in ["descriptor", "globals", "configs", "oracle", "positions"] {
        assert!(data.calls.iter().any(|(name, at)| name == kind && *at == 3));
    }
}

#[tokio::test]
async fn timeout_preserves_only_completed_chunks_and_restart_skips_their_ranges() {
    let reader = MockReader::new(); let position = key(7, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 4; data.delayed_log = Some(1); }
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut options = step_config(); options.operation_timeout = std::time::Duration::from_millis(20);
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::Transport(_))));
    let progress = engine.discovery_checkpoint().unwrap().unwrap(); assert_eq!(progress.through.unwrap(), block(0));
    assert!(progress.positions.is_empty()); assert!(engine.snapshot().is_none());
    reader.0.lock().unwrap().delayed_log = None;
    let mut restarted = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(restarted.restore_discovery(progress).unwrap()); finish(&mut restarted).await;
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 1).count(), 2,
        "the interrupted chunk must be retried, without retrying completed block0");
}

#[tokio::test]
async fn normal_checkpoint_restart_catches_up_in_bounded_steps_without_cold_census() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let state = engine.checkpoint().unwrap();
    reader.set(30, position, 60, 200); reader.event(position, 30);
    reader.0.lock().unwrap().calls.clear();
    let options = EngineConfig { max_blocks_per_sync: 2, ..step_config() };
    let mut restarted = StateEngine::restore(reader.clone(), spec(), options.clone(), state).unwrap();
    let SyncStepOutcome::Progress(_) = restarted.sync_step().await.unwrap() else { panic!("expected bounded catch-up"); };
    let progress = restarted.discovery_checkpoint().unwrap().unwrap();
    let state = restarted.checkpoint().unwrap();
    let mut again = StateEngine::restore(reader.clone(), spec(), options, state).unwrap();
    assert!(again.restore_discovery(progress).unwrap());
    assert!(matches!(finish(&mut again).await, SyncOutcome::Advanced(_)));
    assert_eq!(again.snapshot().unwrap().positions[&position].amounts.debt, 60);
    let data = reader.0.lock().unwrap();
    let starts: Vec<_> = data.calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, n)| *n).collect();
    assert_eq!(starts, (2..=40).collect::<Vec<_>>());
    assert!(data.calls.iter().filter(|(kind, _)| kind == "positions").all(|(_, at)| *at == 40));
}

#[tokio::test]
async fn latest_reorg_discards_orphan_discovery_and_finds_replacement_borrower() {
    let reader = MockReader::new(); let orphan = key(7, 1000); let replacement = key(8, 1000);
    reader.0.lock().unwrap().head = 4;
    reader.set(1, orphan, 100, 200); reader.event(orphan, 1);
    let options = EngineConfig { commitment: Commitment::Latest, ..step_config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(matches!(engine.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
    { let mut data = reader.0.lock().unwrap(); data.blocks.get_mut(&1).unwrap().hash = B256::repeat_byte(199); data.logs.clear(); data.states.clear(); }
    reader.set(1, replacement, 70, 200); reader.event(replacement, 1);
    { let mut data = reader.0.lock().unwrap(); let hash = data.blocks[&1].hash; data.logs[0].block_hash = hash; }
    let SyncStepOutcome::Progress(status) = engine.sync_step().await.unwrap() else { panic!("reset discovery still incomplete"); };
    assert_eq!(status.generation, 2); assert_eq!(status.target.commitment, Commitment::Latest);
    finish(&mut engine).await;
    assert!(!engine.snapshot().unwrap().positions.contains_key(&orphan));
    assert_eq!(engine.snapshot().unwrap().positions[&replacement].amounts.debt, 70);
    assert_eq!(engine.snapshot().unwrap().as_of.commitment, Commitment::Latest);
}

#[tokio::test]
async fn obsolete_discovery_after_state_save_is_ignored_but_corruption_is_rejected() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap();
    engine.sync_step().await.unwrap(); let obsolete = engine.discovery_checkpoint().unwrap().unwrap();
    finish(&mut engine).await; let state = engine.checkpoint().unwrap();
    let mut restarted = StateEngine::restore(reader, spec(), step_config(), state).unwrap();
    assert!(!restarted.restore_discovery(obsolete.clone()).unwrap());
    assert!(matches!(restarted.sync_step().await.unwrap(), SyncStepOutcome::Complete(SyncOutcome::Unchanged { .. })));
    let mut corrupt = obsolete; corrupt.positions.insert(key(9, 1000));
    assert!(restarted.restore_discovery(corrupt).is_err());
}

#[tokio::test]
async fn unavailable_single_block_history_is_actionable_and_never_ready() {
    let reader = MockReader::new(); reader.0.lock().unwrap().range_limit = 0;
    let mut engine = StateEngine::new(reader, spec(), step_config()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::IncompleteHistory(_))));
    assert!(engine.snapshot().is_none());
    assert!(engine.discovery_checkpoint().unwrap().unwrap().through.is_none());
}

#[tokio::test]
async fn getter_stage_exceeding_retention_has_bounded_reanchors_and_preserves_history() {
    let reader = MockReader::new(); let position = key(7, 1000);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.prune_every_position_read = true; }
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::MissingBlock(4))));
    assert!(engine.snapshot().is_none());
    assert_eq!(engine.discovery_checkpoint().unwrap().unwrap().through.unwrap().number, 4);
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, _)| kind == "positions").count(), 3);
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
}

#[tokio::test]
async fn history_call_budget_continues_across_steps_and_never_skips_a_chunk() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 3;
    let mut options = step_config(); options.history_chunks_per_step = 8; options.max_log_calls = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    let SyncStepOutcome::Progress(status) = engine.sync_step().await.unwrap() else { panic!("one history call per step"); };
    assert_eq!(status.through.unwrap().number, 0);
    finish(&mut engine).await;
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, _)| kind == "logs").map(|(_, from)| *from).collect::<Vec<_>>(), vec![0,1,2,3]);
}

#[tokio::test]
async fn reorg_of_unscanned_endpoint_keeps_the_still_canonical_prefix() {
    let reader = MockReader::new(); let early = key(7, 1000); let replacement = key(8, 1000);
    reader.0.lock().unwrap().head = 4;
    reader.set(1, early, 100, 200); reader.event(early, 1);
    let options = EngineConfig { commitment: Commitment::Latest, ..step_config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    engine.sync_step().await.unwrap();
    { let mut data = reader.0.lock().unwrap();
      for n in 2..=4 { data.blocks.get_mut(&n).unwrap().hash = B256::repeat_byte(200 + n as u8); }
    }
    reader.set(2, replacement, 30, 90); reader.event(replacement, 2);
    { let mut data = reader.0.lock().unwrap(); let hash = data.blocks[&2].hash; data.logs.last_mut().unwrap().block_hash = hash; }
    finish(&mut engine).await;
    let snapshot = engine.snapshot().unwrap(); assert!(snapshot.positions.contains_key(&early)); assert!(snapshot.positions.contains_key(&replacement));
    let data = reader.0.lock().unwrap();
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
}

#[tokio::test]
async fn reorg_of_saved_state_rebuilds_with_reset_only_after_complete_discovery() {
    let reader = MockReader::new(); let orphan = key(7, 1000); let replacement = key(8, 1000);
    reader.set(1, orphan, 100, 200); reader.event(orphan, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let saved = engine.checkpoint().unwrap();
    let mut resumed = StateEngine::restore(reader.clone(), spec(), step_config(), saved.clone()).unwrap();
    { let mut data = reader.0.lock().unwrap(); data.head = 4; data.blocks.get_mut(&1).unwrap().hash = B256::repeat_byte(199); data.logs.clear(); data.states.clear(); }
    reader.set(1, replacement, 80, 200); reader.event(replacement, 1);
    { let mut data = reader.0.lock().unwrap(); let hash = data.blocks[&1].hash; data.logs[0].block_hash = hash; }
    let SyncStepOutcome::Progress(status) = resumed.sync_step().await.unwrap() else { panic!("rebuild must be bounded"); };
    assert!(status.rebuilding); assert_eq!(resumed.checkpoint().unwrap(), saved);
    let SyncOutcome::Reset(snapshot) = finish(&mut resumed).await else { panic!("expected reset"); };
    assert_eq!(snapshot.epoch, 2); assert!(!snapshot.positions.contains_key(&orphan)); assert!(snapshot.positions.contains_key(&replacement));
}

#[tokio::test]
async fn discovery_store_rejects_older_conflicting_and_foreign_files_without_damaging_state() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap();
    engine.sync_step().await.unwrap(); let first = engine.discovery_checkpoint().unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap(); let store = DiscoveryFileStore::new(directory.path().join("progress.json"));
    store.save(&first).unwrap(); engine.sync_step().await.unwrap(); let next = engine.discovery_checkpoint().unwrap().unwrap(); store.save(&next).unwrap();
    assert!(store.save(&first).is_err()); assert_eq!(store.load(None, None).unwrap(), Some(next.clone()));
    let too_small = DiscoveryFileStore::new(store.path()).with_max_bytes(1).unwrap(); assert!(too_small.save(&next).is_err());
    assert_eq!(store.load(None, None).unwrap(), Some(next.clone()));
    let foreign_reader = MockReader::new();
    { let mut data = foreign_reader.0.lock().unwrap(); data.identity.chain_id = 10143; data.head = 4; }
    let mut foreign_engine = StateEngine::new(foreign_reader, spec(), step_config()).unwrap();
    foreign_engine.sync_step().await.unwrap();
    let foreign = foreign_engine.discovery_checkpoint().unwrap().unwrap();
    assert!(matches!(store.save(&foreign), Err(Error::IdentityMismatch(_))));
    assert_eq!(store.load(None, None).unwrap(), Some(next));
    finish(&mut engine).await; let ordinary = FileStore::new(directory.path().join("state.json")); ordinary.save(&engine.checkpoint().unwrap()).unwrap();
    assert!(DiscoveryFileStore::new(ordinary.path()).remove().is_err()); assert!(ordinary.load(None, None).unwrap().is_some());
    #[cfg(unix)] { use std::os::unix::fs::symlink; let link = directory.path().join("link.json"); symlink(store.path(), &link).unwrap(); assert!(DiscoveryFileStore::new(link).load(None, None).is_err()); }
}

#[tokio::test]
async fn trusted_identity_and_commitment_are_checked_before_accepting_discovery() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap(); engine.sync_step().await.unwrap();
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    let options = EngineConfig { expected_identity: Some(ChainIdentity { chain_id: 10143, ..progress.identity }), ..step_config() };
    let mut foreign = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(matches!(foreign.restore_discovery(progress.clone()), Err(Error::IdentityMismatch(_))));
    let mut latest = StateEngine::new(reader.clone(), spec(), EngineConfig { commitment: Commitment::Latest, ..step_config() }).unwrap();
    assert!(matches!(latest.restore_discovery(progress), Err(Error::Checkpoint(_))));
    reader.0.lock().unwrap().identity.chain_id = 10143;
    assert!(matches!(engine.sync_step().await, Err(Error::IdentityMismatch(_))));
}

#[tokio::test]
async fn complete_bounded_catchup_matches_the_existing_explicit_target_api() {
    let reader = MockReader::new(); let position = key(7, 1000); let added = key(8, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut baseline = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    baseline.bootstrap_at(block(1)).await.unwrap(); let checkpoint = baseline.checkpoint().unwrap();
    reader.set(3, position, 60, 200); reader.event(position, 3);
    reader.set(4, added, 30, 100); reader.event(added, 4); reader.0.lock().unwrap().head = 4;
    let expected = baseline.sync_to(block(4)).await.unwrap();
    let mut bounded = StateEngine::restore(reader, spec(), step_config(), checkpoint).unwrap();
    assert_eq!(finish(&mut bounded).await, expected);
    assert_eq!(bounded.snapshot(), baseline.snapshot());
    assert_eq!(bounded.checkpoint().unwrap(), baseline.checkpoint().unwrap());
}

#[tokio::test]
async fn invalid_state_quarantine_preserves_exact_bytes_and_valid_state_is_untouched() {
    let reader = MockReader::new(); let mut engine = StateEngine::new(reader, spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let checkpoint = engine.checkpoint().unwrap();
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("state.json"); let store = FileStore::new(&path);
    store.save(&checkpoint).unwrap(); let valid_bytes = std::fs::read(&path).unwrap();
    assert!(store.quarantine_invalid(Some(&checkpoint.snapshot.identity), Some(&spec())).unwrap().is_none());
    assert_eq!(std::fs::read(&path).unwrap(), valid_bytes);
    let mut corrupt: serde_json::Value = serde_json::from_slice(&valid_bytes).unwrap(); corrupt["checksum"] = serde_json::json!(B256::ZERO);
    let invalid_bytes = serde_json::to_vec(&corrupt).unwrap(); std::fs::write(&path, &invalid_bytes).unwrap();
    #[cfg(unix)] { use std::os::unix::fs::PermissionsExt; std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap(); }
    let backup = store.quarantine_invalid(Some(&checkpoint.snapshot.identity), Some(&spec())).unwrap().unwrap();
    assert!(!path.exists()); assert_eq!(std::fs::read(&backup).unwrap(), invalid_bytes);
    #[cfg(unix)] { use std::os::unix::fs::PermissionsExt; assert_eq!(std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777, 0o600); }
    assert!(store.load(None, None).unwrap().is_none()); assert!(store.quarantine_invalid(None, None).unwrap().is_none());
    store.save(&checkpoint).unwrap(); assert_eq!(store.load(None, None).unwrap(), Some(checkpoint));
    assert_eq!(std::fs::read(backup).unwrap(), invalid_bytes, "later cache save must preserve quarantined evidence");
}

#[tokio::test]
async fn invalid_discovery_and_foreign_state_are_quarantined_without_touching_other_cache() {
    let reader = MockReader::new(); let mut engine = StateEngine::new(reader, spec(), step_config()).unwrap();
    engine.sync_step().await.unwrap(); let progress = engine.discovery_checkpoint().unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("progress.json"); let store = DiscoveryFileStore::new(&path);
    store.save(&progress).unwrap(); let valid = std::fs::read(&path).unwrap();
    assert!(store.quarantine_invalid(Some(&progress.identity), Some(&spec())).unwrap().is_none());
    assert_eq!(std::fs::read(&path).unwrap(), valid);
    std::fs::write(&path, b"{\"schema_version\":").unwrap();
    let backup = store.quarantine_invalid(Some(&progress.identity), Some(&spec())).unwrap().unwrap();
    assert_eq!(std::fs::read(backup).unwrap(), b"{\"schema_version\":"); assert!(store.load(None, None).unwrap().is_none());
    finish(&mut engine).await; let checkpoint = engine.checkpoint().unwrap();
    let state_path = directory.path().join("state.json"); let state = FileStore::new(&state_path); state.save(&checkpoint).unwrap();
    let state_bytes = std::fs::read(&state_path).unwrap();
    let foreign_identity = ChainIdentity { chain_id:10143, ..progress.identity };
    let backup = state.quarantine_invalid(Some(&foreign_identity), Some(&spec())).unwrap().unwrap();
    assert_eq!(std::fs::read(backup).unwrap(), state_bytes);
    assert!(store.load(None, None).unwrap().is_none());
}

#[tokio::test]
async fn quarantine_rejects_oversized_symlink_and_nonregular_paths() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("state.json"); std::fs::write(&path, b"not json").unwrap();
    let store = FileStore::new(&path).with_max_bytes(1).unwrap();
    assert!(matches!(store.quarantine_invalid(None, None), Err(Error::ResourceLimit(_)))); assert_eq!(std::fs::read(&path).unwrap(), b"not json");
    assert!(FileStore::new(directory.path()).quarantine_invalid(None, None).is_err());
    #[cfg(unix)] {
        use std::os::unix::fs::symlink; let link = directory.path().join("link.json"); symlink(&path, &link).unwrap();
        assert!(FileStore::new(&link).quarantine_invalid(None, None).is_err()); assert!(std::fs::symlink_metadata(link).unwrap().file_type().is_symlink());
    }
    assert!(directory.path().read_dir().unwrap().all(|entry| !entry.unwrap().file_name().to_string_lossy().contains("quarantine")));
}


fn four_positions(reader: &MockReader) {
    reader.0.lock().unwrap().head = 2;
    for account in 7..=10 { let position = key(account, 1000); reader.set(1, position, 100, 200); reader.event(position, 1); }
}

#[tokio::test]
async fn timed_out_getter_batches_resume_from_disk_without_repeating_completed_positions() {
    let reader = MockReader::new(); four_positions(&reader);
    reader.0.lock().unwrap().position_delay_ms = 40;
    let options = EngineConfig { read_batch_size: 1, operation_timeout: std::time::Duration::from_millis(100), ..config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::Transport(message)) if message.contains("timed out")));
    assert!(engine.snapshot().is_none());
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    let completed = progress.getters.as_ref().unwrap().amounts.clone();
    assert!(!completed.is_empty() && completed.len() < 4);
    let directory = tempfile::tempdir().unwrap(); let store = DiscoveryFileStore::new(directory.path().join("progress.json"));
    store.save(&progress).unwrap();
    let mut restarted = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(restarted.restore_discovery(store.load(None, None).unwrap().unwrap()).unwrap());
    for _ in 0..4 {
        match restarted.sync_step().await {
            Ok(SyncStepOutcome::Complete(_)) => break,
            Ok(SyncStepOutcome::Progress(_)) | Err(Error::Transport(_)) => assert!(restarted.snapshot().is_none()),
            result => panic!("unexpected result: {result:?}"),
        }
    }
    let snapshot = restarted.snapshot().expect("bounded retries must reach the fourth key");
    assert_eq!(snapshot.positions.len(), 4); assert_eq!(snapshot.as_of, block(2));
    let data = reader.0.lock().unwrap();
    for position in completed.keys() {
        assert_eq!(data.position_batches.iter().filter(|(_, keys)| keys.contains(position)).count(), 1);
    }
    assert!(data.position_batches.iter().all(|(at, _)| *at == 2));
    assert_eq!(data.calls.iter().filter(|(kind, _)| kind == "descriptor").count(), 1);
    assert_eq!(data.calls.iter().filter(|(kind, _)| kind == "configs").count(), 1);
}

#[tokio::test]
async fn config_batches_survive_timeout_and_getter_steps_have_an_explicit_budget() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 2;
    for market in 1000..1004 { let position = key(7, market); reader.set(1, position, 100, 200); reader.event(position, 1); }
    reader.0.lock().unwrap().config_delay_ms = 40;
    let options = EngineConfig { read_batch_size: 1, operation_timeout: std::time::Duration::from_millis(100), ..config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::Transport(_))));
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    let configs = progress.getters.as_ref().unwrap().configs.len(); assert!(configs > 0 && configs < 4);
    assert!(engine.snapshot().is_none());
    let options = EngineConfig { getter_batches_per_step: 1, ..options };
    let mut resumed = StateEngine::new(reader.clone(), spec(), options).unwrap(); resumed.restore_discovery(progress).unwrap();
    assert!(matches!(resumed.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
    assert_eq!(resumed.discovery_checkpoint().unwrap().unwrap().getters.unwrap().configs.len(), configs + 1);
    finish(&mut resumed).await;
    assert_eq!(resumed.snapshot().unwrap().markets.len(), 4);
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind == "configs").count(), 4);
}

async fn partial_getters(reader: Arc<MockReader>) -> StateEngine<MockReader> {
    let options = EngineConfig { read_batch_size: 1, getter_batches_per_step: 1, ..config() };
    let mut engine = StateEngine::new(reader, spec(), options).unwrap();
    for _ in 0..20 {
        assert!(matches!(engine.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
        if engine.discovery_checkpoint().unwrap().unwrap().getters.as_ref().is_some_and(|getters| getters.amounts.len() == 1) { return engine; }
    }
    panic!("one completed getter batch expected");
}

#[tokio::test]
async fn pruning_after_saved_getters_discards_old_values_but_retains_certified_history() {
    let reader = MockReader::new(); four_positions(&reader);
    let mut engine = partial_getters(reader.clone()).await;
    { let mut data = reader.0.lock().unwrap(); data.minimum_state_block = 3; data.advance_head_on_pruned = true; }
    reader.set(3, key(7, 1000), 50, 90); // The previously completed key must also be read again.
    finish(&mut engine).await;
    let snapshot = engine.snapshot().unwrap(); assert_eq!(snapshot.as_of, block(3));
    assert_eq!(snapshot.positions[&key(7, 1000)].amounts.debt, 50);
    let data = reader.0.lock().unwrap();
    assert_eq!(data.position_batches.iter().filter(|(_, keys)| keys.contains(&key(7, 1000))).count(), 2);
    assert_eq!(data.calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
}

#[tokio::test]
async fn reorg_after_saved_getters_discards_all_orphan_results() {
    let reader = MockReader::new(); four_positions(&reader);
    let mut engine = partial_getters(reader.clone()).await;
    { let mut data = reader.0.lock().unwrap(); data.blocks.get_mut(&2).unwrap().hash = B256::repeat_byte(199);
      data.states.clear(); data.logs.clear(); }
    let replacement = key(11, 1000); reader.set(1, replacement, 40, 80); reader.event(replacement, 1);
    finish(&mut engine).await;
    let snapshot = engine.snapshot().unwrap(); assert_eq!(snapshot.positions.len(), 1);
    assert_eq!(snapshot.positions[&replacement].amounts.debt, 40);
    assert_eq!(snapshot.as_of.hash, B256::repeat_byte(199));
}

fn reseal_progress(progress: &mut DiscoveryCheckpoint) {
    let history = (progress.identity, progress.spec, progress.abi_fingerprint, progress.base_checksum,
        progress.epoch, progress.generation, progress.rebuilding, progress.from_block, progress.through,
        progress.target, progress.phase, &progress.positions, &progress.markets);
    let bytes = if let Some(refresh) = progress.refresh_all_config { serde_json::to_vec(&(history, &progress.getters, refresh)).unwrap() }
        else if progress.schema_version == 1 { serde_json::to_vec(&history).unwrap() }
        else { serde_json::to_vec(&(history, &progress.getters)).unwrap() };
    progress.checksum = alloy_primitives::keccak256(bytes);
}

#[tokio::test]
async fn getter_checkpoint_rejects_corruption_regression_and_foreign_cleanup() {
    let reader = MockReader::new(); four_positions(&reader);
    let mut engine = partial_getters(reader).await;
    let first = engine.discovery_checkpoint().unwrap().unwrap();
    let directory = tempfile::tempdir().unwrap(); let store = DiscoveryFileStore::new(directory.path().join("progress.json"));
    store.save(&first).unwrap();
    engine.sync_step().await.unwrap(); let next = engine.discovery_checkpoint().unwrap().unwrap(); store.save(&next).unwrap();
    assert!(store.save(&first).is_err()); assert_eq!(store.load(None, None).unwrap(), Some(next.clone()));
    let mut corrupt = next.clone(); corrupt.getters.as_mut().unwrap().amounts.get_mut(&key(7, 1000)).unwrap().debt += 1;
    assert!(corrupt.validate(None, None).is_err());
    let mut incomplete = next.clone(); incomplete.getters.as_mut().unwrap().amounts.remove(&key(7, 1000)); reseal_progress(&mut incomplete);
    assert!(incomplete.validate(None, None).is_err(), "only a sorted completed prefix may be resumed");
    let foreign = ChainIdentity { chain_id: 10143, ..next.identity };
    assert!(matches!(store.remove_for(Some(&foreign), Some(&spec())), Err(Error::IdentityMismatch(_))));
    assert_eq!(store.load(None, None).unwrap(), Some(next.clone()));
    store.remove_for(Some(&next.identity), Some(&spec())).unwrap(); assert!(store.load(None, None).unwrap().is_none());
}

#[tokio::test]
async fn legacy_discovery_schema_is_read_and_upgraded_without_replaying_history() {
    let reader = MockReader::new(); reader.0.lock().unwrap().head = 4;
    let mut engine = StateEngine::new(reader.clone(), spec(), step_config()).unwrap(); engine.sync_step().await.unwrap();
    let mut legacy = engine.discovery_checkpoint().unwrap().unwrap(); legacy.schema_version = 1; legacy.refresh_all_config = None; reseal_progress(&mut legacy);
    legacy.validate(None, None).unwrap();
    let bytes = serde_json::to_vec(&legacy).unwrap(); assert!(!String::from_utf8_lossy(&bytes).contains("getters"));
    let decoded: DiscoveryCheckpoint = serde_json::from_slice(&bytes).unwrap();
    let mut resumed = StateEngine::new(reader.clone(), spec(), step_config()).unwrap(); resumed.restore_discovery(decoded).unwrap();
    resumed.sync_step().await.unwrap(); assert_eq!(resumed.discovery_checkpoint().unwrap().unwrap().schema_version, 2);
    finish(&mut resumed).await;
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(kind, from)| kind == "logs" && *from == 0).count(), 1);
}


#[tokio::test]
async fn interrupted_large_getter_batch_shrinks_across_restart_until_it_can_finish() {
    let reader = MockReader::new(); four_positions(&reader); reader.0.lock().unwrap().position_delay_ms = 40;
    let options = EngineConfig { read_batch_size: 128, operation_timeout: std::time::Duration::from_millis(100), ..config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    assert!(matches!(engine.sync_step().await, Err(Error::Transport(_))));
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    assert!(progress.getters.as_ref().unwrap().amounts.is_empty());
    assert_eq!(progress.getters.as_ref().unwrap().pending_batch_size, Some(4));
    let mut resumed = StateEngine::new(reader.clone(), spec(), options).unwrap(); resumed.restore_discovery(progress).unwrap();
    for _ in 0..5 {
        match resumed.sync_step().await {
            Ok(SyncStepOutcome::Complete(_)) => break,
            Ok(SyncStepOutcome::Progress(_)) | Err(Error::Transport(_)) => {},
            result => panic!("unexpected result: {result:?}"),
        }
    }
    assert_eq!(resumed.snapshot().expect("adaptive batches must finish").positions.len(), 4);
    let data = reader.0.lock().unwrap();
    assert_eq!(data.position_batches[0].1.len(), 4);
    assert_eq!(data.position_batches[1].1.len(), 2);
    assert!(data.position_batches.iter().any(|(_, keys)| keys.len() == 1));
    assert_eq!(data.position_batches.iter().filter(|(_, keys)| keys.contains(&key(7, 1000))).count(), 2,
        "first key appears once in the interrupted batch and once in its successful retry");
}

#[tokio::test]
async fn warm_getter_pages_resume_without_publishing_offsets_or_partial_state() {
    let reader = MockReader::new(); four_positions(&reader);
    let mut initial = StateEngine::new(reader.clone(), spec(), config()).unwrap(); initial.bootstrap_at(block(1)).await.unwrap();
    let before = initial.checkpoint().unwrap();
    { let mut data = reader.0.lock().unwrap(); data.users.insert(1000, (7..=10).map(Address::repeat_byte).collect()); data.calls.clear(); }
    let options = EngineConfig { reconcile_user_pages_per_sync: 4, reconcile_user_page_size: 1,
        getter_batches_per_step: 1, read_batch_size: 1, ..config() };
    let mut engine = StateEngine::restore(reader.clone(), spec(), options.clone(), before.clone()).unwrap();
    for _ in 0..40 {
        assert!(matches!(engine.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
        if engine.discovery_checkpoint().unwrap().unwrap().getters.as_ref()
            .is_some_and(|getters| getters.user_pages_remaining == 2) { break; }
    }
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    assert_eq!(progress.getters.as_ref().unwrap().user_pages_remaining, 2);
    assert_eq!(engine.checkpoint().unwrap(), before);
    let mut resumed = StateEngine::restore(reader.clone(), spec(), options, before.clone()).unwrap(); resumed.restore_discovery(progress).unwrap();
    assert!(matches!(finish(&mut resumed).await, SyncOutcome::Advanced(_)));
    let checkpoint = resumed.checkpoint().unwrap(); assert_eq!(checkpoint.reconciliation.user_offsets[&1000], 4);
    let pages: Vec<_> = reader.0.lock().unwrap().calls.iter().filter(|(kind, _)| kind.starts_with("users:")).map(|(kind, _)| kind.clone()).collect();
    assert_eq!(pages, vec!["users:1000:0", "users:1000:1", "users:1000:2", "users:1000:3"]);
}


#[tokio::test]
async fn legacy_schema_two_without_config_flag_remains_readable() {
    let reader = MockReader::new(); four_positions(&reader);
    let engine = partial_getters(reader.clone()).await;
    let mut legacy = engine.discovery_checkpoint().unwrap().unwrap();
    legacy.refresh_all_config = None; reseal_progress(&mut legacy);
    legacy.validate(None, None).unwrap();
    let encoded = serde_json::to_vec(&legacy).unwrap();
    assert!(!String::from_utf8_lossy(&encoded).contains("refresh_all_config"));
    let decoded: DiscoveryCheckpoint = serde_json::from_slice(&encoded).unwrap();
    let mut options = config(); options.event_driven_refresh = true;
    let mut resumed = StateEngine::new(reader, spec(), options).unwrap();
    assert!(resumed.restore_discovery(decoded).unwrap()); finish(&mut resumed).await;
    resumed.checkpoint().unwrap().validate(None, None).unwrap();
}

use std::{collections::BTreeMap, sync::{Arc, Mutex}};

use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::SolEvent;
use async_trait::async_trait;
use barterlend_state::{*, abi::CreditPool, checkpoint::CHECKPOINT_SCHEMA, risk::UNIT, storage::FileStore};

struct Data {
    identity: ChainIdentity,
    blocks: BTreeMap<u64, BlockRef>,
    states: BTreeMap<u64, BTreeMap<PositionKey, PositionAmounts>>,
    logs: Vec<RawLog>,
    users: BTreeMap<u64, Vec<Address>>,
    calls: Vec<(String, u64)>,
    position_batches: Vec<Vec<PositionKey>>,
    globals: PoolRiskConfig,
    market_config: MarketRiskConfig,
    oracle: OracleStatus,
    fail_positions: bool,
    change_target_during_read: bool,
    range_limit: u64,
    minimum_state_block: u64,
    advance_head_on_pruned: bool,
    head: u64,
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
            states: BTreeMap::new(), logs: Vec::new(), users: BTreeMap::new(), calls: Vec::new(), position_batches: Vec::new(),
            globals: PoolRiskConfig { liquidation_ltv: U256::from(UNIT * 8 / 10), stale_window: 50, min_debt_remaining: 10 },
            market_config: MarketRiskConfig { grace_period: 10, discount_min: U256::from(UNIT / 100), discount_max: U256::from(UNIT / 5) },
            oracle: OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(100_000) }),
            fail_positions: false, change_target_during_read: false, range_limit: 100,
            minimum_state_block: 0, advance_head_on_pruned: false, head: 40,
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
    async fn head(&self, _: Commitment) -> Result<BlockRef> {
        let data = self.0.lock().unwrap(); Ok(data.blocks[&data.head])
    }
    async fn block(&self, number: u64, _: Commitment) -> Result<Option<BlockRef>> {
        Ok(self.0.lock().unwrap().blocks.get(&number).copied())
    }
    async fn describe_pool(&self, spec: &PoolSpec, at: &BlockRef) -> Result<PoolDescriptor> {
        self.0.lock().unwrap().calls.push(("descriptor".into(), at.number));
        Ok(PoolDescriptor { address: spec.address, version: 5,
            asset: TokenDescriptor { address: Address::repeat_byte(3), decimals: 6 },
            collateral: TokenDescriptor { address: Address::repeat_byte(4), decimals: 18 },
            oracle: Address::repeat_byte(5), code_hash: B256::repeat_byte(6) })
    }
    async fn read_positions(&self, _: Address, keys: &[PositionKey], at: &BlockRef) -> Result<BTreeMap<PositionKey, PositionAmounts>> {
        let mut data = self.0.lock().unwrap();
        data.calls.push(("positions".into(), at.number));
        data.position_batches.push(keys.to_vec());
        if data.fail_positions { return Err(Error::Transport("primitive unavailable".into())); }
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
        let mut data = self.0.lock().unwrap(); data.calls.push(("configs".into(), at.number));
        Ok(markets.iter().map(|&market| (market, data.market_config)).collect())
    }
    async fn read_oracle(&self, _: &PoolDescriptor, at: &BlockRef) -> Result<OracleStatus> {
        let mut data = self.0.lock().unwrap(); data.calls.push(("oracle".into(), at.number)); Ok(data.oracle.clone())
    }
    async fn logs(&self, _: Address, from: u64, to: u64) -> Result<Vec<RawLog>> {
        let mut data = self.0.lock().unwrap(); data.calls.push(("logs".into(), from));
        if to - from + 1 > data.range_limit {
            return Err(Error::Rpc { code: -32005, message: "block range too wide".into() });
        }
        Ok(data.logs.iter().filter(|log| log.block_number >= from && log.block_number <= to).cloned().rev().collect())
    }
    async fn market_users(&self, _: Address, market: u64, start: u64, limit: u64, at: &BlockRef) -> Result<Vec<Address>> {
        let mut data = self.0.lock().unwrap(); data.calls.push((format!("users:{market}:{start}"), at.number));
        Ok(data.users.get(&market).into_iter().flatten().skip(start as usize).take(limit as usize).copied().collect())
    }
    async fn active_markets(&self, _: Address, _: &BlockRef) -> Result<Vec<u64>> { Ok(vec![1000]) }
}

#[tokio::test]
async fn census_retains_collateral_only_arbitrary_markets_without_phantom_debt() {
    let reader = MockReader::new();
    let position = key(7, u64::MAX - 100);
    reader.set(1, position, 0, 400);
    reader.event(position, 1);
    reader.event(key(8, 0), 1); // Borrowed emitted by sweep, getter is authoritative zero.
    reader.0.lock().unwrap().range_limit = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    let snapshot = engine.bootstrap_at(block(1)).await.unwrap();
    assert_eq!(snapshot.positions.len(), 1);
    assert_eq!(snapshot.positions[&position].amounts, PositionAmounts { debt: 0, collateral: 400 });
    assert!(!snapshot.markets.contains_key(&0),"empty inactive market must be evicted");
    assert!(snapshot.markets.contains_key(&(u64::MAX - 100)));
    assert!(reader.0.lock().unwrap().calls.iter().filter(|(name, _)| name == "positions").all(|(_, number)| *number == 1));
}

#[tokio::test]
async fn primitive_failure_never_advances_checkpoint_and_retry_converges() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    let before = engine.checkpoint().unwrap();
    reader.set(2, position, 50, 200); reader.event(position, 2);
    reader.0.lock().unwrap().fail_positions = true;
    assert!(engine.sync_to(block(2)).await.is_err());
    assert_eq!(engine.checkpoint().unwrap(), before);
    reader.0.lock().unwrap().fail_positions = false;
    assert!(matches!(engine.sync_to(block(2)).await.unwrap(), SyncOutcome::Advanced(_)));
    assert_eq!(engine.snapshot().unwrap().positions[&position].amounts.debt, 50);
}

#[tokio::test]
async fn oracle_outage_keeps_raw_position_then_price_and_time_update_without_logs() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.0.lock().unwrap().oracle = OracleStatus::Unavailable(OracleFailure::Rpc("oracle outage".into()));
    engine.sync_to(block(2)).await.unwrap();
    assert_eq!(engine.snapshot().unwrap().positions[&position].amounts.debt, 100);
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Unavailable(_)));
    reader.0.lock().unwrap().oracle = OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(100_000) });
    engine.sync_to(block(3)).await.unwrap();
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Ready(PositionRisk { eligible: false, .. })));
    let outcome = engine.sync_to(block(12)).await.unwrap();
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Ready(PositionRisk { eligible: true, .. })));
    assert!(matches!(outcome, SyncOutcome::Advanced(UpdateBatch { changes, .. }) if changes.iter().any(|change| matches!(change, StateUpdate::PositionUpsert(_)))));
}

#[tokio::test]
async fn stale_equality_advances_raw_state_and_recovery_is_explicit() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.0.lock().unwrap().oracle = OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(150) });
    engine.sync_to(block(2)).await.unwrap();
    assert!(matches!(engine.snapshot().unwrap().oracle, OracleStatus::Unavailable(OracleFailure::Stale { .. })));
    assert_eq!(engine.snapshot().unwrap().as_of, block(2));
}

#[tokio::test]
async fn catchup_uses_only_target_state_and_same_head_is_idempotent() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.set(30, position, 20, 40); reader.event(position, 10); reader.event(position, 30);
    { let mut data = reader.0.lock().unwrap(); data.minimum_state_block = 30; data.calls.clear(); }
    engine.sync_to(block(30)).await.unwrap();
    assert_eq!(engine.snapshot().unwrap().positions[&position].amounts.debt, 20);
    assert!(reader.0.lock().unwrap().calls.iter().filter(|(name, _)| name != "logs").all(|(_, number)| *number == 30));
    reader.0.lock().unwrap().calls.clear();
    assert!(matches!(engine.sync_to(block(30)).await.unwrap(), SyncOutcome::Unchanged { .. }));
    assert!(reader.0.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn cursor_reorg_rebuilds_with_new_epoch_and_removed_positions() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.set(2, position, 0, 0);
    { let mut data = reader.0.lock().unwrap(); data.blocks.get_mut(&1).unwrap().hash = B256::repeat_byte(99); data.logs.clear(); }
    let outcome = engine.sync_to(block(2)).await.unwrap();
    assert!(matches!(outcome, SyncOutcome::Reset(StateSnapshot { epoch: 2, .. })));
    assert!(engine.snapshot().unwrap().positions.is_empty());
}

#[tokio::test]
async fn missing_old_header_resets_instead_of_assuming_canonical() {
    let reader = MockReader::new();
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.0.lock().unwrap().blocks.remove(&1);
    assert!(matches!(engine.sync_to(block(2)).await.unwrap(), SyncOutcome::Reset(_)));
}

#[tokio::test]
async fn unverifiable_historical_log_header_keeps_old_checkpoint() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let before = engine.checkpoint().unwrap();
    reader.0.lock().unwrap().blocks.remove(&1);
    assert!(matches!(engine.sync_to(block(2)).await, Err(Error::MissingBlock(1))));
    assert_eq!(engine.checkpoint().unwrap(), before);
}

#[tokio::test]
async fn genesis_or_deployment_change_is_rejected_without_cursor_loss() {
    let reader = MockReader::new();
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let before = engine.checkpoint().unwrap();
    reader.0.lock().unwrap().identity.genesis_hash = B256::repeat_byte(90);
    assert!(matches!(engine.sync_to(block(2)).await, Err(Error::IdentityMismatch(_))));
    assert_eq!(engine.checkpoint().unwrap(), before);
    reader.0.lock().unwrap().identity.genesis_hash = block(0).hash;
    reader.0.lock().unwrap().blocks.get_mut(&0).unwrap().hash = B256::repeat_byte(91);
    assert!(matches!(engine.sync_to(block(2)).await, Err(Error::IdentityMismatch(_))));
    assert_eq!(engine.checkpoint().unwrap(), before);
}

#[tokio::test]
async fn target_race_is_not_published() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let before = engine.checkpoint().unwrap();
    reader.event(position, 2); reader.0.lock().unwrap().change_target_during_read = true;
    assert!(matches!(engine.sync_to(block(2)).await, Err(Error::NonCanonical(2))));
    assert_eq!(engine.checkpoint().unwrap(), before);
}

#[tokio::test]
async fn rotating_full_user_scan_rediscovers_old_zero_debt_account() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.event(position, 1);
    reader.0.lock().unwrap().users.insert(1000, vec![position.borrower, key(8, 1000).borrower]);
    let mut options = config(); options.reconcile_user_pages_per_sync = 1; options.reconcile_user_page_size = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); assert!(engine.snapshot().unwrap().positions.is_empty());
    reader.set(2, position, 100, 200); // simulate a provider omission; no new borrower address.
    engine.sync_to(block(2)).await.unwrap();
    assert_eq!(engine.snapshot().unwrap().positions[&position].amounts.debt, 100);
    let checkpoint = engine.checkpoint().unwrap();
    assert_eq!(checkpoint.reconciliation.user_offsets[&1000], 1);
    let mut restored = StateEngine::restore(reader.clone(), spec(), options, checkpoint).unwrap();
    reader.set(3, position, 40, 80);
    restored.sync_to(block(3)).await.unwrap(); // user index 1
    restored.sync_to(block(4)).await.unwrap(); // terminal empty page, restart index 0
    restored.sync_to(block(5)).await.unwrap(); // old user revisited
    assert_eq!(restored.snapshot().unwrap().positions[&position].amounts.debt, 40);
}

#[tokio::test]
async fn config_changes_refresh_collateral_only_markets_and_dynamic_min_debt() {
    let reader = MockReader::new(); let position = key(7, u64::MAX - 100);
    reader.set(1, position, 0, 100); reader.event(position, 1);
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    { let mut data = reader.0.lock().unwrap(); data.globals.min_debt_remaining = 20; data.market_config.grace_period = 30; }
    let outcome = engine.sync_to(block(2)).await.unwrap();
    assert_eq!(engine.snapshot().unwrap().markets[&position.market].grace_period, 30);
    assert!(matches!(outcome, SyncOutcome::Advanced(UpdateBatch { changes, .. }) if changes.iter().any(|change| matches!(change, StateUpdate::PoolConfigChanged(_)))));
}

#[tokio::test]
async fn single_block_rpc_limit_fails_census_instead_of_skipping() {
    let reader = MockReader::new(); reader.0.lock().unwrap().range_limit = 0;
    let mut engine = StateEngine::new(reader, spec(), config()).unwrap();
    assert!(matches!(engine.bootstrap_at(block(1)).await, Err(Error::IncompleteHistory(_))));
    assert!(engine.snapshot().is_none());
}

#[tokio::test]
async fn checkpoint_storage_keeps_valid_file_on_failed_save_and_rejects_corruption() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.set(1, position, 100, 200); reader.event(position, 1);
    let mut engine = StateEngine::new(reader, spec(), config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let checkpoint = engine.checkpoint().unwrap();
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("pool.json");
    let store = FileStore::new(&path);
    store.save(&checkpoint).unwrap();
    assert_eq!(store.load(Some(&checkpoint.snapshot.identity), Some(&spec())).unwrap(), Some(checkpoint.clone()));
    let old_bytes = std::fs::read(&path).unwrap();
    let mut newer_snapshot = checkpoint.snapshot.clone(); newer_snapshot.epoch += 1;
    let newer = Checkpoint::new(newer_snapshot, checkpoint.reconciliation.clone()).unwrap();
    store.save(&newer).unwrap();
    assert!(store.save(&checkpoint).is_err());
    assert_eq!(store.load(None, None).unwrap(), Some(newer));
    // Use a separate fresh file to exercise failures before a successful write.
    std::fs::remove_file(&path).unwrap(); store.save(&checkpoint).unwrap();
    let writer_lock = std::fs::OpenOptions::new().read(true).write(true).open(directory.path().join("pool.json.lock")).unwrap();
    writer_lock.try_lock().unwrap();
    assert!(store.save(&checkpoint).is_err()); assert_eq!(std::fs::read(&path).unwrap(), old_bytes);
    drop(writer_lock);
    let too_small = FileStore::new(&path).with_max_bytes(1).unwrap();
    assert!(too_small.save(&checkpoint).is_err()); assert_eq!(std::fs::read(&path).unwrap(), old_bytes);
    let mut broken = checkpoint.clone(); broken.snapshot.positions.get_mut(&position).unwrap().amounts.debt = 1;
    assert!(broken.validate(None, None).is_err());
    std::fs::write(&path, serde_json::to_vec(&broken).unwrap()).unwrap(); assert!(store.load(None, None).is_err());
    broken = checkpoint.clone(); broken.schema_version = CHECKPOINT_SCHEMA + 1; assert!(broken.validate(None, None).is_err());
    let wrong_chain = ChainIdentity { chain_id: 10143, ..checkpoint.snapshot.identity };
    assert!(checkpoint.validate(Some(&wrong_chain), None).is_err());
    let mut legacy_snapshot = checkpoint.snapshot.clone();
    legacy_snapshot.abi_fingerprint = alloy_primitives::b256!("8c4724beeb2608e37ead60ffe57c7bd66398140741fd2fb6f0884c44052deec8");
    assert!(matches!(Checkpoint::new(legacy_snapshot, checkpoint.reconciliation),
        Err(Error::Checkpoint(message)) if message == "ABI fingerprint mismatch"));
}

#[tokio::test]
async fn history_budgets_and_unsupported_versions_fail_before_ready() {
    let reader = MockReader::new(); let mut options = config(); options.max_blocks_per_sync = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    assert!(matches!(engine.bootstrap_at(block(1)).await, Err(Error::ResourceLimit(_))));
    assert!(engine.snapshot().is_none());
    assert!(matches!(StateEngine::new(reader, PoolSpec { expected_version: 6, ..spec() }, config()), Err(Error::UnsupportedVersion(6))));
}

#[tokio::test]
async fn pruned_bootstrap_target_repeats_getters_without_replaying_verified_census() {
    let reader = MockReader::new(); let position = key(7, 1000);
    reader.event(position, 2); reader.set(3, position, 100, 200);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.minimum_state_block = 3; data.advance_head_on_pruned = true; }
    let mut engine = StateEngine::new(reader.clone(), spec(), config()).unwrap();
    let snapshot = engine.bootstrap().await.unwrap();
    assert_eq!(snapshot.as_of, block(3)); assert_eq!(snapshot.epoch, 1);
    assert_eq!(snapshot.positions[&position].amounts.debt, 100);
    assert!(reader.0.lock().unwrap().calls.iter().any(|(name, number)| name == "positions" && *number == 2));
    assert!(reader.0.lock().unwrap().calls.iter().any(|(name, number)| name == "positions" && *number == 3));
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(name, number)| name == "logs" && *number == 0).count(), 1);
    assert!(engine.discovery_checkpoint().unwrap().is_none());
}


fn event_driven_config() -> EngineConfig {
    EngineConfig { event_driven_refresh: true, reconciliation_interval: std::time::Duration::from_secs(300),
        reconcile_positions_per_sync: 2, reconcile_user_pages_per_sync: 0, ..config() }
}

async fn complete_step(engine: &mut StateEngine<MockReader>) -> SyncOutcome {
    for _ in 0..100 {
        if let SyncStepOutcome::Complete(outcome) = engine.sync_step().await.unwrap() { return outcome; }
    }
    panic!("bounded discovery did not converge");
}

fn admin_config(reader: &MockReader, number: u64) {
    let data = CreditPool::AdminConfigUpdated { admin: Address::repeat_byte(90), key: "liquidationGracePeriod".into(),
        oldValue: U256::from(10), newValue: U256::from(30) }.encode_log_data();
    reader.0.lock().unwrap().logs.push(RawLog { address: pool(), topics: data.topics().to_vec(), data: data.data,
        block_number: number, block_hash: block(number).hash, transaction_hash: B256::repeat_byte(95), log_index: 100, removed: false });
}

#[tokio::test]
async fn event_driven_idle_sync_reads_oracle_but_not_all_positions_or_configs() {
    let reader = MockReader::new();
    for account in 7..=70 { reader.set(1, key(account, 1000), 100, 200); reader.event(key(account, 1000), 1); }
    let mut engine = StateEngine::new(reader.clone(), spec(), event_driven_config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    let shared = engine.snapshot_shared().unwrap();
    assert!(Arc::ptr_eq(&shared, &engine.snapshot_shared().unwrap()));
    { let mut data = reader.0.lock().unwrap(); data.calls.clear(); data.position_batches.clear(); data.head = 2; }
    assert!(matches!(complete_step(&mut engine).await, SyncOutcome::Advanced(_)));
    { let data = reader.0.lock().unwrap();
      assert!(data.position_batches.is_empty());
      assert!(!data.calls.iter().any(|(name,_)| matches!(name.as_str(), "descriptor" | "globals" | "configs") || name.starts_with("users:")));
      assert_eq!(data.calls.iter().filter(|(name,_)| name == "oracle").count(), 1);
    }
    assert_eq!(shared.as_of, block(1), "previous shared snapshot remains immutable");
    reader.0.lock().unwrap().head = 3; complete_step(&mut engine).await;
    assert!(reader.0.lock().unwrap().position_batches.is_empty());
    reader.0.lock().unwrap().head = 4; complete_step(&mut engine).await;
    assert_eq!(reader.0.lock().unwrap().position_batches.iter().map(Vec::len).sum::<usize>(), 2,
        "defensive refresh uses its small interval budget rather than every known borrower");
    assert_eq!(engine.snapshot().unwrap().positions.len(), 64);
    assert!(!reader.0.lock().unwrap().calls.iter().any(|(name,_)| matches!(name.as_str(), "descriptor" | "globals" | "configs")),
        "position reconciliation must not trigger a full metadata/config burst");
}

#[tokio::test]
async fn event_driven_dirty_positions_and_admin_changes_survive_getter_restart() {
    let reader = MockReader::new(); let original = key(7,1000); let new_market = key(8, 1700);
    reader.set(1, original, 100, 200); reader.event(original,1);
    let mut options = event_driven_config(); options.getter_batches_per_step = 1;
    let mut engine = StateEngine::new(reader.clone(), spec(), options.clone()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let base = engine.checkpoint().unwrap();
    reader.set(2, original, 0, 0); reader.event(original,2);
    reader.set(2, new_market, 70, 300); reader.event(new_market,2); admin_config(&reader,2);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.globals.min_debt_remaining = 21; data.market_config.grace_period = 30; }
    assert!(matches!(engine.sync_step().await.unwrap(), SyncStepOutcome::Progress(_)));
    let progress = engine.discovery_checkpoint().unwrap().unwrap();
    assert_eq!(progress.refresh_all_config, Some(true));
    assert_eq!(progress.base_checksum, Some(base.checksum));
    let mut resumed = StateEngine::restore(reader.clone(), spec(), options, base).unwrap();
    assert!(resumed.restore_discovery(progress).unwrap()); complete_step(&mut resumed).await;
    let snapshot = resumed.snapshot().unwrap();
    assert!(!snapshot.positions.contains_key(&original));
    assert_eq!(snapshot.positions[&new_market].amounts.debt, 70);
    assert_eq!(snapshot.globals.min_debt_remaining, 21);
    assert!(snapshot.markets.values().all(|config| config.grace_period == 30));
    resumed.checkpoint().unwrap().validate(None, None).unwrap();
}

#[tokio::test]
async fn event_driven_price_and_maturity_change_without_position_reads() {
    let reader = MockReader::new(); let position = key(7,1000);
    reader.set(1,position,100,200); reader.event(position,1);
    let mut options = event_driven_config(); options.reconciliation_interval = std::time::Duration::from_secs(100_000);
    let mut engine = StateEngine::new(reader.clone(),spec(),options).unwrap(); engine.bootstrap_at(block(1)).await.unwrap();
    { let mut data = reader.0.lock().unwrap(); data.position_batches.clear(); data.head = 2;
      data.oracle = OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(2), updated_at: U256::from(100_000) }); }
    complete_step(&mut engine).await;
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Ready(PositionRisk { eligible:true, .. })));
    { let mut data = reader.0.lock().unwrap(); data.head = 3;
      data.oracle = OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(100_000) }); }
    complete_step(&mut engine).await;
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Ready(PositionRisk { eligible:false, .. })));
    reader.0.lock().unwrap().head = 12; complete_step(&mut engine).await;
    assert!(matches!(engine.snapshot().unwrap().positions[&position].risk, RiskStatus::Ready(PositionRisk { eligible:true, .. })));
    assert!(reader.0.lock().unwrap().position_batches.is_empty());
}

#[tokio::test]
async fn event_driven_reorg_and_identity_changes_do_not_reuse_orphan_amounts() {
    let reader = MockReader::new(); let orphan = key(7,1000); let replacement = key(8,1000);
    reader.set(1,orphan,100,200); reader.event(orphan,1);
    let mut engine = StateEngine::new(reader.clone(),spec(),event_driven_config()).unwrap(); engine.bootstrap_at(block(1)).await.unwrap();
    { let mut data = reader.0.lock().unwrap(); data.head=2; data.blocks.get_mut(&1).unwrap().hash=B256::repeat_byte(200); data.logs.clear(); data.states.clear(); }
    reader.set(2,replacement,50,100); reader.event(replacement,2);
    assert!(matches!(complete_step(&mut engine).await,SyncOutcome::Reset(_)));
    assert!(!engine.snapshot().unwrap().positions.contains_key(&orphan));
    assert_eq!(engine.snapshot().unwrap().positions[&replacement].amounts.debt,50);
    let before=engine.checkpoint().unwrap();
    reader.0.lock().unwrap().identity.chain_id=999;
    assert!(matches!(engine.sync_step().await,Err(Error::IdentityMismatch(_))));
    assert_eq!(engine.checkpoint().unwrap(),before);
}


#[tokio::test]
async fn discovery_builder_keeps_its_base_after_engine_advances() {
    let reader=MockReader::new(); let position=key(7,1000);
    reader.set(1,position,100,200); reader.event(position,1);
    let mut options=event_driven_config(); options.getter_batches_per_step=1;
    let mut engine=StateEngine::new(reader.clone(),spec(),options.clone()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let base=engine.checkpoint().unwrap();
    reader.set(2,position,50,200); reader.event(position,2); reader.0.lock().unwrap().head=2;
    assert!(matches!(engine.sync_step().await.unwrap(),SyncStepOutcome::Progress(_)));
    let builder=engine.discovery_checkpoint_builder().unwrap();
    complete_step(&mut engine).await;
    let progress=tokio::task::spawn_blocking(move || builder.build()).await.unwrap().unwrap();
    assert_eq!(progress.base_checksum,Some(base.checksum));
    assert_ne!(progress.base_checksum,Some(engine.checkpoint().unwrap().checksum));
    let mut resumed=StateEngine::restore(reader,spec(),options,base).unwrap();
    assert!(resumed.restore_discovery(progress).unwrap()); complete_step(&mut resumed).await;
    assert_eq!(resumed.snapshot().unwrap(),engine.snapshot().unwrap());
}

#[tokio::test]
async fn event_driven_rejects_backwards_timestamp_before_publishing_shared_state() {
    let reader=MockReader::new(); let position=key(7,1000);
    reader.set(1,position,100,200); reader.event(position,1);
    let mut engine=StateEngine::new(reader.clone(),spec(),event_driven_config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let old=engine.snapshot_shared().unwrap();
    { let mut data=reader.0.lock().unwrap(); data.head=2; data.blocks.get_mut(&2).unwrap().timestamp=99; }
    assert!(engine.sync_step().await.is_err());
    assert!(Arc::ptr_eq(&old,&engine.snapshot_shared().unwrap()));
}


#[tokio::test]
async fn event_driven_rejects_empty_chain_or_deployment_identity() {
    for invalid in 0..3 {
        let reader = MockReader::new();
        { let mut data = reader.0.lock().unwrap(); data.head = 2;
          match invalid {
              0 => data.identity.chain_id = 0,
              1 => data.identity.genesis_hash = B256::ZERO,
              _ => data.blocks.get_mut(&spec().deployment_block).unwrap().hash = B256::ZERO,
          }
        }
        let mut engine = StateEngine::new(reader, spec(), event_driven_config()).unwrap();
        assert!(matches!(engine.sync_step().await, Err(Error::IdentityMismatch(_))));
        assert!(engine.snapshot().is_none());
    }
}


#[tokio::test]
async fn event_driven_restore_refreshes_metadata_once_then_only_rotates_positions() {
    let reader=MockReader::new();let position=key(7,1000);
    reader.set(1,position,100,200);reader.event(position,1);
    let options=event_driven_config();
    let mut original=StateEngine::new(reader.clone(),spec(),options.clone()).unwrap();
    original.bootstrap_at(block(1)).await.unwrap();
    let mut engine=StateEngine::restore(reader.clone(),spec(),options,original.checkpoint().unwrap()).unwrap();
    { let mut data=reader.0.lock().unwrap();data.calls.clear();data.head=2; }
    complete_step(&mut engine).await;
    for kind in ["descriptor","globals","configs"] {
        assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(name,_)|name==kind).count(),1,"{kind} must refresh after restore");
    }
    { let mut data=reader.0.lock().unwrap();data.calls.clear();data.position_batches.clear();data.head=5; }
    complete_step(&mut engine).await;
    let data=reader.0.lock().unwrap();
    assert!(!data.position_batches.is_empty(),"position reconciliation remains scheduled");
    assert!(!data.calls.iter().any(|(name,_)|matches!(name.as_str(),"descriptor"|"globals"|"configs")));
}

#[tokio::test]
async fn restored_getter_metadata_is_rechecked_without_repeating_completed_amounts() {
    let reader=MockReader::new();let first=key(7,1000);let second=key(8,1000);
    for position in [first,second] { reader.set(1,position,100,200);reader.event(position,1); }
    let mut options=event_driven_config();options.getter_batches_per_step=1;options.read_batch_size=1;
    let mut engine=StateEngine::new(reader.clone(),spec(),options.clone()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();let base=engine.checkpoint().unwrap();
    for position in [first,second] { reader.set(2,position,90,200);reader.event(position,2); }
    reader.0.lock().unwrap().head=2;
    let mut saved=None;
    for _ in 0..30 {
        assert!(matches!(engine.sync_step().await.unwrap(),SyncStepOutcome::Progress(_)));
        let progress=engine.discovery_checkpoint().unwrap().unwrap();
        if progress.getters.as_ref().is_some_and(|getters|getters.amounts.len()==1) { saved=Some(progress);break; }
    }
    let saved=saved.expect("one completed amount batch");
    let completed=*saved.getters.as_ref().unwrap().amounts.keys().next().unwrap();
    let mut resumed=StateEngine::restore(reader.clone(),spec(),options,base).unwrap();
    assert!(resumed.restore_discovery(saved).unwrap());
    { let mut data=reader.0.lock().unwrap();data.calls.clear();data.position_batches.clear(); }
    complete_step(&mut resumed).await;
    let data=reader.0.lock().unwrap();
    assert!(!data.position_batches.iter().flatten().any(|key|*key==completed),"saved borrower amounts must survive metadata validation");
    for kind in ["descriptor","globals","configs"] {
        assert_eq!(data.calls.iter().filter(|(name,_)|name==kind).count(),1,"restored {kind} needs exactly one live check");
    }
    assert_eq!(resumed.snapshot().unwrap().positions[&first].amounts.debt,90);
    assert_eq!(resumed.snapshot().unwrap().positions[&second].amounts.debt,90);
}

#[tokio::test]
async fn event_driven_known_market_changes_do_not_reread_unchanged_configs() {
    let reader=MockReader::new(); let position=key(7,1000);
    reader.set(1,position,100,200); reader.event(position,1);
    let mut engine=StateEngine::new(reader.clone(),spec(),event_driven_config()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.set(2,position,75,180); reader.event(position,2);
    { let mut data=reader.0.lock().unwrap(); data.calls.clear(); data.head=2; }
    complete_step(&mut engine).await;
    assert_eq!(engine.snapshot().unwrap().positions[&position].amounts.debt,75);
    assert!(!reader.0.lock().unwrap().calls.iter().any(|(name,_)|name=="configs" || name=="globals"));
    // Event-discovered arbitrary markets still require their effective config.
    let fresh=key(9,1750); reader.set(3,fresh,50,100); reader.event(fresh,3);
    { let mut data=reader.0.lock().unwrap(); data.calls.clear(); data.head=3; }
    complete_step(&mut engine).await;
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(name,_)|name=="configs").count(),1);
    assert!(engine.snapshot().unwrap().markets.contains_key(&1750));
    // Any admin event refreshes every effective config, not just event markets.
    admin_config(&reader,4);
    { let mut data=reader.0.lock().unwrap(); data.calls.clear(); data.head=4; data.market_config.grace_period=99; }
    complete_step(&mut engine).await;
    assert!(engine.snapshot().unwrap().markets.values().all(|market|market.grace_period==99));
    assert_eq!(reader.0.lock().unwrap().calls.iter().filter(|(name,_)|name=="configs").count(),1);
}

#[tokio::test]
async fn ordered_position_merge_matches_cold_state_with_same_second_blocks() {
    let reader=MockReader::new();
    for account in [10,30,50] { reader.set(1,key(account,1000),100,200); reader.event(key(account,1000),1); }
    let options=event_driven_config();
    let mut engine=StateEngine::new(reader.clone(),spec(),options.clone()).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap(); let previous=engine.snapshot_shared().unwrap();
    // Cover inserts before/after old keys, a deletion, collateral-only debt-zero,
    // and one unchanged record in the same timestamp shared by adjacent blocks.
    for (account,debt,collateral) in [(5,70,150),(10,0,0),(30,0,50),(70,80,180)] {
        reader.set(2,key(account,1000),debt,collateral); reader.event(key(account,1000),2);
    }
    { let mut data=reader.0.lock().unwrap(); data.head=2; data.blocks.get_mut(&2).unwrap().timestamp=100; }
    let outcome=complete_step(&mut engine).await;
    let SyncOutcome::Advanced(batch)=outcome else { panic!("expected incremental update"); };
    assert!(batch.changes.contains(&StateUpdate::PositionRemoved(key(10,1000))));
    let latest=engine.snapshot().unwrap(); assert_eq!(latest.positions.len(),4);
    assert_eq!(latest.positions[&key(50,1000)],previous.positions[&key(50,1000)]);
    assert_eq!(previous.positions.len(),3,"shared old snapshot must remain immutable");
    let at=latest.as_of;
    let mut cold=StateEngine::new(reader.clone(),spec(),options.clone()).unwrap();
    let rebuilt=cold.bootstrap_at(at).await.unwrap();
    for (key,record) in &latest.positions {
        assert_eq!(record.amounts,rebuilt.positions[key].amounts);
        assert_eq!(record.risk,rebuilt.positions[key].risk);
    }
    // Even within the same second, changing oracle inputs must invalidate risk.
    { let mut data=reader.0.lock().unwrap(); data.head=3; data.blocks.get_mut(&3).unwrap().timestamp=100;
      data.oracle=OracleStatus::Ready(Price { numerator:U256::from(1),denominator:U256::from(10),updated_at:U256::from(100_000) }); }
    complete_step(&mut engine).await;
    assert_ne!(engine.snapshot().unwrap().positions[&key(50,1000)].risk,previous.positions[&key(50,1000)].risk);
    let rebuilt=cold.bootstrap_at(engine.snapshot().unwrap().as_of).await.unwrap();
    for (key,record) in &engine.snapshot().unwrap().positions { assert_eq!(record.risk,rebuilt.positions[key].risk); }
    engine.checkpoint().unwrap().validate(None,None).unwrap();
}

#[tokio::test]
async fn staged_reconciliation_fills_earlier_keys_and_keeps_checkpoints_valid() {
    let reader = MockReader::new();
    let later = key(9, 1000); let earlier = key(7, 1000);
    reader.set(1, later, 100, 200); reader.event(later, 1);
    let options = EngineConfig { read_batch_size: 1, reconcile_positions_per_sync: 1,
        reconcile_user_pages_per_sync: 1, reconcile_user_page_size: 1, ..config() };
    let mut engine = StateEngine::new(reader.clone(), spec(), options).unwrap();
    engine.bootstrap_at(block(1)).await.unwrap();
    reader.set(2, earlier, 20, 50);
    { let mut data = reader.0.lock().unwrap(); data.head = 2; data.users.insert(1000, vec![earlier.borrower]); }
    let directory = tempfile::tempdir().unwrap();
    let store = storage::DiscoveryFileStore::new(directory.path().join("progress.json"));
    let mut completed = false;
    for _ in 0..100 {
        let result = engine.sync_step().await.unwrap();
        if let Some(progress) = engine.discovery_checkpoint().unwrap() {
            progress.validate(None, None).unwrap(); store.save(&progress).unwrap();
        }
        if matches!(result, SyncStepOutcome::Complete(_)) { completed = true; break; }
    }
    assert!(completed);
    let snapshot = engine.snapshot().unwrap();
    assert_eq!(snapshot.positions[&earlier].amounts.debt, 20);
    assert_eq!(snapshot.positions[&later].amounts.debt, 100);
}

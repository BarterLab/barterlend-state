//! Current census completeness, long-gap recovery and isolation from full SDK checkpoints.
use std::{collections::BTreeMap, sync::{Arc, Mutex}, time::Duration};
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::SolEvent;
use async_trait::async_trait;
use barterlend_state::{*, abi::{CreditPool, ABI_FINGERPRINT}, risk::UNIT, storage::FileStore};

const START: u64 = 1_700_000_000;
const DAY: u64 = 86_400;
fn pool() -> Address { Address::repeat_byte(1) }
fn key(id: u8, market: u64) -> PositionKey { PositionKey { pool: pool(), market, borrower: Address::repeat_byte(id) } }
fn header(n: u64, fork: u8) -> BlockRef {
    BlockRef { number:n, hash:keccak256([n.to_be_bytes().as_slice(), &[if n==0 {0}else{fork}]].concat()),
        parent_hash:keccak256([n.saturating_sub(1).to_be_bytes().as_slice(), &[if n<=1 {0}else{fork}]].concat()),
        timestamp: START+n*3/10, commitment:Commitment::Finalized }
}
fn spec() -> PoolSpec { PoolSpec { address:pool(), deployment_block:0, expected_version:5, deployment_block_hash:Some(header(0,0).hash) } }
fn identity() -> ChainIdentity { ChainIdentity { chain_id:143, genesis_hash:header(0,0).hash } }
fn options() -> EngineConfig { EngineConfig { expected_identity:Some(identity()), event_driven_refresh:true,
    getter_batches_per_step:2, read_batch_size:2, reconcile_positions_per_sync:0, reconcile_user_pages_per_sync:0,
    reconcile_user_page_size:2, reconciliation_interval:Duration::from_secs(60), ..Default::default() } }
struct Data {
    head:u64, fork:u8, states:BTreeMap<PositionKey,PositionAmounts>, logs:Vec<RawLog>,
    reads:Vec<(String,u64)>, ranges:Vec<(u64,u64)>, fail_page:Option<u64>, duplicate_page:bool,
    malformed_positions:bool, reorg_on_read:bool, prune_page:bool, unavailable_header:Option<u64>,
}
struct Reader(Mutex<Data>);
impl Reader {
    fn new() -> Arc<Self> { Arc::new(Self(Mutex::new(Data { head:300_000,fork:0,states:BTreeMap::new(),logs:Vec::new(),
        reads:Vec::new(),ranges:Vec::new(),fail_page:None,duplicate_page:false,malformed_positions:false,reorg_on_read:false,prune_page:false,unavailable_header:None }))) }
    fn set(&self,id:u8,market:u64,debt:u128,collateral:u128) { self.0.lock().unwrap().states.insert(key(id,market),PositionAmounts {debt,collateral}); }
    fn schedule() -> MarketSchedule { MarketSchedule { start:START,durations:[DAY;3],active:[1;3],extensions:[0;3] } }
}
#[async_trait]
impl StateReader for Reader {
    async fn identity(&self)->Result<ChainIdentity> {Ok(identity())}
    async fn head(&self,_:Commitment)->Result<BlockRef> {let d=self.0.lock().unwrap();Ok(header(d.head,d.fork))}
    async fn block(&self,n:u64,_:Commitment)->Result<Option<BlockRef>> {
        let d=self.0.lock().unwrap();if d.unavailable_header==Some(n) {return Err(Error::MissingBlock(n));}
        Ok((n<=d.head).then(||header(n,d.fork)))
    }
    async fn market_schedule(&self,_:Address,at:&BlockRef)->Result<MarketSchedule> {self.0.lock().unwrap().reads.push(("schedule".into(),at.number));Ok(Self::schedule())}
    async fn active_markets(&self,_:Address,at:&BlockRef)->Result<Vec<u64>> {Ok(vec![START+((at.timestamp-START)/DAY+1)*DAY])}
    async fn market_users(&self,_:Address,market:u64,start:u64,limit:u64,at:&BlockRef)->Result<Vec<Address>> {
        let mut d=self.0.lock().unwrap();d.reads.push((format!("users:{market}:{start}"),at.number));
        if d.fail_page==Some(start) {return Err(Error::Transport("page unavailable".into()));}
        if d.prune_page {d.prune_page=false;d.head+=1;return Err(Error::MissingBlock(at.number));}
        let offset=if d.duplicate_page {0}else{start};
        Ok(d.states.keys().filter(|k|k.market==market).skip(offset as usize).take(limit as usize).map(|k|k.borrower).collect())
    }
    async fn describe_pool(&self,_:&PoolSpec,at:&BlockRef)->Result<PoolDescriptor> {
        self.0.lock().unwrap().reads.push(("descriptor".into(),at.number));
        Ok(PoolDescriptor {address:pool(),version:5,asset:TokenDescriptor{address:Address::repeat_byte(2),decimals:18},
            collateral:TokenDescriptor{address:Address::repeat_byte(3),decimals:18},oracle:Address::repeat_byte(4),code_hash:B256::repeat_byte(5)})
    }
    async fn read_positions(&self,_:Address,keys:&[PositionKey],at:&BlockRef)->Result<BTreeMap<PositionKey,PositionAmounts>> {
        let mut d=self.0.lock().unwrap();d.reads.push(("amounts".into(),at.number));
        if d.reorg_on_read {d.reorg_on_read=false;d.fork+=1;}
        let mut values:BTreeMap<_,_>=keys.iter().map(|k|(*k,d.states.get(k).copied().unwrap_or_default())).collect();
        if d.malformed_positions {values.pop_first();} Ok(values)
    }
    async fn read_globals(&self,_:Address,_:&BlockRef)->Result<PoolRiskConfig> {Ok(PoolRiskConfig{liquidation_ltv:U256::from(UNIT*8/10),stale_window:60,min_debt_remaining:10})}
    async fn read_market_configs(&self,_:Address,markets:&[u64],_:&BlockRef)->Result<BTreeMap<u64,MarketRiskConfig>> {
        Ok(markets.iter().map(|m|(*m,MarketRiskConfig{grace_period:10,discount_min:U256::from(UNIT/100),discount_max:U256::from(UNIT/5)})).collect())
    }
    async fn read_oracle(&self,_:&PoolDescriptor,at:&BlockRef)->Result<OracleStatus> {Ok(OracleStatus::Ready(Price{numerator:U256::from(1),denominator:U256::from(1),updated_at:U256::from(at.timestamp)}))}
    async fn logs(&self,_:Address,from:u64,to:u64)->Result<Vec<RawLog>> {
        let mut d=self.0.lock().unwrap();d.ranges.push((from,to));
        Ok(d.logs.iter().filter(|l|l.block_number>=from && l.block_number<=to).cloned().collect())
    }
}
async fn finish(engine:&mut CurrentStateEngine<Reader>)->CurrentSnapshot {
    for _ in 0..300 {if matches!(engine.sync_step().await.unwrap().outcome,SyncStepOutcome::Complete(_)) {return engine.snapshot_shared().unwrap();}}
    panic!("bounded current census did not complete")
}
fn engine(reader:Arc<Reader>)->CurrentStateEngine<Reader> {CurrentStateEngine::new(reader,spec(),options(),CurrentStateConfig::default()).unwrap()}

#[tokio::test]
async fn month_restart_reads_current_storage_and_finds_new_and_closed_debts_without_logs() {
    let r=Reader::new();r.set(8,START+DAY,100,200);r.set(9,START+DAY,20,30);
    let mut e=engine(r.clone());let first=finish(&mut e).await;assert_eq!(first.state.positions.len(),2);
    let saved=e.checkpoint_builder().unwrap().build().unwrap();drop(e);
    {let mut d=r.0.lock().unwrap();d.head+=8_640_000;d.reads.clear();d.ranges.clear();d.unavailable_header=Some(first.state.as_of.number);}
    r.set(8,START+DAY,0,0);r.set(9,START+DAY,7,30);r.set(10,START+30*DAY,50,90);
    let mut e=CurrentStateEngine::restore(r.clone(),spec(),options(),CurrentStateConfig::default(),saved).unwrap();
    let fresh=finish(&mut e).await;
    assert_eq!(fresh.coverage,CurrentCoverage::AllDebtPositionsV5);assert_eq!(fresh.state.epoch,first.state.epoch+1);
    assert!(!fresh.state.positions.contains_key(&key(8,START+DAY)));
    assert_eq!(fresh.state.positions[&key(9,START+DAY)].amounts.debt,7);
    assert_eq!(fresh.state.positions[&key(10,START+30*DAY)].amounts.debt,50);
    let d=r.0.lock().unwrap();assert!(d.ranges.is_empty());assert!(d.reads.iter().all(|(_,n)|*n==d.head));
}

#[tokio::test]
async fn small_tail_uses_events_and_unchanged_head_does_not_enumerate_users() {
    let r=Reader::new();r.set(8,START+DAY,100,200);let mut e=engine(r.clone());let first=finish(&mut e).await;
    {let mut d=r.0.lock().unwrap();d.head+=1;let n=d.head;d.reads.clear();d.states.get_mut(&key(8,START+DAY)).unwrap().debt=90;
        let event=CreditPool::DebtRepaid{payer:Address::repeat_byte(8),account:Address::repeat_byte(8),market:START+DAY,amount:10}.encode_log_data();
        d.logs.push(RawLog{address:pool(),topics:event.topics().to_vec(),data:event.data,block_number:n,block_hash:header(n,0).hash,transaction_hash:B256::repeat_byte(9),log_index:0,removed:false});}
    let fresh=finish(&mut e).await;assert_eq!(fresh.state.epoch,first.state.epoch);assert_eq!(fresh.state.positions[&key(8,START+DAY)].amounts.debt,90);
    assert_eq!(r.0.lock().unwrap().ranges,vec![(first.state.as_of.number+1,first.state.as_of.number+1)]);
    r.0.lock().unwrap().reads.clear();finish(&mut e).await;assert!(r.0.lock().unwrap().reads.is_empty());
}

#[tokio::test]
async fn incomplete_pages_resume_durably_without_repeating_completed_pages() {
    let r=Reader::new();for id in 8..13 {r.set(id,START+DAY,100,200);}r.0.lock().unwrap().fail_page=Some(2);
    let mut e=engine(r.clone());assert!(matches!(e.sync_step().await.unwrap().outcome,SyncStepOutcome::Progress(_)));
    let mut failed=false;for _ in 0..12 {if e.sync_step().await.is_err() {failed=true;break;}}
    assert!(failed);assert!(e.snapshot_shared().is_none());
    let dir=tempfile::tempdir().unwrap();let store=CurrentFileStore::new(dir.path().join("current.json"));
    store.save(&e.checkpoint_builder().unwrap().build().unwrap()).unwrap();
    assert!(FileStore::new(store.path()).load(None,None).is_err());
    let saved=store.load(Some(&identity()),Some(&spec())).unwrap().unwrap();
    {let mut d=r.0.lock().unwrap();d.fail_page=None;d.reads.clear();}
    let mut e=CurrentStateEngine::restore(r.clone(),spec(),options(),CurrentStateConfig::default(),saved).unwrap();
    assert_eq!(finish(&mut e).await.state.positions.len(),5);
    assert!(!r.0.lock().unwrap().reads.iter().any(|(s,_)|*s==format!("users:{}:0",START+DAY)));
}

#[tokio::test]
async fn unfinished_month_old_census_restarts_at_head_without_old_state_reads() {
    let r=Reader::new();r.set(8,START+DAY,100,200);let mut e=engine(r.clone());e.sync_step().await.unwrap();
    let saved=e.checkpoint_builder().unwrap().build().unwrap();
    {let mut d=r.0.lock().unwrap();d.head+=8_640_000;d.reads.clear();}
    let mut e=CurrentStateEngine::restore(r.clone(),spec(),options(),CurrentStateConfig::default(),saved).unwrap();finish(&mut e).await;
    let d=r.0.lock().unwrap();assert!(d.reads.iter().all(|(_,n)|*n==d.head));assert!(d.ranges.is_empty());
}

#[tokio::test]
async fn reorg_discards_partial_census_and_never_falls_back_to_deployment_logs() {
    let r=Reader::new();r.set(8,START+DAY,100,200);let mut e=engine(r.clone());let first=finish(&mut e).await;
    {let mut d=r.0.lock().unwrap();d.fork=1;d.head+=3;d.reorg_on_read=true;}
    let mut rejected=false;for _ in 0..20 {if matches!(e.sync_step().await,Err(Error::NonCanonical(_))) {rejected=true;break;}}
    assert!(rejected);let fresh=finish(&mut e).await;assert_eq!(fresh.state.as_of.hash,header(fresh.state.as_of.number,2).hash);
    assert!(fresh.state.epoch>first.state.epoch);assert!(r.0.lock().unwrap().ranges.is_empty());
}

#[tokio::test]
async fn pruned_page_reanchors_and_missing_position_response_is_not_published() {
    let r=Reader::new();r.set(8,START+DAY,100,200);r.0.lock().unwrap().prune_page=true;let mut e=engine(r.clone());e.sync_step().await.unwrap();
    assert!(matches!(e.sync_step().await,Err(Error::MissingBlock(_))));finish(&mut e).await;
    let r=Reader::new();r.set(8,START+DAY,100,200);r.0.lock().unwrap().malformed_positions=true;let mut e=engine(r);
    let mut failed=false;for _ in 0..20 {if e.sync_step().await.is_err(){failed=true;break;}}
    assert!(failed);assert!(e.snapshot_shared().is_none());
}

#[tokio::test]
async fn duplicate_pages_and_resource_caps_fail_closed() {
    let r=Reader::new();for id in 8..11 {r.set(id,START+DAY,100,200);}r.0.lock().unwrap().duplicate_page=true;
    let mut e=engine(r.clone());let mut failed=false;for _ in 0..10 {if matches!(e.sync_step().await,Err(Error::Decode(_))){failed=true;break;}}
    assert!(failed);assert!(e.snapshot_shared().is_none());
    let mut c=options();c.max_positions=1;let mut e=CurrentStateEngine::new(r,spec(),c,CurrentStateConfig::default()).unwrap();
    e.sync_step().await.unwrap();assert!(matches!(e.sync_step().await,Err(Error::ResourceLimit(_))));
}

#[tokio::test]
async fn explicit_coverage_does_not_claim_arbitrary_collateral_and_checkpoint_tampering_fails() {
    let r=Reader::new();r.set(8,START+DAY,100,200);r.set(9,START+123,0,50);let mut e=engine(r);let state=finish(&mut e).await;
    assert_eq!(state.state.positions.len(),1);assert_eq!(state.coverage,CurrentCoverage::AllDebtPositionsV5);
    let saved=e.checkpoint_builder().unwrap().build().unwrap();let mut value=serde_json::to_value(saved).unwrap();value["schema_version"]=serde_json::json!(2);
    let bad:CurrentCheckpoint=serde_json::from_value(value).unwrap();assert!(bad.validate(None,None).is_err());
}

#[test]
fn schedule_boundaries_extensions_duplicates_and_overflow_are_checked() {
    let s=Reader::schedule();assert_eq!(s.census_markets(START-1,&[],10).unwrap(),Vec::<u64>::new());
    assert_eq!(s.census_markets(START,&[START+DAY],10).unwrap(),vec![START+DAY]);
    assert_eq!(s.census_markets(START+DAY,&[START+2*DAY],10).unwrap(),vec![START+DAY,START+2*DAY]);
    assert!(s.census_markets(START+DAY,&[],10).is_err());assert!(s.census_markets(START,&[START+DAY,START+DAY],10).is_err());
    assert!(s.census_markets(START+100*DAY,&[START+101*DAY],10).is_err());
    let mut s=s;s.durations[1]=DAY+1;assert!(s.census_markets(START,&[],10).is_err());
    let s=MarketSchedule{start:1,durations:[u64::MAX;3],active:[2;3],extensions:[0;3]};assert!(s.census_markets(2,&[],10).is_err());
    let s=MarketSchedule{start:100,durations:[10,20,40],active:[1,2,1],extensions:[5,0,1]};
    assert_eq!(s.census_markets(115,&[120,130,140],10).unwrap(),vec![110,120,130,140]);
}


#[tokio::test]
async fn closed_registry_is_streamed_under_live_cap_and_resumes_without_repeating_pages() {
    let r=Reader::new();for id in 8..128 {r.set(id,START+DAY,0,0);}r.set(200,START+DAY,5,10);
    let mut config=options();config.max_positions=1;config.reconcile_user_page_size=8;
    let policy=CurrentStateConfig::default();
    let mut e=CurrentStateEngine::new(r.clone(),spec(),config.clone(),policy.clone()).unwrap();
    for _ in 0..5 {e.sync_step().await.unwrap();}
    let saved=e.checkpoint_builder().unwrap().build().unwrap();saved.validate(Some(&identity()),Some(&spec())).unwrap();
    let value=serde_json::to_value(&saved).unwrap();assert!(value["census"]["keys"].as_array().unwrap().len()<=1);
    let pages_before=r.0.lock().unwrap().reads.iter().filter(|(s,_)|s.starts_with("users:")).cloned().collect::<Vec<_>>();
    r.0.lock().unwrap().reads.clear();
    let mut e=CurrentStateEngine::restore(r.clone(),spec(),config,policy,saved).unwrap();
    let result=finish(&mut e).await;assert_eq!(result.state.positions.len(),1);assert!(result.state.positions.contains_key(&key(200,START+DAY)));
    assert!(r.0.lock().unwrap().reads.iter().filter(|(s,_)|s.starts_with("users:")).all(|entry|!pages_before.contains(entry)));
}

#[tokio::test]
async fn calendar_age_does_not_exhaust_batch_cap_and_empty_market_rotation_stays_bounded() {
    let r=Reader::new();r.0.lock().unwrap().head=30*DAY*10/3;
    let mut config=options();config.max_markets=1;config.log_chunk_size=1_000_000;
    let policy=CurrentStateConfig {max_calendar_markets:1,max_event_catchup_blocks:1_000_000};
    let mut e=CurrentStateEngine::new(r.clone(),spec(),config,policy).unwrap();
    let first=finish(&mut e).await;assert_eq!(first.state.markets.len(),1);assert!(first.state.positions.is_empty());
    for _ in 0..20 {
        r.0.lock().unwrap().head+=288_000;
        let current=finish(&mut e).await;assert_eq!(current.state.markets.len(),1);assert!(current.state.positions.is_empty());
        e.checkpoint_builder().unwrap().build().unwrap().validate(Some(&identity()),Some(&spec())).unwrap();
    }
}

fn emit_collateral(r:&Reader,market:u64,borrower:u8,collateral:u128) {
    let mut data=r.0.lock().unwrap();data.head+=1;let n=data.head;
    data.states.insert(key(borrower,market),PositionAmounts {debt:0,collateral});
    let event=CreditPool::CollateralDeposited {account:Address::repeat_byte(borrower),market,amount:collateral}.encode_log_data();
    data.logs.push(RawLog {address:pool(),topics:event.topics().to_vec(),data:event.data,block_number:n,block_hash:header(n,0).hash,transaction_hash:keccak256(n.to_be_bytes()),log_index:0,removed:false});
}

#[tokio::test]
async fn mature_debt_is_retained_and_evicted_market_is_rediscovered_by_events() {
    let r=Reader::new();let mature=START+DAY;r.set(8,mature,10,20);
    let mut config=options();config.max_markets=2;
    let mut e=CurrentStateEngine::new(r.clone(),spec(),config,CurrentStateConfig::default()).unwrap();
    let first=finish(&mut e).await;assert_eq!(first.state.markets.len(),2);assert!(first.state.positions.contains_key(&key(8,mature)));
    emit_collateral(&r,mature,8,0);
    let mut removed=false;
    for _ in 0..50 {if let SyncStepOutcome::Complete(outcome)=e.sync_step().await.unwrap().outcome {
        if let SyncOutcome::Advanced(batch)=outcome {removed=batch.changes.contains(&StateUpdate::MarketRemoved(mature));}break;
    }}
    let empty=e.snapshot_shared().unwrap();assert!(removed);assert!(!empty.state.markets.contains_key(&mature));
    emit_collateral(&r,mature,8,30);let fresh=finish(&mut e).await;
    assert!(fresh.state.markets.contains_key(&mature));assert_eq!(fresh.state.positions[&key(8,mature)].amounts.collateral,30);
    assert_eq!(fresh.state.epoch,first.state.epoch);
}

#[tokio::test]
async fn repeated_empty_user_page_is_rejected_without_retaining_all_closed_keys() {
    let r=Reader::new();for id in 8..12 {r.set(id,START+DAY,0,0);}r.0.lock().unwrap().duplicate_page=true;
    let mut config=options();config.max_positions=1;
    let mut e=CurrentStateEngine::new(r,spec(),config,CurrentStateConfig::default()).unwrap();
    let mut rejected=false;for _ in 0..20 {if matches!(e.sync_step().await,Err(Error::Decode(_))) {rejected=true;break;}}
    assert!(rejected);assert!(e.snapshot_shared().is_none());
}

#[tokio::test]
async fn legacy_census_checksum_loads_and_migrates_once_to_streaming() {
    // Preserve the exact old field order: old JSON/checksum must remain readable.
    #[derive(serde::Serialize)]
    struct LegacyCensus {
        identity: ChainIdentity, spec: PoolSpec, target: BlockRef, epoch: u64, generation: u64,
        schedule: Option<MarketSchedule>, active: Option<Vec<u64>>, markets: Option<Vec<u64>>,
        market_cursor: usize, pending_users: BTreeMap<u64,u64>, keys: std::collections::BTreeSet<PositionKey>,
    }
    let r = Reader::new(); let mature = START + DAY; let active = START + 2 * DAY;
    r.set(8, mature, 0, 0); r.set(9, mature, 0, 0); r.set(10, mature, 5, 10);
    let legacy = LegacyCensus { identity: identity(), spec: spec(), target: header(300_000,0), epoch: 1, generation: 1,
        schedule: Some(Reader::schedule()), active: Some(vec![active]), markets: Some(vec![mature, active]),
        market_cursor: 0, pending_users: [(mature,2),(active,0)].into(), keys: [key(8,mature),key(9,mature)].into() };
    let checksum = keccak256(serde_json::to_vec(&(1u32, ABI_FINGERPRINT, CurrentCoverage::AllDebtPositionsV5,
        Option::<Checkpoint>::None, Option::<DiscoveryCheckpoint>::None, &legacy)).unwrap());
    let value = serde_json::json!({"schema_version":1,"abi_fingerprint":ABI_FINGERPRINT,
        "coverage":CurrentCoverage::AllDebtPositionsV5,"state":null,"discovery":null,"census":legacy,"checksum":checksum});
    let saved: CurrentCheckpoint = serde_json::from_value(value).unwrap();
    saved.validate(Some(&identity()),Some(&spec())).unwrap();
    let mut engine = CurrentStateEngine::restore(r.clone(),spec(),options(),CurrentStateConfig::default(),saved).unwrap();
    let migrated = engine.checkpoint_builder().unwrap().build().unwrap();
    assert!(serde_json::to_value(&migrated).unwrap()["census"]["stream"].is_object());
    let snapshot = finish(&mut engine).await;
    assert_eq!(snapshot.state.positions.len(),1);
    assert_eq!(snapshot.state.positions[&key(10,mature)].amounts.debt,5);
    let saved = engine.checkpoint_builder().unwrap().build().unwrap(); r.0.lock().unwrap().reads.clear();
    let mut restored = CurrentStateEngine::restore(r.clone(),spec(),options(),CurrentStateConfig::default(),saved).unwrap();
    finish(&mut restored).await;
    assert!(!r.0.lock().unwrap().reads.iter().any(|(name,_)|name.starts_with("users:")));
}

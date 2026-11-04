//! Actual HTTP counters for bounded historical discovery and age-independent warm restart.
//! The server computes headers on demand; it never allocates the simulated chain history.

use std::{collections::BTreeMap, io::{BufRead, BufReader, Read, Write}, net::{Shutdown, TcpListener, TcpStream}, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}}, thread, time::{Duration, Instant}};
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolEvent};
use barterlend_state::{*, abi::{ABI_FINGERPRINT, CreditPool}, checkpoint::ReconciliationCursor, risk::{UNIT, evaluate_risk}, rpc::RpcConfig, storage::{FileStore, DiscoveryFileStore}};
use serde_json::{Value, json};

const HEIGHTS: [u64; 3] = [10_000, 1_000_000, 100_000_000];
fn pool() -> Address { Address::repeat_byte(1) }
fn header(n: u64) -> BlockRef { BlockRef { number:n, hash:keccak256(n.to_be_bytes()), parent_hash:keccak256(n.saturating_sub(1).to_be_bytes()), timestamp:1_700_000_000+n*3/10, commitment:Commitment::Finalized } }
fn spec() -> PoolSpec { PoolSpec { address:pool(),deployment_block:0,expected_version:5,deployment_block_hash:Some(header(0).hash) } }
fn identity() -> ChainIdentity { ChainIdentity { chain_id:143,genesis_hash:header(0).hash } }
fn key() -> PositionKey { PositionKey { pool:pool(),market:1_699_999_900,borrower:Address::repeat_byte(9) } }
fn globals() -> PoolRiskConfig { PoolRiskConfig { liquidation_ltv:U256::from(UNIT*8/10),stale_window:60,min_debt_remaining:10 } }
fn market() -> MarketRiskConfig { MarketRiskConfig { grace_period:10,discount_min:U256::from(UNIT/100),discount_max:U256::from(UNIT/5) } }
fn descriptor() -> PoolDescriptor { PoolDescriptor { address:pool(),version:5,asset:TokenDescriptor { address:Address::repeat_byte(3),decimals:6 },collateral:TokenDescriptor { address:Address::repeat_byte(4),decimals:18 },oracle:Address::repeat_byte(5),code_hash:keccak256([0x60,0x00]) } }
fn checkpoint(n: u64) -> Checkpoint {
    let at=header(n);let amounts=PositionAmounts { debt:100,collateral:200 };
    let oracle=OracleStatus::Unavailable(OracleFailure::Rpc("fixture oracle unavailable".into()));
    let record=PositionRecord { key:key(),amounts,risk:evaluate_risk(&amounts,key().market,&globals(),&market(),&oracle,at.timestamp),last_changed:at };
    Checkpoint::new(StateSnapshot { identity:identity(),spec:spec(),pool:descriptor(),abi_fingerprint:ABI_FINGERPRINT,epoch:7,as_of:at,globals:globals(),markets:BTreeMap::from([(key().market,market())]),oracle,positions:BTreeMap::from([(key(),record)]) },ReconciliationCursor::default()).unwrap()
}
fn options() -> EngineConfig { EngineConfig { expected_identity:Some(identity()),event_driven_refresh:true,log_chunk_size:100,history_chunks_per_step:2,getter_batches_per_step:16,reconcile_positions_per_sync:0,reconcile_user_pages_per_sync:0,operation_timeout:Duration::from_secs(5),..Default::default() } }
fn word(value: U256) -> Bytes { Bytes::from(value.to_be_bytes::<32>().to_vec()) }
fn address_word(value: Address) -> Bytes { let mut bytes=[0;32];bytes[12..].copy_from_slice(value.as_slice());Bytes::from(bytes.to_vec()) }
fn q(value: &Value) -> u64 { u64::from_str_radix(value.as_str().unwrap().strip_prefix("0x").unwrap(),16).unwrap() }

struct Server { url:String,requests:Arc<Mutex<Vec<Value>>>,head:Arc<AtomicU64>,stall_from:Arc<AtomicU64>,stop:Arc<AtomicBool>,sockets:Arc<Mutex<Vec<TcpStream>>>,worker:Option<thread::JoinHandle<()>> }
impl Server {
    fn new(head: u64) -> Self {
        let listener=TcpListener::bind("127.0.0.1:0").unwrap();let address=listener.local_addr().unwrap();listener.set_nonblocking(true).unwrap();
        let requests=Arc::new(Mutex::new(Vec::new()));let head=Arc::new(AtomicU64::new(head));let stall_from=Arc::new(AtomicU64::new(u64::MAX));let stop=Arc::new(AtomicBool::new(false));let sockets=Arc::new(Mutex::new(Vec::new()));
        let (seen,height,stall,stopping,owned)=(requests.clone(),head.clone(),stall_from.clone(),stop.clone(),sockets.clone());
        let worker=thread::spawn(move || {
            let mut threads=Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream,_))=>{
                        stream.set_nonblocking(false).unwrap();stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();stream.set_write_timeout(Some(Duration::from_secs(2))).unwrap();owned.lock().unwrap().push(stream.try_clone().unwrap());
                        let (seen,height,stall,stop)=(seen.clone(),height.clone(),stall.clone(),stopping.clone());
                        threads.push(thread::spawn(move || {
                            let mut reader=BufReader::new(stream);let mut line=String::new();
                            while !stop.load(Ordering::Relaxed) {
                                line.clear();if reader.read_line(&mut line).unwrap_or(0)==0 { break; } assert!(line.starts_with("POST "));
                                let mut length=0;
                                loop {
                                    line.clear();
                                    if reader.read_line(&mut line).unwrap_or(0)==0 { return; }
                                    if line=="\r\n" { break; }
                                    if let Some(value)=line.to_ascii_lowercase().strip_prefix("content-length:") { length=value.trim().parse::<usize>().unwrap(); }
                                }
                                assert!(length>0 && length<=64*1024);let mut bytes=vec![0;length];if reader.read_exact(&mut bytes).is_err() { break; }
                                let request:Value=serde_json::from_slice(&bytes).unwrap();seen.lock().unwrap().push(request.clone());
                                if request["method"]=="eth_getLogs" && q(&request["params"][0]["fromBlock"])==stall.load(Ordering::Relaxed) { thread::sleep(Duration::from_millis(250)); }
                                let result=reply(&request,height.load(Ordering::Relaxed));
                                let body=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":result})).unwrap();
                                if write!(reader.get_mut(),"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",body.len()).is_err() || reader.get_mut().write_all(&body).is_err() { break; }
                            }
                        }));
                    },
                    Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>thread::sleep(Duration::from_millis(1)),
                    Err(error)=>panic!("local accept: {error}"),
                }
            }
            for stream in owned.lock().unwrap().iter() { let _=stream.shutdown(Shutdown::Both); }
            for thread in threads { thread.join().unwrap(); }
        });
        Self { url:format!("http://{address}"),requests,head,stall_from,stop,sockets,worker:Some(worker) }
    }
    fn reader(&self) -> Arc<HttpReader> { Arc::new(HttpReader::new(&self.url,RpcConfig { min_request_interval:Duration::ZERO,max_retries:0,request_timeout:Duration::from_secs(2),..Default::default() }).unwrap()) }
    fn ranges(&self) -> Vec<(u64,u64)> { self.requests.lock().unwrap().iter().filter(|v|v["method"]=="eth_getLogs").map(|v|(q(&v["params"][0]["fromBlock"]),q(&v["params"][0]["toBlock"]))).collect() }
    fn counts(&self) -> BTreeMap<String,usize> { let mut counts=BTreeMap::new();for request in self.requests.lock().unwrap().iter() { *counts.entry(request["method"].as_str().unwrap().to_owned()).or_default()+=1; } counts }
    fn clear(&self) { self.requests.lock().unwrap().clear(); }
}
impl Drop for Server { fn drop(&mut self) { self.stop.store(true,Ordering::Relaxed);for stream in self.sockets.lock().unwrap().iter() { let _=stream.shutdown(Shutdown::Both); } if let Some(worker)=self.worker.take() { worker.join().unwrap(); } } }

fn call(data: &Bytes) -> Bytes {
    if data.starts_with(&CreditPool::multicallCall::SELECTOR) {
        let calls=CreditPool::multicallCall::abi_decode_validate(data).unwrap();let values:Vec<_>=calls.data.iter().map(call).collect();return Bytes::from(CreditPool::multicallCall::abi_encode_returns(&values));
    }
    let selector=&data[..4];
    if selector==CreditPool::versionCall::SELECTOR { word(U256::from(5u64)) }
    else if selector==CreditPool::assetCall::SELECTOR { address_word(Address::repeat_byte(3)) }
    else if selector==CreditPool::collateralCall::SELECTOR { address_word(Address::repeat_byte(4)) }
    else if selector==CreditPool::priceOracleCall::SELECTOR { address_word(Address::repeat_byte(5)) }
    else if selector==[0x31,0x3c,0xe5,0x67] { word(U256::from(18u64)) }
    else if selector==CreditPool::getActiveMarketsCall::SELECTOR { Bytes::from(CreditPool::getActiveMarketsCall::abi_encode_returns(&Vec::<u64>::new())) }
    else if selector==CreditPool::getLiquidationLTVCall::SELECTOR { word(globals().liquidation_ltv) }
    else if selector==CreditPool::getStalePriceWindowCall::SELECTOR { word(U256::from(globals().stale_window)) }
    else if selector==CreditPool::getMinDebtRemainingAfterLiquidationCall::SELECTOR { word(U256::from(10u64)) }
    else if selector==CreditPool::getLiquidationGracePeriodCall::SELECTOR { word(U256::from(10u64)) }
    else if selector==CreditPool::getLiquidationDiscountMinCall::SELECTOR { word(market().discount_min) }
    else if selector==CreditPool::getLiquidationDiscountMaxCall::SELECTOR { word(market().discount_max) }
    else if selector==CreditPool::debtOfCall::SELECTOR { word(U256::from(101u64)) }
    else if selector==CreditPool::collateralOfCall::SELECTOR { word(U256::from(200u64)) }
    else { Bytes::new() } // Malformed oracle return becomes an explicit unavailable price.
}
fn reply(request: &Value, head: u64) -> Value {
    match request["method"].as_str().unwrap() {
        "eth_chainId"=>json!("0x8f"),
        "eth_getBlockByNumber"=>{ let tag=&request["params"][0];let n=if tag.as_str().unwrap().starts_with("0x") { q(tag) } else { head };let at=header(n);json!({"number":format!("0x{n:x}"),"hash":at.hash,"parentHash":at.parent_hash,"timestamp":format!("0x{:x}",at.timestamp)}) },
        "eth_getCode"=>json!("0x6000"),
        "eth_call"=>{ let data:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();let target:Address=serde_json::from_value(request["params"][0]["to"].clone()).unwrap();if data.starts_with(&[0x31,0x3c,0xe5,0x67]) && target==Address::repeat_byte(3) { json!(word(U256::from(6u64))) } else { json!(call(&data)) } },
        "eth_getLogs"=>{
            let from=q(&request["params"][0]["fromBlock"]);let to=q(&request["params"][0]["toBlock"]);
            let events:Vec<_>=[50,head-2].into_iter().filter(|block|(from..=to).contains(block)).map(|event_block| { let event=CreditPool::Borrowed { account:key().borrower,market:key().market,assetsBorrowed:1,debtIncurred:1,endAPR:U256::ZERO }.encode_log_data();json!({"address":pool(),"topics":event.topics(),"data":event.data,"blockNumber":format!("0x{event_block:x}"),"blockHash":header(event_block).hash,"transactionHash":B256::repeat_byte(8),"logIndex":"0x0","removed":false}) }).collect();json!(events)
        },
        method=>panic!("unexpected RPC method {method}"),
    }
}

#[tokio::test]
async fn restored_checkpoint_reads_only_constant_tail_at_10k_1m_100m() {
    let mut expected=None;
    for head in HEIGHTS {
        let server=Server::new(head);let directory=tempfile::tempdir().unwrap();let store=FileStore::new(directory.path().join("state.json"));store.save(&checkpoint(head-3)).unwrap();
        let saved=store.load(Some(&identity()),Some(&spec())).unwrap().unwrap();let mut engine=StateEngine::restore(server.reader(),spec(),options(),saved).unwrap();
        assert!(server.counts().is_empty(),"restoration must perform no network I/O");
        let started=Instant::now();let outcome=engine.sync_step().await.unwrap();let wall=started.elapsed();assert!(matches!(outcome,SyncStepOutcome::Complete(SyncOutcome::Advanced(_))));
        assert_eq!(server.ranges(),vec![(head-2,head)]);let snapshot=engine.snapshot().unwrap();assert_eq!(snapshot.as_of,header(head));assert_eq!(snapshot.epoch,7);assert_eq!(snapshot.positions[&key()].amounts.debt,101);
        let counts=server.counts();if let Some(expected)=&expected { assert_eq!(&counts,expected); } else { expected=Some(counts.clone()); }
        for request in server.requests.lock().unwrap().iter().filter(|r|r["method"]=="eth_call") { assert_eq!(request["params"][1],json!(format!("0x{head:x}"))); }
        assert!(wall<Duration::from_secs(5));eprintln!("LONG_OPERATIONS {}",json!({"case":"warm","head":head,"rpc":counts,"log_ranges":server.ranges(),"wall_ms":wall.as_millis()}));
        let published=engine.snapshot_shared().unwrap();server.clear();let unchanged=Instant::now();assert!(matches!(engine.sync_step().await.unwrap(),SyncStepOutcome::Complete(SyncOutcome::Unchanged { .. })));assert!(server.ranges().is_empty());assert!(!server.counts().contains_key("eth_call"));assert!(Arc::ptr_eq(&published,&engine.snapshot_shared().unwrap()));
        eprintln!("LONG_OPERATIONS {}",json!({"case":"unchanged","head":head,"rpc":server.counts(),"wall_ms":unchanged.elapsed().as_millis()}));
    }
}

#[tokio::test]
async fn cold_step_and_durable_restart_bound_work_independently_of_chain_age() {
    let mut expected=None;
    for head in HEIGHTS {
        let server=Server::new(head);let reader=server.reader();let mut engine=StateEngine::new(reader.clone(),spec(),options()).unwrap();let started=Instant::now();
        let SyncStepOutcome::Progress(status)=engine.sync_step().await.unwrap() else { panic!("old chain must remain incomplete"); };
        assert_eq!(status.through.unwrap().number,199);assert_eq!(server.ranges(),vec![(0,99),(100,199)]);assert!(engine.snapshot().is_none());assert!(engine.discovery_checkpoint().unwrap().unwrap().positions.contains(&key()));
        let counts=server.counts();if let Some(expected)=&expected { assert_eq!(&counts,expected); } else { expected=Some(counts.clone()); }
        let directory=tempfile::tempdir().unwrap();let store=DiscoveryFileStore::new(directory.path().join("discovery.json"));store.save(&engine.discovery_checkpoint().unwrap().unwrap()).unwrap();drop(engine);
        let mut resumed=StateEngine::new(reader,spec(),options()).unwrap();assert!(resumed.restore_discovery(store.load(Some(&identity()),Some(&spec())).unwrap().unwrap()).unwrap());server.clear();
        let SyncStepOutcome::Progress(status)=resumed.sync_step().await.unwrap() else { panic!("resumed census must remain incomplete"); };assert_eq!(status.through.unwrap().number,399);assert_eq!(server.ranges(),vec![(200,299),(300,399)]);assert!(resumed.snapshot().is_none());assert!(resumed.discovery_checkpoint().unwrap().unwrap().positions.contains(&key()));
        assert!(started.elapsed()<Duration::from_secs(5));eprintln!("LONG_OPERATIONS {}",json!({"case":"cold_restart","head":head,"first_step_rpc":counts,"restart_step_rpc":server.counts(),"restart_log_ranges":server.ranges(),"wall_ms":started.elapsed().as_millis()}));
    }
}

#[tokio::test]
async fn cancelled_inflight_range_restarts_only_that_range_after_durable_restore() {
    let server=Server::new(100_000_000);server.stall_from.store(200,Ordering::Relaxed);let mut config=options();config.history_chunks_per_step=3;
    let mut engine=StateEngine::new(server.reader(),spec(),config.clone()).unwrap();let started=Instant::now();
    {
        let mut step=Box::pin(engine.sync_step());
        tokio::select! {
            result=&mut step=>panic!("sync unexpectedly finished before cancellation: {result:?}"),
            _=async { tokio::time::timeout(Duration::from_secs(3),async { while !server.ranges().contains(&(200,299)) { tokio::time::sleep(Duration::from_millis(1)).await; } }).await.unwrap(); }=>{},
        }
    }
    let progress=engine.discovery_checkpoint().unwrap().unwrap();assert_eq!(progress.through.unwrap().number,199);assert!(progress.positions.contains(&key()));assert!(engine.snapshot().is_none());
    let directory=tempfile::tempdir().unwrap();let store=DiscoveryFileStore::new(directory.path().join("discovery.json"));store.save(&progress).unwrap();drop(engine);server.stall_from.store(u64::MAX,Ordering::Relaxed);
    let mut resumed=StateEngine::new(server.reader(),spec(),config).unwrap();assert!(resumed.restore_discovery(store.load(Some(&identity()),Some(&spec())).unwrap().unwrap()).unwrap());
    let SyncStepOutcome::Progress(status)=resumed.sync_step().await.unwrap() else { panic!("restart must remain incomplete"); };assert_eq!(status.through.unwrap().number,499);
    assert_eq!(server.ranges(),vec![(0,99),(100,199),(200,299),(200,299),(300,399),(400,499)]);
    assert!(started.elapsed()<Duration::from_secs(5));assert_eq!(server.head.load(Ordering::Relaxed),100_000_000);
    eprintln!("LONG_OPERATIONS {}",json!({"case":"cancel_restart","head":100_000_000,"rpc":server.counts(),"log_ranges":server.ranges(),"wall_ms":started.elapsed().as_millis()}));
}

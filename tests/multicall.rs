//! Public Multicall3 and scoped-read compatibility/security regressions.
use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::Duration};
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{sol,SolCall,SolValue};
use barterlend_state::{Multicall3Config,PinnedRead,reader::Multicall3,rpc::BlockPinning};
use barterlend_state::{BlockRef, Commitment, Error, HttpReader, StateReader, rpc::RpcConfig};
use serde_json::{Value, json};

#[allow(dead_code)]
enum Reply {
    Result(Value),
    Error(i64, &'static str),
    Status(u16),
    Wire(Value),
    NoLength(Value),
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
}

// The server retains a socket clone for shutdown. Closing only the worker's
// handle after an idle timeout leaves a half-open connection in the HTTP pool.
struct ConnectionShutdown(TcpStream);
impl Drop for ConnectionShutdown {
    fn drop(&mut self) { let _=self.0.shutdown(std::net::Shutdown::Both); }
}

impl Server {
    fn new(handler: impl Fn(&Value) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let opened = sockets.clone();
        let handler = Arc::new(handler);
        let thread = thread::spawn(move || {
            let mut connections = Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("local listener: {error}"),
                };
                if stopping.load(Ordering::Relaxed) { break; }
                // Explicitly restore blocking mode on accepted sockets.
                stream.set_nonblocking(false).unwrap();
                opened.lock().unwrap().push(stream.try_clone().unwrap());
                let handler = handler.clone();
                let seen = seen.clone();
                let connection_stop = stopping.clone();
                connections.push(thread::spawn(move || {
                let _shutdown=ConnectionShutdown(stream.try_clone().unwrap());
                stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                while !connection_stop.load(Ordering::Relaxed) {
                let Some(request) = read_request(&stream) else { return; };
                seen.lock().unwrap().push(request.clone());
                let reply = handler(&request);
                let (status, envelope, include_length) = match reply {
                    Reply::Result(value) => (200, json!({"jsonrpc":"2.0","id":request["id"],"result":value}), true),
                    Reply::Error(code, message) => (200, json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":code,"message":message}}), true),
                    Reply::Status(status) => (status, json!({}), true),
                    Reply::Wire(value) => (200, value, true),
                    Reply::NoLength(value) => (200, json!({"jsonrpc":"2.0","id":request["id"],"result":value}), false),
                };
                let body = serde_json::to_vec(&envelope).unwrap();
                let length_header = if include_length { format!("Content-Length: {}\r\n", body.len()) } else { String::new() };
                if write!(stream, "HTTP/1.1 {status} test\r\nContent-Type: application/json\r\n{length_header}Connection: keep-alive\r\n\r\n").is_err() { return; }
                // A body-cap test may close the socket after headers.
                if stream.write_all(&body).is_err() { return; }
                }
                }));
            }
            for connection in connections { connection.join().unwrap(); }
        });
        Self { url: format!("http://{address}"), requests, stop, thread: Some(thread), sockets }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for socket in self.sockets.lock().unwrap().iter() { let _ = socket.shutdown(std::net::Shutdown::Both); }
        let _ = TcpStream::connect(self.url.strip_prefix("http://").unwrap());
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() && !thread::panicking() { panic!("local RPC server failed"); }
        }
    }
}

fn read_request(stream: &TcpStream) -> Option<Value> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 { return None; }
    if !line.starts_with("POST ") { return None; }
    let mut length = None;
    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 { return None; }
        if line == "\r\n" { break; }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = Some(value.trim().parse::<usize>().ok()?);
        }
    }
    let length = length?;
    if length > 1024 * 1024 { return None; }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    serde_json::from_slice(&body).ok()
}


fn config() -> RpcConfig { RpcConfig { max_retries: 0, min_request_interval: Duration::ZERO,
    request_timeout: Duration::from_secs(5), ..Default::default() } }
fn block(number: u64) -> BlockRef {
    BlockRef { number, hash: keccak256(number.to_be_bytes()),
        parent_hash: if number == 0 { B256::ZERO } else { keccak256((number - 1).to_be_bytes()) },
        timestamp: number / 3, commitment: Commitment::Latest }
}
fn header(value: BlockRef) -> Value {
    json!({"number":format!("0x{:x}",value.number),"hash":value.hash,
        "parentHash":value.parent_hash,"timestamp":format!("0x{:x}",value.timestamp)})
}
#[allow(dead_code)]
fn requested(request: &Value) -> u64 {
    assert_eq!(request["method"], "eth_getBlockByNumber");
    u64::from_str_radix(request["params"][0].as_str().unwrap().strip_prefix("0x").unwrap(), 16).unwrap()
}

sol! { interface View { function value(uint256 n) external view returns(uint256); } }
fn code()->Bytes { ("0x".to_owned()+include_str!("fixtures/multicall3-runtime.hex").trim()).parse().unwrap() }
fn reads(n:usize)->Vec<PinnedRead> {(0..n).map(|i|PinnedRead::call(Address::repeat_byte(1),&View::valueCall{n:U256::from(i)})).collect()}
fn response(request:&Value,mode:usize,calls:&AtomicUsize)->Reply {
    match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber"=>{
            let mut at=block(7);if mode==6 && calls.load(Ordering::SeqCst)>0 {at.hash=B256::repeat_byte(99);}
            Reply::Result(header(at))
        },
        "eth_getCode"=>Reply::Result(json!(if mode==1 {Bytes::from_static(&[0])}else{code()})),
        "eth_call"=>{
            calls.fetch_add(1,Ordering::SeqCst);
            let data:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            if data.starts_with(&View::valueCall::SELECTOR) {
                return Reply::Result(json!(Bytes::from(View::valueCall::abi_decode_validate(&data).unwrap().n.abi_encode())));
            }
            assert_eq!(request["params"][0]["to"],json!(Multicall3Config::canonical().address));
            assert_eq!(request["params"][0]["gas"],json!("0x989680"));
            if mode==5 || mode==6 {return Reply::Error(3,"fixture revert");}
            let call=Multicall3::aggregate3Call::abi_decode_validate(&data).unwrap();
            let mut values:Vec<_>=call.calls.iter().map(|c| {
                assert!(c.allowFailure);
                Multicall3::Result{success:mode!=2,returnData:View::valueCall::abi_decode_validate(&c.callData).unwrap().n.abi_encode().into()}
            }).collect();
            if mode==3 {values.pop();}
            let mut bytes=Multicall3::aggregate3Call::abi_encode_returns(&values);
            if mode==4 {bytes.push(0);}
            Reply::Result(json!(Bytes::from(bytes)))
        },
        _=>panic!("unexpected RPC"),
    }
}
fn server(mode:usize)->Server {
    let calls=AtomicUsize::new(0);Server::new(move|r|response(r,mode,&calls))
}
#[tokio::test]
async fn sixty_four_views_are_one_call_with_code_pin_for_every_pinning_mode() {
    assert_eq!(keccak256(code()),Multicall3Config::canonical().code_hash);
    for pinning in [BlockPinning::NumberBracket,BlockPinning::Eip1898,BlockPinning::RawHash] {
        let server=server(0);let mut config=config();config.pinning=pinning;
        let reader=HttpReader::new(&server.url,config).unwrap().with_multicall3(Multicall3Config::canonical()).unwrap();
        let values=reader.read_views(&reads(64),&block(7)).await.unwrap();
        assert_eq!(values.len(),64);
        for (i,value) in values.iter().enumerate() {assert_eq!(View::valueCall::abi_decode_returns_validate(value).unwrap(),U256::from(i));}
        let requests=server.requests.lock().unwrap();assert_eq!(requests.len(),4);
        assert_eq!(requests.iter().filter(|r|r["method"]=="eth_call").count(),1);
        let pin=&requests.iter().find(|r|r["method"]=="eth_call").unwrap()["params"][1];
        match pinning {BlockPinning::NumberBracket=>assert_eq!(pin,&json!("0x7")),BlockPinning::Eip1898=>assert_eq!(pin,&json!({"blockHash":block(7).hash,"requireCanonical":true})),BlockPinning::RawHash=>assert_eq!(pin,&json!(block(7).hash))}
    }
}
#[tokio::test]
async fn legacy_bundle_keeps_direct_sender_semantics_and_optional_chunks_keep_order() {
    let server=server(0);let mut config=Multicall3Config::canonical();config.max_calls=2;
    let reader=HttpReader::new(&server.url,self::config()).unwrap().with_multicall3(config).unwrap();
    reader.read_bundle(&reads(3),&block(7)).await.unwrap();
    assert!(server.requests.lock().unwrap().iter().all(|r|r["method"]!="eth_getCode"));
    let result=reader.read_views(&reads(5),&block(7)).await.unwrap();
    assert_eq!(View::valueCall::abi_decode_returns_validate(&result[4]).unwrap(),U256::from(4));
    let requests=server.requests.lock().unwrap();assert_eq!(requests.iter().filter(|r|r["method"]=="eth_call").count(),6);
}
#[tokio::test]
async fn malformed_failed_or_substituted_aggregates_return_no_partial_data() {
    for mode in 1..=6 {
        let server=server(mode);let reader=HttpReader::new(&server.url,config()).unwrap().with_multicall3(Multicall3Config::canonical()).unwrap();
        let error=reader.read_views(&reads(3),&block(7)).await.unwrap_err();
        if mode==1 {assert!(matches!(error,Error::IdentityMismatch(_)));}
        if mode==6 {assert!(matches!(error,Error::NonCanonical(7)),"closing reorg dominates aggregate revert");}
        assert_eq!(server.requests.lock().unwrap().iter().filter(|r|r["method"]=="eth_getBlockByNumber").count(),2);
    }
}
#[tokio::test]
async fn scoped_views_share_headers_and_code_without_retaining_ttl_state() {
    let server=server(0);let reader=HttpReader::new(&server.url,config()).unwrap().with_multicall3(Multicall3Config::canonical()).unwrap();
    reader.verified_read(async {reader.read_views(&reads(3),&block(7)).await?;reader.read_views(&reads(3),&block(7)).await}).await.unwrap().unwrap();
    {let requests=server.requests.lock().unwrap();assert_eq!(requests.len(),5);assert_eq!(requests.iter().filter(|r|r["method"]=="eth_getCode").count(),1);}
    reader.read_views(&reads(3),&block(7)).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(),9);
}
#[tokio::test]
async fn recursive_and_oversized_batches_fail_without_aggregate_calls() {
    let server=server(0);let mut config=Multicall3Config::canonical();config.max_calldata_bytes=256;
    let reader=HttpReader::new(&server.url,self::config()).unwrap().with_multicall3(config).unwrap();
    assert!(reader.read_views(&reads(64),&block(7)).await.is_err());
    let recursive=[PinnedRead::call(Multicall3Config::canonical().address,&View::valueCall{n:U256::ZERO})];
    assert!(reader.read_views(&recursive,&block(7)).await.is_err());
    assert!(server.requests.lock().unwrap().iter().all(|r|r["method"]=="eth_getBlockByNumber"));
}

#[tokio::test]
#[ignore = "requires an explicitly supplied disposable loopback Anvil"]
async fn canonical_runtime_executes_real_evm_batches_and_rejects_subcall_reverts() {
    let endpoint=std::env::var("MULTICALL_ANVIL_RPC").expect("disposable Anvil endpoint required");
    let url:url::Url=endpoint.parse().unwrap();
    assert_eq!(url.scheme(),"http");assert_eq!(url.host_str(),Some("127.0.0.1"));
    let http=reqwest::Client::builder().no_proxy().build().unwrap();
    async fn rpc(http:&reqwest::Client,url:&str,method:&str,params:Value)->Value {
        let value:Value=http.post(url).json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send().await.unwrap().json().await.unwrap();assert!(value.get("error").is_none(),"{value}");value["result"].clone()
    }
    assert_eq!(rpc(&http,&endpoint,"eth_chainId",json!([])).await,json!("0x7a69"));
    rpc(&http,&endpoint,"anvil_setCode",json!([Multicall3Config::canonical().address,code()])).await;
    // Returns calldata word at offset 4: value(uint256) without compiler/toolchain dependencies.
    rpc(&http,&endpoint,"anvil_setCode",json!([Address::repeat_byte(1),"0x60043560005260206000f3"])).await;
    rpc(&http,&endpoint,"anvil_setCode",json!([Address::repeat_byte(2),"0x60006000fd"])).await;
    rpc(&http,&endpoint,"evm_mine",json!([])).await;
    let reader=HttpReader::new(&endpoint,config()).unwrap().with_multicall3(Multicall3Config::canonical()).unwrap();
    let at=reader.head(Commitment::Latest).await.unwrap();
    let direct=reader.read_bundle(&reads(64),&at).await.unwrap();
    let aggregated=reader.read_views(&reads(64),&at).await.unwrap();assert_eq!(direct,aggregated);
    let mut failed=reads(3);failed[1]=PinnedRead::call(Address::repeat_byte(2),&View::valueCall{n:U256::ZERO});
    assert!(reader.read_views(&failed,&at).await.is_err());
    rpc(&http,&endpoint,"anvil_mine",json!(["0x12c"])).await;
    let at=reader.head(Commitment::Latest).await.unwrap();
    let mut witnesses=Vec::new();
    for n in [at.number-299,at.number-257,at.number-256,at.number-200,at.number-1,at.number] {
        witnesses.push((n,reader.block(n,Commitment::Latest).await.unwrap().unwrap().hash));
    }
    let reader=optimized(reader);
    reader.verify_log_blocks(&witnesses,Commitment::Latest).await.unwrap();
    witnesses[2].1=B256::repeat_byte(91);
    assert!(matches!(reader.verify_log_blocks(&witnesses,Commitment::Latest).await,Err(Error::NonCanonical(_))));
    println!("EVM_MULTICALL_PASS runtime_bytes={} code_hash={} anchor={} reads=64 exact_match=true revert_rejected=true",code().len(),keccak256(code()),at.number);
}

fn optimized(reader:HttpReader)->HttpReader {
    reader.with_multicall3(Multicall3Config::canonical()).unwrap().with_read_optimizations(
        barterlend_state::ReadOptimizationConfig {coalesce_pending_checks:true,batch_log_hashes:true,
            position_batch_size:128,..Default::default()}).unwrap()
}
#[tokio::test]
async fn recent_hash_proofs_are_bounded_and_reject_zero_wrong_and_noncanonical_anchors() {
    for bad in 0..4 {
        let calls=AtomicUsize::new(0);
        let server=Server::new(move|r|match r["method"].as_str().unwrap() {
            "eth_getBlockByNumber"=>{let mut b=block(requested(r));
                if bad==3 && calls.load(Ordering::SeqCst)>0 {b.hash=B256::repeat_byte(77);}
                Reply::Result(header(b))},
            "eth_getCode"=>Reply::Result(json!(code())),
            "eth_call"=>{calls.fetch_add(1,Ordering::SeqCst);
                let at=u64::from_str_radix(r["params"][1].as_str().unwrap().trim_start_matches("0x"),16).unwrap();
                let data:Bytes=serde_json::from_value(r["params"][0]["data"].clone()).unwrap();
                let c=Multicall3::aggregate3Call::abi_decode_validate(&data).unwrap();
                let values:Vec<_>=c.calls.iter().map(|c|{
                    let n=u64::try_from(Multicall3::getBlockHashCall::abi_decode_validate(&c.callData).unwrap().blockNumber).unwrap();
                    assert!(n<at && at-n<=256);assert_eq!(c.target,Multicall3Config::canonical().address);
                    let hash=if bad==1 {B256::ZERO}else if bad==2 {B256::repeat_byte(88)}else{block(n).hash};
                    Multicall3::Result{success:true,returnData:hash.abi_encode().into()}
                }).collect();Reply::Result(json!(Bytes::from(Multicall3::aggregate3Call::abi_encode_returns(&values))))},
            _=>panic!("unexpected RPC"),
        });
        let reader=optimized(HttpReader::new(&server.url,config()).unwrap());
        let witnesses:Vec<_>=[1,2,3,44,45,100,200,299,300].iter().map(|&n|(n,block(n).hash)).collect();
        let result=reader.verify_log_blocks(&witnesses,Commitment::Latest).await;
        if bad==0 {result.unwrap();}else {assert!(matches!(result,Err(Error::NonCanonical(_))));}
    }
}
#[tokio::test]
async fn pending_checks_share_work_but_later_checks_are_fresh_and_errors_keep_their_class() {
    for fail in [false,true] {
        let server=Server::new(move|r|match r["method"].as_str().unwrap(){
            "eth_chainId"=>if fail {Reply::Error(-32001,"permanent")}else{Reply::Result(json!("0x1"))},
            "eth_getBlockByNumber"=>Reply::Result(header(block(requested(r)))),_=>panic!("unexpected RPC")});
        let mut c=config();c.min_request_interval=Duration::from_millis(60);
        let reader=optimized(HttpReader::new(&server.url,c).unwrap());
        reader.block(7,Commitment::Latest).await.unwrap(); // occupy the next rate slot
        let (a,b,c,d)=tokio::join!(reader.identity(),reader.identity(),reader.identity(),reader.identity());
        for r in [a,b,c,d] {if fail {assert!(matches!(r,Err(Error::Rpc{code:-32001,..})));}else{r.unwrap();}}
        let before=server.requests.lock().unwrap().len();
        assert_eq!(before,if fail {2}else{3});
        let _=reader.identity().await;
        assert_eq!(server.requests.lock().unwrap().len(),before+if fail {1}else{2});
    }
}
#[tokio::test]
async fn dispatched_headers_cannot_satisfy_new_checks_and_cancellation_releases_pending_work() {
    let entered=Arc::new(AtomicBool::new(false));let flag=entered.clone();
    let server=Server::new(move|r|{flag.store(true,Ordering::SeqCst);thread::sleep(Duration::from_millis(80));Reply::Result(header(block(requested(r))))});
    let reader=optimized(HttpReader::new(&server.url,config()).unwrap());
    let first=reader.clone();let task=tokio::spawn(async move{first.block(7,Commitment::Latest).await});
    while !entered.load(Ordering::SeqCst) {tokio::time::sleep(Duration::from_millis(1)).await;}
    reader.block(7,Commitment::Latest).await.unwrap();task.await.unwrap().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(),2);
    let mut c=config();c.min_request_interval=Duration::from_millis(150);
    let reader=optimized(HttpReader::new(&server.url,c).unwrap());
    reader.block(7,Commitment::Latest).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(5),reader.block(8,Commitment::Latest)).await.is_err());
    assert_eq!(reader.block(9,Commitment::Latest).await.unwrap().unwrap().number,9);
}
#[tokio::test]
async fn enlarged_position_batches_keep_exact_values_and_caps() {
    use barterlend_state::{PositionKey,abi::CreditPool};
    let server=Server::new(|r|match r["method"].as_str().unwrap(){
        "eth_getBlockByNumber"=>Reply::Result(header(block(requested(r)))),
        "eth_call"=>{assert_eq!(r["params"][0]["gas"],json!("0x989680"));
            let data:Bytes=serde_json::from_value(r["params"][0]["data"].clone()).unwrap();
            let calls=CreditPool::multicallCall::abi_decode_validate(&data).unwrap();assert!(calls.data.len()<=256);
            let values:Vec<Bytes>=calls.data.iter().map(|d|{
                if d.starts_with(&CreditPool::debtOfCall::SELECTOR) {
                    CreditPool::debtOfCall::abi_decode_validate(d).unwrap().market.abi_encode().into()
                }else {assert!(d.starts_with(&CreditPool::collateralOfCall::SELECTOR));123u128.abi_encode().into()}
            }).collect();Reply::Result(json!(Bytes::from(CreditPool::multicallCall::abi_encode_returns(&values))))},
        _=>panic!("unexpected RPC"),
    });
    let reader=optimized(HttpReader::new(&server.url,config()).unwrap());let pool=Address::repeat_byte(9);
    let keys:Vec<_>=(1..=200).map(|market|PositionKey{pool,market,borrower:Address::repeat_byte(1)}).collect();
    let values=reader.read_positions(pool,&keys,&block(7)).await.unwrap();assert_eq!(values.len(),200);
    for key in &keys {assert_eq!(values[key].debt,u128::from(key.market));assert_eq!(values[key].collateral,123);}
    assert_eq!(server.requests.lock().unwrap().iter().filter(|r|r["method"]=="eth_call").count(),2);
    let mut bounds=barterlend_state::ReadOptimizationConfig::default();bounds.position_batch_size=128;bounds.position_batch_calldata_limit=256;
    let limited=reader.with_read_optimizations(bounds).unwrap();
    assert!(matches!(limited.read_positions(pool,&keys,&block(7)).await,Err(Error::ResourceLimit(_))));
}

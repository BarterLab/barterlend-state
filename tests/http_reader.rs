//! HTTP and JSON-RPC transport checks against a local server.

use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, process::{Child, Command, Stdio}, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};

use alloy_primitives::{Address, B256, Bytes, U256};
use barterlend_state::{Error, HttpReader, StateReader, abi::{CreditPool, IPriceOracle}, rpc::{BlockPinning, RpcClient, RpcConfig, RpcErrorKind, classify_rpc_error}, types::*};
use alloy_sol_types::{SolCall, SolEvent};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../examples/watch_positions.rs"]
mod watch_example;

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
                // accept can inherit O_NONBLOCK; a timeout does not clear it.
                stream.set_nonblocking(false).unwrap();
                let handler = handler.clone();
                let seen = seen.clone();
                connections.push(thread::spawn(move || {
                stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
                if write!(stream, "HTTP/1.1 {status} test\r\nContent-Type: application/json\r\n{length_header}Connection: close\r\n\r\n").is_err() { return; }
                // The body-limit test deliberately closes the connection after headers.
                let _ = stream.write_all(&body);
                }));
            }
            for connection in connections { connection.join().unwrap(); }
        });
        Self { url: format!("http://{address}"), requests, stop, thread: Some(thread) }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
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

fn rpc_config(pinning: BlockPinning) -> RpcConfig {
    RpcConfig { pinning, max_retries: 0, min_request_interval: Duration::ZERO, request_timeout: Duration::from_secs(2), ..RpcConfig::default() }
}

fn block() -> BlockRef {
    BlockRef { number: 7, hash: B256::repeat_byte(7), parent_hash: B256::repeat_byte(6), timestamp: 70, commitment: Commitment::Finalized }
}

fn header(block: BlockRef) -> Value {
    json!({"number":format!("0x{:x}",block.number),"hash":block.hash,"parentHash":block.parent_hash,"timestamp":format!("0x{:x}",block.timestamp)})
}

fn word(number: u128) -> Value {
    json!(Bytes::from(U256::from(number).to_be_bytes::<32>().to_vec()))
}

#[tokio::test]
async fn typed_reads_reject_trailing_abi_words_in_every_pinning_mode() {
    for mode in [BlockPinning::NumberBracket,BlockPinning::Eip1898,BlockPinning::RawHash] {
        let server=Server::new(|request| match request["method"].as_str().unwrap() {
            "eth_getBlockByNumber"=>Reply::Result(header(block())),
            "eth_call"=>{
                let mut bytes=CreditPool::versionCall::abi_encode_returns(&U256::from(5));
                bytes.extend_from_slice(&[0;32]);Reply::Result(json!(Bytes::from(bytes)))
            },
            method=>panic!("unexpected method {method}"),
        });
        let reader=HttpReader::new(&server.url,rpc_config(mode)).unwrap();
        assert!(matches!(reader.contract_call(pool().address,&CreditPool::versionCall{},&block()).await,Err(Error::Decode(_))));
        assert_eq!(server.requests.lock().unwrap().len(),3,"malformed data still receives the closing canonical check");
    }
}

fn pool_multicall_reply(request: &Value, value: impl Fn(&Bytes) -> Bytes) -> Reply {
    let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
    let call = CreditPool::multicallCall::abi_decode_validate(&data).unwrap();
    assert!(!call.data.is_empty() && call.data.len() <= 64);
    let values: Vec<Bytes> = call.data.iter().map(value).collect();
    Reply::Result(json!(Bytes::from(CreditPool::multicallCall::abi_encode_returns(&values))))
}

#[tokio::test]
async fn current_user_pages_share_one_native_multicall_and_preserve_page_order() {
    let server = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => pool_multicall_reply(request, |data| {
            let page=CreditPool::getMarketUsersCall::abi_decode_validate(data).unwrap();
            assert_eq!(page.start,U256::from(5)); assert_eq!(page.limit,U256::from(2));
            CreditPool::getMarketUsersCall::abi_encode_returns(&vec![Address::repeat_byte(page.market as u8)]).into()
        }),_=>unreachable!(),
    });
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let pages=[barterlend_state::MarketUserPage{market:2,start:5,limit:2},barterlend_state::MarketUserPage{market:1,start:5,limit:2}];
    assert_eq!(reader.market_user_pages(pool().address,&pages,&block()).await.unwrap(),vec![vec![Address::repeat_byte(2)],vec![Address::repeat_byte(1)]]);
    assert_eq!(server.requests.lock().unwrap().len(),3);
    assert!(reader.market_user_pages(pool().address,&[],&block()).await.is_err());
    assert!(reader.market_user_pages(pool().address,&[pages[0];65],&block()).await.is_err());
    assert!(reader.market_user_pages(pool().address,&[barterlend_state::MarketUserPage{limit:0,..pages[0]}],&block()).await.is_err());
    assert_eq!(server.requests.lock().unwrap().len(),3,"invalid inputs must not issue RPC");
}

#[tokio::test]
async fn current_user_pages_reject_oversized_or_noncanonical_inner_arrays() {
    for trailing in [false,true] {
        let server=Server::new(move|request|match request["method"].as_str().unwrap(){
            "eth_getBlockByNumber"=>Reply::Result(header(block())),
            "eth_call"=>pool_multicall_reply(request,|_|{
                let mut encoded=CreditPool::getMarketUsersCall::abi_encode_returns(&vec![Address::repeat_byte(1);if trailing{1}else{3}]);
                if trailing{encoded.extend_from_slice(&[0;32]);} encoded.into()
            }),_=>unreachable!(),
        });
        let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
        assert!(matches!(reader.market_user_pages(pool().address,&[barterlend_state::MarketUserPage{market:1,start:0,limit:2}],&block()).await,Err(Error::Decode(_))));
        assert_eq!(server.requests.lock().unwrap().len(),3);
    }
}

fn pool() -> PoolDescriptor {
    PoolDescriptor {
        address: Address::repeat_byte(1), version: 5,
        asset: TokenDescriptor { address: Address::repeat_byte(2), decimals: 6 },
        collateral: TokenDescriptor { address: Address::repeat_byte(3), decimals: 18 },
        oracle: Address::repeat_byte(4), code_hash: B256::repeat_byte(5),
    }
}

#[test]
fn endpoint_identity_compares_complete_url_without_network_or_url_getter() {
    let endpoint = "https://example:placeholder@example.invalid/rpc?token=placeholder";
    let reader = HttpReader::new(endpoint, RpcConfig::default()).unwrap();
    let same = reqwest::Url::parse(endpoint).unwrap();
    assert!(reader.endpoint_matches(&same));
    assert!(reader.rpc().endpoint_matches(&same));
    for other in [
        "http://example:placeholder@example.invalid/rpc?token=placeholder",
        "https://example:placeholder@other.invalid/rpc?token=placeholder",
        "https://example:placeholder@example.invalid/other?token=placeholder",
        "https://example:placeholder@example.invalid/rpc?token=different",
        "https://example:different@example.invalid/rpc?token=placeholder",
    ] {
        assert!(!reader.endpoint_matches(&reqwest::Url::parse(other).unwrap()));
    }
}

#[test]
fn extreme_durations_are_rejected_before_client_construction() {
    for field in 0..3 {
        let mut config = RpcConfig::default();
        match field {
            0 => config.min_request_interval = Duration::MAX,
            1 => config.request_timeout = Duration::MAX,
            2 => config.retry_backoff = Duration::MAX,
            _ => unreachable!(),
        }
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
        // Creates no runtime, listener or HTTP request.
        assert!(matches!(RpcClient::new("http://127.0.0.1:9", config), Err(Error::InvalidConfig(_))));
    }
    let config = RpcConfig {
        min_request_interval: Duration::from_secs(60),
        request_timeout: Duration::from_secs(300),
        retry_backoff: Duration::from_secs(30),
        ..RpcConfig::default()
    };
    config.validate().unwrap();
}

#[tokio::test]
async fn all_pinning_modes_send_explicit_block_and_canonical_brackets() {
    for mode in [BlockPinning::NumberBracket, BlockPinning::Eip1898, BlockPinning::RawHash] {
        let server = Server::new(|request| match request["method"].as_str().unwrap() {
            "eth_getBlockByNumber" => Reply::Result(header(block())),
            "eth_call" => Reply::Result(word(5)),
            method => panic!("unexpected method {method}"),
        });
        let reader = HttpReader::new(&server.url, rpc_config(mode)).unwrap();
        assert_eq!(reader.contract_call(pool().address, &CreditPool::versionCall {}, &block()).await.unwrap(), U256::from(5));
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["params"][0], "0x7");
        assert_eq!(requests[2]["params"][0], "0x7");
        let expected = match mode {
            BlockPinning::NumberBracket => json!("0x7"),
            BlockPinning::RawHash => json!(block().hash),
            BlockPinning::Eip1898 => json!({"blockHash":block().hash,"requireCanonical":true}),
        };
        assert_eq!(requests[1]["params"][1], expected);
        assert!(requests.iter().all(|request| !request["params"].to_string().contains("latest")));
    }
}

#[tokio::test]
async fn canonical_race_discards_successful_getter_result() {
    let headers = Arc::new(AtomicUsize::new(0));
    let counter = headers.clone();
    let server = Server::new(move |request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => {
            let mut block = block();
            if counter.fetch_add(1, Ordering::Relaxed) > 0 { block.hash = B256::repeat_byte(9); }
            Reply::Result(header(block))
        }
        "eth_call" => Reply::Result(word(5)),
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.contract_call(pool().address, &CreditPool::versionCall {}, &block()).await, Err(Error::NonCanonical(7))));
}

#[tokio::test]
async fn pruned_state_is_missing_block_but_contract_revert_stays_rpc() {
    for (code, message, pruned) in [
        (-32000, "historical state unavailable", true),
        (-32000, "missing trie node", true),
        (-32000, "execution reverted: missing trie node", false),
        (3, "historical state unavailable", false),
        (3, "oracle offline", false),
    ] {
        let server = Server::new(move |request| match request["method"].as_str().unwrap() {
            "eth_getBlockByNumber" => Reply::Result(header(block())),
            "eth_call" => Reply::Error(code, message),
            _ => unreachable!(),
        });
        let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
        let error = reader.contract_call(pool().address, &CreditPool::versionCall {}, &block()).await.unwrap_err();
        if pruned { assert!(matches!(error, Error::MissingBlock(7))); }
        else { assert!(matches!(error, Error::Rpc { .. })); }
        assert_eq!(server.requests.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn primitive_reads_are_separate_from_oracle_revert() {
    let server = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => {
            if request["params"][0]["to"] == json!(pool().oracle) { return Reply::Error(3, "oracle offline"); }
            pool_multicall_reply(request, |data| {
                let amount = if data.starts_with(&CreditPool::debtOfCall::SELECTOR) { 500u128 }
                    else if data.starts_with(&CreditPool::collateralOfCall::SELECTOR) { 1_000u128 }
                    else { panic!("primitive reader used a risk/unknown getter"); };
                Bytes::from(U256::from(amount).to_be_bytes::<32>().to_vec())
            })
        }
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    let key = PositionKey { pool: pool().address, market: 100, borrower: Address::repeat_byte(8) };
    let amounts = reader.read_positions(pool().address, &[key], &block()).await.unwrap();
    assert_eq!(amounts[&key], PositionAmounts { debt: 500, collateral: 1_000 });
    assert!(matches!(reader.read_oracle(&pool(), &block()).await.unwrap(), OracleStatus::Unavailable(OracleFailure::Rpc(_))));
}

#[tokio::test]
async fn oracle_wrong_direction_is_unavailable_without_get_price() {
    let server = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => {
            let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            assert!(!data.starts_with(&IPriceOracle::getPriceCall::SELECTOR));
            Reply::Result(json!(Bytes::from(Address::repeat_byte(9).into_word().to_vec())))
        }
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.read_oracle(&pool(), &block()).await.unwrap(), OracleStatus::Unavailable(OracleFailure::PairMismatch { .. })));
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn oracle_decode_failure_is_status_but_noncanonical_block_is_error() {
    let malformed = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => {
            let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            let address = if data.starts_with(&IPriceOracle::sourceCall::SELECTOR) { pool().collateral.address }
                else if data.starts_with(&IPriceOracle::targetCall::SELECTOR) { pool().asset.address }
                else { return Reply::Result(json!("0x")); };
            Reply::Result(json!(Bytes::from(address.into_word().to_vec())))
        }
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&malformed.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.read_oracle(&pool(), &block()).await.unwrap(), OracleStatus::Unavailable(OracleFailure::Decode(_))));
    let headers = Arc::new(AtomicUsize::new(0));
    let counter = headers.clone();
    let race = Server::new(move |request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => {
            let mut at = block();
            if counter.fetch_add(1, Ordering::Relaxed) > 0 { at.hash = B256::repeat_byte(99); }
            Reply::Result(header(at))
        }
        "eth_call" => Reply::Result(json!(Bytes::from(pool().collateral.address.into_word().to_vec()))),
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&race.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.read_oracle(&pool(), &block()).await, Err(Error::NonCanonical(7))));
}

#[tokio::test]
async fn unsupported_hash_modes_do_not_fallback_to_latest() {
    let server = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => Reply::Error(-32602, "blockHash objects unsupported"),
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::Eip1898)).unwrap();
    assert!(matches!(reader.contract_call(pool().address, &CreditPool::versionCall {}, &block()).await, Err(Error::Rpc { code: -32602, .. })));
    assert_eq!(server.requests.lock().unwrap().iter().filter(|request| request["method"] == "eth_call").count(), 1);
}

#[tokio::test]
async fn probe_reports_supported_raw_hash_and_rejects_ignored_hash() {
    for ignore_hash in [false, true] {
        let server = Server::new(move |request| match request["method"].as_str().unwrap() {
            "eth_getBlockByNumber" => Reply::Result(header(block())),
            "eth_getBlockByHash" => Reply::Result(Value::Null),
            "eth_call" => {
                let pin = &request["params"][1];
                if pin.is_object() { return Reply::Error(-32602, "EIP1898 unsupported"); }
                if pin == &json!("0x7") || pin == &json!(block().hash) || ignore_hash { Reply::Result(word(5)) }
                else { Reply::Error(-32000, "unknown block") }
            }
            _ => unreachable!(),
        });
        let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
        let capabilities = reader.probe_capabilities(pool().address, &block()).await.unwrap();
        assert!(!capabilities.eip1898);
        assert_eq!(capabilities.raw_hash, !ignore_hash);
        assert!(capabilities.number_bracket && capabilities.finalized && capabilities.safe);
    }
}

#[tokio::test]
async fn capability_probe_does_not_treat_rate_limit_as_missing_hash() {
    let server = Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_getBlockByHash" => Reply::Result(Value::Null),
        "eth_call" => {
            let pin = &request["params"][1];
            if pin.is_object() { Reply::Error(-32602, "EIP1898 unsupported") }
            else if pin == &json!("0x7") || pin == &json!(block().hash) { Reply::Result(word(5)) }
            else { Reply::Status(429) }
        }
        _ => unreachable!(),
    });
    let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.probe_capabilities(pool().address, &block()).await, Err(Error::Rpc { code: 429, .. })));
}

#[tokio::test]
async fn resource_limits_reject_headers_and_streamed_oversize_body() {
    for length_header in [true, false] {
        let server = Server::new(move |_| {
            let value = json!("x".repeat(2_000));
            if length_header { Reply::Result(value) } else { Reply::NoLength(value) }
        });
        let mut config = rpc_config(BlockPinning::NumberBracket);
        config.max_response_bytes = 128;
        let rpc = RpcClient::new(&server.url, config).unwrap();
        let error = rpc.request("eth_chainId", json!([])).await.unwrap_err();
        assert_eq!(classify_rpc_error(&error), RpcErrorKind::PayloadTooLarge);
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn retries_are_bounded_and_do_not_retry_range_errors() {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    let server = Server::new(move |_| {
        if counter.fetch_add(1, Ordering::Relaxed) < 2 { Reply::Status(429) } else { Reply::Result(json!("0x8f")) }
    });
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.max_retries = 2;
    config.retry_backoff = Duration::ZERO;
    let rpc = RpcClient::new(&server.url, config.clone()).unwrap();
    assert_eq!(rpc.request("eth_chainId", json!([])).await.unwrap(), "0x8f");
    assert_eq!(count.load(Ordering::Relaxed), 3);
    let range = Server::new(|_| Reply::Error(-32005, "block range too wide"));
    let rpc = RpcClient::new(&range.url, config).unwrap();
    let error = rpc.request("eth_getLogs", json!([{}])).await.unwrap_err();
    assert_eq!(classify_rpc_error(&error), RpcErrorKind::RangeTooWide);
    assert_eq!(range.requests.lock().unwrap().len(), 1);
    let unavailable = Server::new(|_| Reply::Status(503));
    let mut retry_config = rpc_config(BlockPinning::NumberBracket);
    retry_config.max_retries = 2;
    retry_config.retry_backoff = Duration::ZERO;
    let rpc = RpcClient::new(&unavailable.url, retry_config).unwrap();
    assert!(matches!(rpc.request("eth_chainId", json!([])).await, Err(Error::Rpc { code: 503, .. })));
    assert_eq!(unavailable.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn runtime_http_timeout_is_bounded_fail_closed_and_releases_capacity() {
    let delayed = Arc::new(AtomicBool::new(true));
    let delay = delayed.clone();
    let server = Server::new(move |request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => {
            if delay.load(Ordering::Relaxed) { thread::sleep(Duration::from_millis(350)); }
            Reply::Result(word(5))
        }
        _ => unreachable!(),
    });
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.request_timeout = Duration::from_millis(80);
    config.max_retries = 1;
    config.retry_backoff = Duration::from_millis(5);
    config.max_concurrent_requests = 1;
    let reader = HttpReader::new(&server.url, config).unwrap();
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2),
        reader.contract_call(pool().address, &CreditPool::versionCall {}, &block())).await.unwrap();
    assert!(matches!(result, Err(Error::Transport(_))), "late successful body must not become a getter value: {result:?}");
    assert!(started.elapsed() >= Duration::from_millis(140), "both bounded timeout attempts should run");
    assert_eq!(server.requests.lock().unwrap().iter().filter(|request| request["method"] == "eth_call").count(), 2);
    delayed.store(false, Ordering::Relaxed);
    // A timed-out HTTP request releases the sole client permit, even while its server handler is still delayed.
    assert_eq!(tokio::time::timeout(Duration::from_millis(200),
        reader.contract_call(pool().address, &CreditPool::versionCall {}, &block())).await.unwrap().unwrap(), U256::from(5));
}

#[tokio::test]
async fn cloned_clients_share_actual_request_rate_spacing() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let times = received.clone();
    let server = Server::new(move |_| {
        times.lock().unwrap().push(Instant::now());
        Reply::Result(json!("0x8f"))
    });
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.min_request_interval = Duration::from_millis(150);
    let rpc = RpcClient::new(&server.url, config).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let clone = rpc.clone();
        tasks.push(tokio::spawn(async move { clone.request("eth_chainId", json!([])).await }));
    }
    for task in tasks { assert_eq!(task.await.unwrap().unwrap(), "0x8f"); }
    let mut received = received.lock().unwrap().clone();
    received.sort();
    assert_eq!(received.len(), 4);
    // 50ms scheduling/network margin, while a clone-local limiter would send all four immediately.
    for pair in received.windows(2) {
        assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(100), "request starts were not spaced: {received:?}");
    }
    assert!(received[3].duration_since(received[0]) >= Duration::from_millis(400));
}

#[tokio::test]
async fn cloned_clients_bound_actual_inflight_requests() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let (busy, max_seen, released) = (active.clone(), maximum.clone(), release.clone());
    let server = Server::new(move |_| {
        let now = busy.fetch_add(1, Ordering::SeqCst) + 1;
        max_seen.fetch_max(now, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !released.load(Ordering::SeqCst) && Instant::now() < deadline { thread::sleep(Duration::from_millis(1)); }
        busy.fetch_sub(1, Ordering::SeqCst);
        Reply::Result(json!("0x8f"))
    });
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.max_concurrent_requests = 2;
    let rpc = RpcClient::new(&server.url, config).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let clone = rpc.clone();
        tasks.push(tokio::spawn(async move { clone.request("eth_chainId", json!([])).await }));
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while active.load(Ordering::SeqCst) < 2 && Instant::now() < deadline { tokio::time::sleep(Duration::from_millis(2)).await; }
    tokio::time::sleep(Duration::from_millis(120)).await;
    let admitted_before_release = server.requests.lock().unwrap().len();
    release.store(true, Ordering::SeqCst);
    for task in tasks { assert_eq!(task.await.unwrap().unwrap(), "0x8f"); }
    assert_eq!(admitted_before_release, 2, "waiting requests must not reach HTTP while two slots are occupied");
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(server.requests.lock().unwrap().len(), 6);
}

#[tokio::test]
async fn cancelling_active_and_waiting_requests_releases_client_permit() {
    let started = Arc::new(AtomicBool::new(false));
    let seen = started.clone();
    let server = Server::new(move |request| {
        if request["params"][0] == "active" {
            seen.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(600));
        }
        Reply::Result(json!("0x8f"))
    });
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.max_concurrent_requests = 1;
    let rpc = RpcClient::new(&server.url, config).unwrap();
    let first = rpc.clone();
    let active = tokio::spawn(async move { first.request("eth_chainId", json!(["active"])).await });
    let deadline = Instant::now() + Duration::from_secs(1);
    while !started.load(Ordering::SeqCst) && Instant::now() < deadline { tokio::time::sleep(Duration::from_millis(2)).await; }
    assert!(started.load(Ordering::SeqCst));
    let second = rpc.clone();
    let waiting = tokio::spawn(async move { second.request("eth_chainId", json!(["waiting"])).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    waiting.abort(); assert!(waiting.await.unwrap_err().is_cancelled());
    active.abort(); assert!(active.await.unwrap_err().is_cancelled());
    assert_eq!(tokio::time::timeout(Duration::from_millis(250),
        rpc.request("eth_chainId", json!(["after-cancel"]))).await.unwrap().unwrap(), "0x8f");
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["params"][0], "after-cancel");
}

#[tokio::test]
async fn wrong_response_id_and_readonly_whitelist_are_rejected() {
    let server = Server::new(|_| Reply::Wire(json!({"jsonrpc":"2.0","id":999,"result":"0x8f"})));
    let rpc = RpcClient::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(rpc.request("eth_sendRawTransaction", json!(["0x00"])).await, Err(Error::InvalidConfig(_))));
    assert!(server.requests.lock().unwrap().is_empty());
    assert!(matches!(rpc.request("eth_chainId", json!([])).await, Err(Error::Decode(_))));
}

#[tokio::test]
async fn log_range_count_and_foreign_logs_are_rejected() {
    let wire = json!({"address":pool().address,"topics":[],"data":"0x","blockNumber":"0x7","blockHash":block().hash,"transactionHash":B256::repeat_byte(8),"logIndex":"0x0","removed":false});
    let server = Server::new(move |_| Reply::Result(json!([wire, wire])));
    let mut config = rpc_config(BlockPinning::NumberBracket);
    config.max_logs = 1;
    let reader = HttpReader::new(&server.url, config).unwrap();
    let error = reader.logs(pool().address, 0, 101).await.unwrap_err();
    assert_eq!(classify_rpc_error(&error), RpcErrorKind::RangeTooWide);
    assert!(server.requests.lock().unwrap().is_empty());
    let error = reader.logs(pool().address, 7, 7).await.unwrap_err();
    assert_eq!(classify_rpc_error(&error), RpcErrorKind::PayloadTooLarge);
    let foreign = Server::new(|_| Reply::Result(json!([{"address":Address::repeat_byte(99),"topics":[],"data":"0x","blockNumber":"0x7","blockHash":block().hash,"transactionHash":B256::repeat_byte(8),"logIndex":"0x0"}])));
    let reader = HttpReader::new(&foreign.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.logs(pool().address, 7, 7).await, Err(Error::Decode(_))));
}

const WATCH_HELPER: &str = "BARTERLEND_STATE_WATCH_TEST_HELPER";
const ECHOED_CREDENTIAL: &str = "provider echoed example-credential-123";

#[test]
fn watch_example_process() {
    if std::env::var_os(WATCH_HELPER).is_none() { return; }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let succeeded = runtime.block_on(watch_example::run_with_reporting());
    // Stop before libtest writes its own trailing text to a deliberately closed pipe.
    std::process::exit(i32::from(!succeeded));
}

fn watch_block(number: u64) -> BlockRef {
    BlockRef { number, hash: B256::repeat_byte((number + 1) as u8),
        parent_hash: if number == 0 { B256::ZERO } else { B256::repeat_byte(number as u8) },
        timestamp: number * 10, commitment: Commitment::Finalized }
}

fn watch_reply(request: &Value) -> Reply {
    match request["method"].as_str().unwrap() {
        "eth_chainId" => Reply::Result(json!("0x7a69")),
        "eth_getBlockByNumber" => {
            let tag = request["params"][0].as_str().unwrap();
            let number = if tag == "finalized" { 7 } else { u64::from_str_radix(tag.strip_prefix("0x").unwrap(),16).unwrap() };
            Reply::Result(header(watch_block(number)))
        }
        "eth_getCode" => Reply::Result(json!("0x6000")),
        "eth_getLogs" => {
            let encoded = CreditPool::Borrowed { account: Address::repeat_byte(11), market: 100, assetsBorrowed: 100, debtIncurred: 100, endAPR: U256::ZERO }.encode_log_data();
            Reply::Result(json!([{"address":pool().address,"topics":encoded.topics(),"data":encoded.data,
                "blockNumber":"0x7","blockHash":watch_block(7).hash,"transactionHash":B256::repeat_byte(12),"logIndex":"0x0","removed":false}]))
        }
        "eth_call" => {
            let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            let signature = &data[..4];
            if signature == CreditPool::multicallCall::SELECTOR {
                return pool_multicall_reply(request, |nested| {
                    let mut inner = request.clone(); inner["params"][0]["data"] = json!(nested);
                    match watch_reply(&inner) { Reply::Result(value) => serde_json::from_value(value).unwrap(),
                        _ => panic!("unexpected nested getter error") }
                });
            }
            if signature == CreditPool::versionCall::SELECTOR { Reply::Result(word(5)) }
            else if signature == CreditPool::assetCall::SELECTOR || signature == IPriceOracle::targetCall::SELECTOR {
                Reply::Result(json!(Bytes::from(U256::from_be_slice(pool().asset.address.as_slice()).to_be_bytes::<32>().to_vec())))
            }
            else if signature == CreditPool::collateralCall::SELECTOR || signature == IPriceOracle::sourceCall::SELECTOR {
                Reply::Result(json!(Bytes::from(U256::from_be_slice(pool().collateral.address.as_slice()).to_be_bytes::<32>().to_vec())))
            }
            else if signature == CreditPool::priceOracleCall::SELECTOR {
                Reply::Result(json!(Bytes::from(U256::from_be_slice(pool().oracle.as_slice()).to_be_bytes::<32>().to_vec())))
            }
            else if signature == [0x31,0x3c,0xe5,0x67] { Reply::Result(word(18)) }
            else if signature == CreditPool::getActiveMarketsCall::SELECTOR {
                let mut encoded = vec![0;64]; encoded[31]=32; Reply::Result(json!(Bytes::from(encoded)))
            }
            else if signature == CreditPool::getLiquidationLTVCall::SELECTOR { Reply::Result(word(800_000_000_000_000_000)) }
            else if signature == CreditPool::getStalePriceWindowCall::SELECTOR { Reply::Result(word(50)) }
            else if signature == CreditPool::getMinDebtRemainingAfterLiquidationCall::SELECTOR { Reply::Result(word(10)) }
            else if signature == CreditPool::getLiquidationGracePeriodCall::SELECTOR { Reply::Result(word(20)) }
            else if signature == CreditPool::getLiquidationDiscountMinCall::SELECTOR { Reply::Result(word(10_000_000_000_000_000)) }
            else if signature == CreditPool::getLiquidationDiscountMaxCall::SELECTOR { Reply::Result(word(100_000_000_000_000_000)) }
            else if signature == CreditPool::debtOfCall::SELECTOR || signature == CreditPool::collateralOfCall::SELECTOR { Reply::Result(word(100)) }
            else if signature == IPriceOracle::getPriceCall::SELECTOR { Reply::Error(-32000,ECHOED_CREDENTIAL) }
            else { panic!("unexpected watch getter"); }
        }
        _ => panic!("unexpected watch RPC method"),
    }
}

struct WatchProcess(Option<Child>);
impl Drop for WatchProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() { let _=child.kill();let _=child.wait(); }
    }
}

fn spawn_watch(server: &Server, checkpoint: &std::path::Path, continuous: bool) -> WatchProcess {
    WatchProcess(Some(Command::new(std::env::current_exe().unwrap())
        .args(["--exact","watch_example_process","--nocapture","--test-threads=1"])
        .env(WATCH_HELPER,"1").env("BL_STATE_RPC_URL",&server.url)
        .env("BL_STATE_CHAIN_ID","31337").env("BL_STATE_POOL",pool().address.to_string())
        .env("BL_STATE_DEPLOYMENT_BLOCK","0").env("BL_STATE_CHECKPOINT",checkpoint)
        .env("BL_STATE_POLLS",if continuous { "0" } else { "1" }).env("BL_STATE_POLL_MS","200")
        .env("BL_STATE_COMMITMENT","finalized").env("BL_STATE_PINNING","number_bracket")
        .env_remove("BL_STATE_GENESIS_HASH").env_remove("BL_STATE_DEPLOYMENT_BLOCK_HASH")
        .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap()))
}

fn finish_watch(mut process: WatchProcess) -> std::process::Output {
    let deadline=Instant::now()+Duration::from_secs(8);
    loop {
        if process.0.as_mut().unwrap().try_wait().unwrap().is_some() { break; }
        assert!(Instant::now()<deadline,"keyless watcher did not terminate within its local deadline");
        thread::sleep(Duration::from_millis(1));
    }
    process.0.take().unwrap().wait_with_output().unwrap()
}

#[test]
fn keyless_example_redacts_fatal_and_embedded_provider_errors() {
    let directory=tempfile::tempdir().unwrap();
    let fatal=Server::new(|_| Reply::Error(-32000,ECHOED_CREDENTIAL));
    let result=finish_watch(spawn_watch(&fatal,&directory.path().join("fatal.json"),false));
    assert!(!result.status.success());
    let stderr=String::from_utf8(result.stderr).unwrap();
    assert_eq!(stderr.trim(),"{\"event\":\"state_watch_failed\",\"code\":\"rpc_error\"}");
    assert!(!String::from_utf8(result.stdout).unwrap().contains(ECHOED_CREDENTIAL));
    let raw=Server::new(watch_reply);
    let result=finish_watch(spawn_watch(&raw,&directory.path().join("oracle.json"),false));
    assert!(result.status.success());
    assert!(result.stderr.is_empty());
    let stdout=String::from_utf8(result.stdout).unwrap();
    assert!(!stdout.contains(ECHOED_CREDENTIAL));
    let outcome:SyncOutcome=stdout.lines().find_map(|line| {
        let start=line.find('{')?;
        serde_json::from_str(&line[start..]).ok()
    }).unwrap();
    let SyncOutcome::Bootstrapped(snapshot)=outcome else { panic!("watcher did not publish initial snapshot"); };
    assert_eq!(snapshot.oracle,OracleStatus::Unavailable(OracleFailure::Rpc("rpc details omitted".into())));
    let position=snapshot.positions.values().next().unwrap();
    assert_eq!(position.amounts,PositionAmounts { debt:100,collateral:100 });
    assert_eq!(position.risk,RiskStatus::Unavailable(RiskFailure::Oracle(OracleFailure::Rpc("rpc details omitted".into()))));
    // Raw diagnostics remain available in the local checkpoint, whose permissions are private.
    let checkpoint=barterlend_state::storage::FileStore::new(directory.path().join("oracle.json")).load(None,None).unwrap().unwrap();
    assert!(matches!(checkpoint.snapshot.oracle,OracleStatus::Unavailable(OracleFailure::Rpc(message)) if message.contains(ECHOED_CREDENTIAL)));
}

#[test]
#[cfg(unix)]
fn keyless_example_closed_consumer_pipe_stops_successfully() {
    let directory=tempfile::tempdir().unwrap();
    let checkpoint=directory.path().join("pipe.json");
    let server=Server::new(watch_reply);
    let mut process=spawn_watch(&server,&checkpoint,true);
    let deadline=Instant::now()+Duration::from_secs(8);
    while !checkpoint.exists() {
        assert!(process.0.as_mut().unwrap().try_wait().unwrap().is_none(),"watcher stopped before its initial snapshot");
        assert!(Instant::now()<deadline,"watcher did not persist its initial snapshot");
        thread::sleep(Duration::from_millis(1));
    }
    drop(process.0.as_mut().unwrap().stdout.take());
    let result=finish_watch(process);
    assert!(result.status.success(),"closed read-only consumer pipe was treated as failure");
    assert!(result.stderr.is_empty());
    let calls=server.requests.lock().unwrap().len();
    thread::sleep(Duration::from_millis(250));
    assert_eq!(server.requests.lock().unwrap().len(),calls,"watcher kept requesting after its output pipe closed");
}


fn pinned_oracle_reply(request: &Value) -> Reply {
    match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_call" => {
            let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            if data.starts_with(&IPriceOracle::sourceCall::SELECTOR) {
                Reply::Result(json!(Bytes::from(pool().collateral.address.into_word().to_vec())))
            } else if data.starts_with(&IPriceOracle::targetCall::SELECTOR) {
                Reply::Result(json!(Bytes::from(pool().asset.address.into_word().to_vec())))
            } else {
                assert!(data.starts_with(&IPriceOracle::getPriceCall::SELECTOR));
                let bytes: Vec<u8> = [2u128,1,70].into_iter().flat_map(|n| U256::from(n).to_be_bytes::<32>()).collect();
                Reply::Result(json!(Bytes::from(bytes)))
            }
        }
        method => panic!("unexpected oracle RPC {method}"),
    }
}

fn pinned_descriptor_reply(request: &Value) -> Reply {
    match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => Reply::Result(header(block())),
        "eth_getCode" => Reply::Result(json!("0x60006000")),
        "eth_call" => {
            let data: Bytes = serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            if data.starts_with(&CreditPool::versionCall::SELECTOR) { return Reply::Result(word(5)); }
            if data.starts_with(&[0x31,0x3c,0xe5,0x67]) {
                return Reply::Result(word(if request["params"][0]["to"] == json!(pool().asset.address) { 6 } else { 18 }));
            }
            let address = if data.starts_with(&CreditPool::assetCall::SELECTOR) { pool().asset.address }
                else if data.starts_with(&CreditPool::collateralCall::SELECTOR) { pool().collateral.address }
                else { assert!(data.starts_with(&CreditPool::priceOracleCall::SELECTOR)); pool().oracle };
            Reply::Result(json!(Bytes::from(address.into_word().to_vec())))
        }
        method => panic!("unexpected descriptor RPC {method}"),
    }
}

#[tokio::test]
async fn dependent_oracle_reads_use_five_requests_and_keep_every_pinning_mode() {
    for mode in [BlockPinning::NumberBracket,BlockPinning::Eip1898,BlockPinning::RawHash] {
        let server=Server::new(pinned_oracle_reply);
        let reader=HttpReader::new(&server.url,rpc_config(mode)).unwrap();
        assert_eq!(reader.read_oracle(&pool(),&block()).await.unwrap(),OracleStatus::Ready(Price {
            numerator:U256::from(2),denominator:U256::from(1),updated_at:U256::from(70),
        }));
        let calls=server.requests.lock().unwrap();assert_eq!(calls.len(),5);
        assert_eq!(calls[0]["method"],"eth_getBlockByNumber");assert_eq!(calls[4]["method"],"eth_getBlockByNumber");
        let expected=match mode {
            BlockPinning::NumberBracket=>json!("0x7"), BlockPinning::RawHash=>json!(block().hash),
            BlockPinning::Eip1898=>json!({"blockHash":block().hash,"requireCanonical":true}),
        };
        for call in calls.iter().filter(|call|call["method"]=="eth_call") { assert_eq!(call["params"][1],expected); }
        let last_data:Bytes=serde_json::from_value(calls[3]["params"][0]["data"].clone()).unwrap();
        assert!(last_data.starts_with(&IPriceOracle::getPriceCall::SELECTOR),"price must follow both pair reads");
    }
}

#[tokio::test]
async fn descriptor_and_token_metadata_share_one_canonical_bracket() {
    let server=Server::new(pinned_descriptor_reply);
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let spec=PoolSpec { address:pool().address,deployment_block:7,deployment_block_hash:Some(block().hash),expected_version:5 };
    let descriptor=reader.describe_pool(&spec,&block()).await.unwrap();
    assert_eq!(descriptor.asset,pool().asset);assert_eq!(descriptor.collateral,pool().collateral);assert_eq!(descriptor.oracle,pool().oracle);
    let calls=server.requests.lock().unwrap();
    assert_eq!(calls.len(),10,"deployment binding + two canonical checks + seven pinned reads");
    assert_eq!(calls.iter().filter(|call|call["method"]=="eth_getBlockByNumber").count(),3);
    assert_eq!(calls.last().unwrap()["method"],"eth_getBlockByNumber");
    for call in calls.iter().filter(|call|call["method"]!="eth_getBlockByNumber") { assert_eq!(call["params"][1],"0x7"); }
}

#[tokio::test]
async fn primitive_and_config_batches_use_bounded_native_multicall() {
    let server=Server::new(|request| match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber"=>Reply::Result(header(block())),
        "eth_call"=>pool_multicall_reply(request, |_| Bytes::from(U256::from(5).to_be_bytes::<32>().to_vec())),_=>unreachable!(),
    });
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let keys:Vec<_>=(1..=33).map(|i|PositionKey { pool:pool().address,market:100,borrower:Address::repeat_byte(i) }).collect();
    assert_eq!(reader.read_positions(pool().address,&keys,&block()).await.unwrap().len(),33);
    assert_eq!(server.requests.lock().unwrap().len(),6,"66 values in two multicalls and two brackets");
    server.requests.lock().unwrap().clear();
    let markets:Vec<_>=(1..=22).collect();
    assert_eq!(reader.read_market_configs(pool().address,&markets,&block()).await.unwrap().len(),22);
    assert_eq!(server.requests.lock().unwrap().len(),6,"66 values in two multicalls and two brackets");
    server.requests.lock().unwrap().clear();
    assert!(reader.read_positions(pool().address,&[],&block()).await.unwrap().is_empty());
    assert!(reader.read_market_configs(pool().address,&[],&block()).await.unwrap().is_empty());
    assert_eq!(server.requests.lock().unwrap().len(),2,"empty requests retain their explicit canonical check");
}

#[tokio::test]
async fn native_pool_multicall_preserves_pinning_and_has_no_sender_or_gas_override() {
    for mode in [BlockPinning::NumberBracket, BlockPinning::Eip1898, BlockPinning::RawHash] {
        let server = Server::new(|request| match request["method"].as_str().unwrap() {
            "eth_getBlockByNumber" => Reply::Result(header(block())),
            "eth_call" => pool_multicall_reply(request, |data| {
                assert!([CreditPool::getLiquidationLTVCall::SELECTOR, CreditPool::getStalePriceWindowCall::SELECTOR,
                    CreditPool::getMinDebtRemainingAfterLiquidationCall::SELECTOR].iter().any(|s|data.starts_with(s)));
                Bytes::from(U256::from(10).to_be_bytes::<32>().to_vec())
            }), _ => unreachable!(),
        });
        let reader = HttpReader::new(&server.url, rpc_config(mode)).unwrap();
        let globals = reader.read_globals(pool().address, &block()).await.unwrap();
        assert_eq!(globals.stale_window, 10);
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 3, "two canonical headers plus one pool multicall");
        assert_eq!(calls[1]["params"][0]["to"], json!(pool().address));
        assert!(calls[1]["params"][0].get("from").is_none());
        assert!(calls[1]["params"][0].get("gas").is_none());
        let expected = match mode {
            BlockPinning::NumberBracket => json!("0x7"), BlockPinning::RawHash => json!(block().hash),
            BlockPinning::Eip1898 => json!({"blockHash":block().hash,"requireCanonical":true}),
        };
        assert_eq!(calls[1]["params"][1], expected);
    }
}

#[tokio::test]
async fn native_pool_multicall_rejects_bad_count_words_padding_and_trailing_encoding() {
    for malformed in 0..5 {
        let server = Server::new(move |request| {
            if request["method"] == "eth_getBlockByNumber" { return Reply::Result(header(block())); }
            assert_eq!(request["method"], "eth_call");
            let mut values = vec![Bytes::from(U256::from(5).to_be_bytes::<32>().to_vec()); 3];
            match malformed {
                0 => { values.pop(); }, 1 => values.push(Bytes::from(vec![0;32])),
                2 => values[0] = Bytes::from(vec![0;31]),
                3 => values[1] = Bytes::from(U256::MAX.to_be_bytes::<32>().to_vec()),
                _ => {},
            }
            let mut encoded = CreditPool::multicallCall::abi_encode_returns(&values);
            if malformed == 4 { encoded.extend_from_slice(&[0;32]); }
            Reply::Result(json!(Bytes::from(encoded)))
        });
        let reader = HttpReader::new(&server.url, rpc_config(BlockPinning::NumberBracket)).unwrap();
        assert!(matches!(reader.read_globals(pool().address, &block()).await, Err(Error::Decode(_))), "case {malformed}");
        assert_eq!(server.requests.lock().unwrap().len(), 3, "no individual-call fallback");
    }
}

#[tokio::test]
async fn native_pool_multicall_error_still_checks_reorg_and_pruning() {
    for (changed, code, message, pruned) in [(false,-32000,"missing trie node",true),
        (false,3,"missing trie node",false),(true,3,"execution reverted",false)] {
        let headers = Arc::new(AtomicUsize::new(0)); let observed = headers.clone();
        let server = Server::new(move |request| {
            if request["method"] == "eth_getBlockByNumber" {
                let mut at = block(); if observed.fetch_add(1,Ordering::Relaxed)>0 && changed { at.hash=B256::repeat_byte(99); }
                return Reply::Result(header(at));
            }
            assert_eq!(request["method"], "eth_call"); Reply::Error(code,message)
        });
        let reader = HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
        let result = reader.read_globals(pool().address,&block()).await;
        if changed { assert!(matches!(result,Err(Error::NonCanonical(7)))); }
        else if pruned { assert!(matches!(result,Err(Error::MissingBlock(7)))); }
        else { assert!(matches!(result,Err(Error::Rpc{code:3,..}))); }
        assert_eq!(headers.load(Ordering::Relaxed),2);
        assert_eq!(server.requests.lock().unwrap().len(),3);
    }
}

#[tokio::test]
async fn native_pool_multicall_does_not_return_a_successful_prefix() {
    let calls = Arc::new(AtomicUsize::new(0)); let seen = calls.clone();
    let server = Server::new(move |request| {
        if request["method"] == "eth_getBlockByNumber" { return Reply::Result(header(block())); }
        if seen.fetch_add(1,Ordering::Relaxed)>0 { return Reply::Error(3,"second chunk unavailable"); }
        pool_multicall_reply(request, |_| Bytes::from(U256::from(5).to_be_bytes::<32>().to_vec()))
    });
    let reader = HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let keys:Vec<_>=(1..=33).map(|i|PositionKey { pool:pool().address,market:100,borrower:Address::repeat_byte(i) }).collect();
    assert!(matches!(reader.read_positions(pool().address,&keys,&block()).await,Err(Error::Rpc{code:3,..})));
    assert_eq!(calls.load(Ordering::Relaxed),2);
    assert_eq!(server.requests.lock().unwrap().len(),6);
}

#[tokio::test]
async fn dependent_read_reorg_dominates_pair_price_and_metadata_errors() {
    for stage in 0..4 {
        let headers=Arc::new(AtomicUsize::new(0));let counter=headers.clone();
        let server=Server::new(move |request| {
            if request["method"]=="eth_getBlockByNumber" {
                let mut at=block();if counter.fetch_add(1,Ordering::Relaxed)>0 { at.hash=B256::repeat_byte(99); }
                return Reply::Result(header(at));
            }
            if request["method"]=="eth_call" {
                let data:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
                if stage==0 || stage==1 && data.starts_with(&IPriceOracle::getPriceCall::SELECTOR)
                    || stage==2 && data.starts_with(&CreditPool::assetCall::SELECTOR)
                    || stage==3 && data.starts_with(&[0x31,0x3c,0xe5,0x67]) { return Reply::Error(3,"test revert"); }
            }
            if stage<2 { pinned_oracle_reply(request) } else { pinned_descriptor_reply(request) }
        });
        let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
        let result=if stage<2 { reader.read_oracle(&pool(),&block()).await.map(|_|()) }
            else { reader.describe_pool(&PoolSpec { address:pool().address,deployment_block:0,deployment_block_hash:None,expected_version:5 },&block()).await.map(|_|()) };
        assert!(matches!(result,Err(Error::NonCanonical(7))),"stage {stage}: {result:?}");
        assert_eq!(headers.load(Ordering::Relaxed),2);
    }
}

#[tokio::test]
async fn dependent_oracle_preserves_pruning_and_revert_classification() {
    for price_stage in [false,true] {
        for (code,message,pruned) in [(-32000,"missing trie node",true),(3,"missing trie node",false),(-32000,"execution reverted: missing trie node",false)] {
            let server=Server::new(move |request| {
                if request["method"]=="eth_call" {
                    let data:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
                    if !price_stage || data.starts_with(&IPriceOracle::getPriceCall::SELECTOR) { return Reply::Error(code,message); }
                }
                pinned_oracle_reply(request)
            });
            let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
            let result=reader.read_oracle(&pool(),&block()).await;
            if pruned { assert!(matches!(result,Err(Error::MissingBlock(7)))); }
            else { assert!(matches!(result,Ok(OracleStatus::Unavailable(OracleFailure::Rpc(_))))); }
            assert_eq!(server.requests.lock().unwrap().iter().filter(|call|call["method"]=="eth_getBlockByNumber").count(),2);
        }
    }
}

#[tokio::test]
async fn public_pool_views_preserve_all_pins_and_decode_dynamic_and_borrower_quotes() {
    use barterlend_state::PinnedRead;
    for mode in [BlockPinning::NumberBracket,BlockPinning::Eip1898,BlockPinning::RawHash] {
        let server=Server::new(|request| {
            if request["method"]=="eth_getBlockByNumber" {return Reply::Result(header(block()));}
            pool_multicall_reply(request,|data| {
                if data.starts_with(&CreditPool::getActiveMarketsCall::SELECTOR) {
                    CreditPool::getActiveMarketsCall::abi_encode_returns(&vec![100,200]).into()
                } else {
                    let quote=CreditPool::previewBorrowCall::abi_decode_validate(data).unwrap();
                    assert_eq!(quote.borrower,Address::repeat_byte(9));
                    [U256::from(quote.amount).to_be_bytes::<32>(),U256::from(quote.amount+10).to_be_bytes::<32>()].concat().into()
                }
            })
        });
        let reader=HttpReader::new(&server.url,rpc_config(mode)).unwrap();
        let reads=[PinnedRead::call(pool().address,&CreditPool::getActiveMarketsCall{}),
            PinnedRead::call(pool().address,&CreditPool::previewBorrowCall{market:100,amount:2000,borrower:Address::repeat_byte(9)})];
        let values=reader.read_pool_views(pool().address,&reads,&block()).await.unwrap();
        assert_eq!(CreditPool::getActiveMarketsCall::abi_decode_returns_validate(&values[0]).unwrap(),vec![100,200]);
        assert_eq!(CreditPool::previewBorrowCall::abi_decode_returns_validate(&values[1]).unwrap().debt,2010);
        let requests=server.requests.lock().unwrap();assert_eq!(requests.len(),3);
        assert!(requests[1]["params"][0].get("from").is_none());assert!(requests[1]["params"][0].get("gas").is_none());
        let pin=match mode {BlockPinning::NumberBracket=>json!("0x7"),BlockPinning::RawHash=>json!(block().hash),
            BlockPinning::Eip1898=>json!({"blockHash":block().hash,"requireCanonical":true})};
        assert_eq!(requests[1]["params"][1],pin);
    }
}

#[tokio::test]
async fn public_pool_views_reject_mutations_foreign_targets_nested_and_noncanonical_before_rpc() {
    use barterlend_state::PinnedRead;
    let server=Server::new(|_|panic!("invalid input must not perform RPC"));
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let mut extra=CreditPool::assetCall{}.abi_encode();extra.extend_from_slice(&[0;32]);
    let calls=vec![PinnedRead::call(pool().asset.address,&CreditPool::assetCall{}),
        PinnedRead::Code{address:pool().address},
        PinnedRead::call(pool().address,&CreditPool::multicallCall{data:vec![]}),
        PinnedRead::Call{to:pool().address,data:Bytes::from_static(&[0xde,0xad,0xbe,0xef])},
        PinnedRead::Call{to:pool().address,data:extra.into()}];
    for call in calls {assert!(reader.read_pool_views(pool().address,&[call],&block()).await.is_err());}
    assert!(reader.read_pool_views(pool().address,&[],&block()).await.is_err());
    let many=vec![PinnedRead::call(pool().address,&CreditPool::assetCall{});65];
    assert!(reader.read_pool_views(pool().address,&many,&block()).await.is_err());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn public_pool_views_reorg_dominates_reverts_and_malformed_results_are_atomic() {
    use barterlend_state::PinnedRead;
    for failure in 0..5 {
        let headers=Arc::new(AtomicUsize::new(0));let seen=headers.clone();
        let server=Server::new(move |request| {
            if request["method"]=="eth_getBlockByNumber" {
                let mut at=block();if seen.fetch_add(1,Ordering::SeqCst)>0 && failure==0 {at.hash=B256::repeat_byte(99);}
                return Reply::Result(header(at));
            }
            if failure==0 {return Reply::Error(3,"execution reverted");}
            let mut values=vec![Bytes::from(U256::from(5).to_be_bytes::<32>().to_vec());2];
            if failure==1 {values.pop();}
            if failure==2 {values[1]=Bytes::from(U256::MAX.to_be_bytes::<32>().to_vec());}
            if failure==3 {values[1]=Bytes::from(vec![0;31]);}
            let mut bytes=CreditPool::multicallCall::abi_encode_returns(&values);
            if failure==4 {bytes.extend_from_slice(&[0;32]);}
            Reply::Result(json!(Bytes::from(bytes)))
        });
        let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
        let reads=[PinnedRead::call(pool().address,&CreditPool::debtOfCall{market:100,account:Address::repeat_byte(9)}),
            PinnedRead::call(pool().address,&CreditPool::collateralOfCall{market:100,account:Address::repeat_byte(9)})];
        let result=reader.read_pool_views(pool().address,&reads,&block()).await;
        if failure==0 {assert!(matches!(result,Err(Error::NonCanonical(7))));}
        else {assert!(matches!(result,Err(Error::Decode(_))),"case {failure}");}
        assert_eq!(headers.load(Ordering::SeqCst),2);assert_eq!(server.requests.lock().unwrap().len(),3);
    }
}

#[tokio::test]
async fn filtered_logs_require_wildcard_positions_and_match_explicit_topics() {
    let server=Server::new(|_| Reply::Result(json!([{
        "address":Address::repeat_byte(1),"topics":[B256::repeat_byte(2)],"data":"0x",
        "blockNumber":"0x7","blockHash":B256::repeat_byte(7),
        "transactionHash":B256::repeat_byte(8),"logIndex":"0x0","removed":false
    }])));
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let address=Address::repeat_byte(1);
    assert_eq!(reader.filtered_logs(address,7,7,&[None]).await.unwrap().len(),1);
    assert!(matches!(reader.filtered_logs(address,7,7,&[None,None]).await,Err(Error::Decode(_))));
    assert!(matches!(reader.filtered_logs(address,7,7,&[Some(B256::repeat_byte(3))]).await,Err(Error::Decode(_))));
    assert_eq!(reader.filtered_logs(address,7,7,&[Some(B256::repeat_byte(2))]).await.unwrap().len(),1);
    assert_eq!(reader.logs(address,7,7).await.unwrap().len(),1);
    let requests=server.requests.lock().unwrap();
    assert!(requests.last().unwrap()["params"][0].get("topics").is_none(),"legacy wire shape is preserved");
}

#[test]
fn absent_block_classification_never_accepts_generic_invalid_params_or_reverts() {
    use barterlend_state::rpc::is_missing_block_error;
    let monad="Block requested not found. Request might be querying historical state that is not available. If possible, reformulate query to point to more recent blocks";
    assert!(is_missing_block_error(-32602,monad));
    assert!(!is_missing_block_error(3,monad));
    assert!(!is_missing_block_error(-32602,&format!("execution reverted: {monad}")));
    for message in ["invalid params", "invalid params: header not found", "rate limit", "timeout", "blockHash objects unsupported"] {
        assert!(!is_missing_block_error(-32602,message));
    }
}

#[tokio::test]
async fn monad_absent_block_errors_qualify_hash_modes_and_map_pruned_state() {
    let monad="Block requested not found. Request might be querying historical state that is not available. If possible, reformulate query to point to more recent blocks";
    let server=Server::new(move |request|match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber"=>Reply::Result(header(block())),
        "eth_getBlockByHash"=>Reply::Result(Value::Null),
        "eth_call"=>{
            let pin=&request["params"][1];
            if pin==&json!("0x7") || pin==&json!(block().hash) || pin.get("blockHash")==Some(&json!(block().hash)) {Reply::Result(word(5))}
            else {Reply::Error(-32602,monad)}
        }
        _=>unreachable!(),
    });
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let capabilities=reader.probe_capabilities(pool().address,&block()).await.unwrap();
    assert!(capabilities.eip1898 && capabilities.raw_hash && capabilities.number_bracket);
    let unavailable=Server::new(move |request|match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber"=>Reply::Result(header(block())),
        "eth_call"=>Reply::Error(-32602,monad),
        _=>unreachable!(),
    });
    let reader=HttpReader::new(&unavailable.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    assert!(matches!(reader.contract_call(pool().address,&CreditPool::versionCall{},&block()).await,Err(Error::MissingBlock(7))));
}

#[tokio::test]
async fn factory_pages_find_only_registered_create_children_and_resume_without_history() {
    use barterlend_state::factory::{CreditPoolFactory as F,FactorySpec};
    let address=Address::repeat_byte(9);let nonce=Arc::new(AtomicUsize::new(5));let current=nonce.clone();
    let server=Server::new(move|request|match request["method"].as_str().unwrap(){
        "eth_getBlockByNumber"=>Reply::Result(header(block())),
        "eth_getCode"=>Reply::Result(json!("0x6000")),
        "eth_getTransactionCount"=>{assert_eq!(request["params"][1],"0x7");Reply::Result(json!(format!("0x{:x}",current.load(Ordering::SeqCst))))},
        "eth_call"=>{
            let bytes:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
            let encoded=if bytes.starts_with(&F::poolVersionCall::SELECTOR){F::poolVersionCall::abi_encode_returns(&U256::from(5))}
                else if bytes.starts_with(&F::previousFactoryCall::SELECTOR){F::previousFactoryCall::abi_encode_returns(&Address::ZERO)}
                else{let call=F::isPoolCall::abi_decode(&bytes).unwrap();F::isPoolCall::abi_encode_returns(&[address.create(2),address.create(4),address.create(5)].contains(&call.pool))};
            Reply::Result(json!(Bytes::from(encoded)))
        },_=>panic!("unexpected method: {}",request["method"]),
    });
    let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
    let spec=FactorySpec{address,code_hash:alloy_primitives::keccak256([0x60,0]),previous_factory:Address::ZERO};
    let first=reader.factory_page(&spec,1,2,&block()).await.unwrap();assert_eq!(first.pools,vec![address.create(2)]);assert_eq!(first.next_nonce,3);
    let next=reader.factory_page(&spec,first.next_nonce,64,&block()).await.unwrap();assert_eq!(next.pools,vec![address.create(4)]);assert_eq!(next.next_nonce,5);
    assert!(reader.factory_page(&spec,5,32,&block()).await.unwrap().pools.is_empty());
    nonce.store(6,Ordering::SeqCst);
    let appended=reader.factory_page(&spec,5,32,&block()).await.unwrap();assert_eq!(appended.pools,vec![address.create(5)]);assert_eq!(appended.next_nonce,6);
    let mut bad=spec.clone();bad.code_hash=B256::ZERO;assert!(reader.factory_page(&bad,1,32,&block()).await.is_err());
    bad.code_hash=B256::repeat_byte(3);assert!(matches!(reader.factory_page(&bad,1,32,&block()).await,Err(Error::IdentityMismatch(_))));
    bad=spec.clone();bad.previous_factory=Address::repeat_byte(8);assert!(reader.factory_page(&bad,1,32,&block()).await.is_err());
    assert!(reader.factory_page(&spec,7,32,&block()).await.is_err());
    let before=server.requests.lock().unwrap().len();
    for (start,size) in [(0,32),(1,0),(1,65)] {assert!(reader.factory_page(&spec,start,size,&block()).await.is_err());}
    assert_eq!(before,server.requests.lock().unwrap().len());
}

#[tokio::test]
async fn factory_page_rejects_trailing_membership_bytes_and_reorg() {
    use barterlend_state::factory::{CreditPoolFactory as F,FactorySpec};
    for reorg in [false,true] {
        let changed=Arc::new(AtomicBool::new(false));let flag=changed.clone();
        let server=Server::new(move|request|match request["method"].as_str().unwrap(){
            "eth_getBlockByNumber"=>{let mut b=block();if flag.load(Ordering::SeqCst)&&reorg{b.hash=B256::repeat_byte(8);}Reply::Result(header(b))},
            "eth_getCode"=>Reply::Result(json!("0x6000")),"eth_getTransactionCount"=>Reply::Result(json!("0x2")),
            "eth_call"=>{
                let bytes:Bytes=serde_json::from_value(request["params"][0]["data"].clone()).unwrap();
                let encoded=if bytes.starts_with(&F::poolVersionCall::SELECTOR){F::poolVersionCall::abi_encode_returns(&U256::from(5))}
                    else if bytes.starts_with(&F::previousFactoryCall::SELECTOR){F::previousFactoryCall::abi_encode_returns(&Address::ZERO)}
                    else{flag.store(true,Ordering::SeqCst);let mut value=F::isPoolCall::abi_encode_returns(&true);if !reorg{value.extend_from_slice(&[0;32]);}value};
                Reply::Result(json!(Bytes::from(encoded)))
            },_=>unreachable!(),
        });
        let reader=HttpReader::new(&server.url,rpc_config(BlockPinning::NumberBracket)).unwrap();
        let spec=FactorySpec{address:Address::repeat_byte(9),code_hash:alloy_primitives::keccak256([0x60,0]),previous_factory:Address::ZERO};
        let result=reader.factory_page(&spec,1,32,&block()).await;
        assert!(if reorg{matches!(result,Err(Error::NonCanonical(_)))}else{matches!(result,Err(Error::Decode(_)))});
    }
}

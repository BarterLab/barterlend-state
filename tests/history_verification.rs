//! Canonical ancestry reuse is bounded, shared, and independent of TTL or finality assumptions.
use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::Duration};
use alloy_primitives::{B256, keccak256};
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
                // accept can inherit O_NONBLOCK; a timeout does not clear it.
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
                // The body-limit test deliberately closes the connection after headers.
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
fn requested(request: &Value) -> u64 {
    assert_eq!(request["method"], "eth_getBlockByNumber");
    u64::from_str_radix(request["params"][0].as_str().unwrap().strip_prefix("0x").unwrap(), 16).unwrap()
}
fn witnesses(from: u64, to: u64) -> Vec<(u64, B256)> {
    (from..=to).map(|number| (number, block(number).hash)).collect()
}

#[tokio::test]
async fn dense_history_is_shared_by_four_pool_readers_without_caching_canonical_checks() {
    let server = Server::new(|request| Reply::Result(header(block(requested(request)))));
    let reader = HttpReader::new(&server.url, config()).unwrap();
    let values = witnesses(1, 100);
    let clone = reader.clone();
    let control = reader.control_reader(config()).unwrap();
    let (a,b,c,d) = tokio::join!(reader.verify_log_blocks(&values, Commitment::Latest),
        clone.verify_log_blocks(&values, Commitment::Safe), control.verify_log_blocks(&values, Commitment::Finalized),
        reader.verify_log_blocks(&values, Commitment::Latest));
    for result in [a,b,c,d] { result.unwrap(); }
    // One immutable chain + two live anchor checks for every caller (not 4*100 headers).
    assert_eq!(server.requests.lock().unwrap().len(), 99 + 8);
    reader.block(1, Commitment::Latest).await.unwrap();
    reader.block(1, Commitment::Latest).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 99 + 10);
}

#[tokio::test]
async fn cached_intermediate_hash_mismatch_fails_closed() {
    let server = Server::new(|request| Reply::Result(header(block(requested(request)))));
    let reader = HttpReader::new(&server.url, config()).unwrap();
    let mut values = witnesses(1, 20);
    reader.verify_log_blocks(&values, Commitment::Latest).await.unwrap();
    values[8].1 = B256::repeat_byte(99);
    assert!(matches!(reader.verify_log_blocks(&values, Commitment::Latest).await, Err(Error::NonCanonical(9))));
}

#[tokio::test]
async fn inconsistent_parent_header_is_rejected_before_any_successful_verification() {
    let server = Server::new(|request| {
        let mut value = block(requested(request));
        if value.number == 10 { value.hash = B256::repeat_byte(99); }
        Reply::Result(header(value))
    });
    let reader = HttpReader::new(&server.url, config()).unwrap();
    assert!(matches!(reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await, Err(Error::NonCanonical(10))));
}

#[tokio::test]
async fn reorg_during_walk_is_detected_by_final_anchor_check() {
    let anchor_reads = Arc::new(AtomicUsize::new(0));
    let reads = anchor_reads.clone();
    let server = Server::new(move |request| {
        let mut value = block(requested(request));
        if value.number == 20 && reads.fetch_add(1, Ordering::SeqCst) >= 1 { value.hash = B256::repeat_byte(99); }
        Reply::Result(header(value))
    });
    let reader = HttpReader::new(&server.url, config()).unwrap();
    let result = reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await;
    assert!(matches!(result, Err(Error::NonCanonical(20))), "unexpected verification result: {result:?}");
    assert!(matches!(reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await, Err(Error::NonCanonical(20))));
}

#[tokio::test]
async fn cached_old_branch_cannot_certify_replacement_history_after_reorg() {
    fn fork(number: u64) -> BlockRef {
        let mut value = block(number);
        if number >= 10 {
            value.hash = keccak256((number + 10000).to_be_bytes());
            if number > 10 { value.parent_hash = keccak256((number + 9999).to_be_bytes()); }
        }
        value
    }
    let reorg = Arc::new(AtomicBool::new(false)); let active = reorg.clone();
    let server = Server::new(move |request| Reply::Result(header(
        if active.load(Ordering::SeqCst) { fork(requested(request)) } else { block(requested(request)) })));
    let reader = HttpReader::new(&server.url, config()).unwrap();
    reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await.unwrap();
    reorg.store(true, Ordering::SeqCst);
    assert!(matches!(reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await, Err(Error::NonCanonical(20))));
    let replacement: Vec<_> = (1..=20).map(|number| (number, fork(number).hash)).collect();
    reader.verify_log_blocks(&replacement, Commitment::Latest).await.unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|request| requested(request) == 10).count(), 2);
    assert_eq!(requests.iter().filter(|request| requested(request) == 8).count(), 1);
}

#[tokio::test]
async fn cancelled_walk_retains_verified_immutable_headers_for_the_next_attempt() {
    let arrived = Arc::new(AtomicBool::new(false)); let stalled = arrived.clone();
    let once = Arc::new(AtomicBool::new(true)); let first = once.clone();
    let server = Server::new(move |request| {
        let number = requested(request);
        if number == 10 && first.swap(false, Ordering::SeqCst) {
            stalled.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(150));
        }
        Reply::Result(header(block(number)))
    });
    let reader = HttpReader::new(&server.url, config()).unwrap();
    let task_reader = reader.clone();
    let task = tokio::spawn(async move { task_reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !arrived.load(Ordering::SeqCst) { tokio::time::sleep(Duration::from_millis(1)).await; }
    }).await.unwrap();
    task.abort(); assert!(task.await.unwrap_err().is_cancelled());
    reader.verify_log_blocks(&witnesses(1,20), Commitment::Latest).await.unwrap();
    let requests = server.requests.lock().unwrap();
    // The previous eight-header window completed before the stalled window.
    // Other requests in the cancelled window are allowed to be retried.
    for number in 12..20 { assert_eq!(requests.iter().filter(|r| requested(r) == number).count(), 1); }
    assert_eq!(requests.iter().filter(|r| requested(r) == 10).count(), 2);
}

#[tokio::test]
async fn missing_headers_use_bounded_actual_http_concurrency() {
    let active = Arc::new(AtomicUsize::new(0)); let maximum = Arc::new(AtomicUsize::new(0));
    let pending = active.clone(); let high = maximum.clone();
    let server = Server::new(move |request| {
        let count = pending.fetch_add(1, Ordering::SeqCst) + 1;
        high.fetch_max(count, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(20));
        let reply = Reply::Result(header(block(requested(request))));
        pending.fetch_sub(1, Ordering::SeqCst);
        reply
    });
    let reader = HttpReader::new(&server.url, RpcConfig { max_concurrent_requests: 3, ..config() }).unwrap();
    reader.verify_log_blocks(&witnesses(1,30), Commitment::Latest).await.unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 3, "prefetch must use the configured HTTP capacity");
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(server.requests.lock().unwrap().len(), 31);
}

#[tokio::test]
async fn sparse_ranges_remain_bounded_by_log_count_and_invalid_order_is_rejected_without_rpc() {
    let server = Server::new(|request| Reply::Result(header(block(requested(request)))));
    let reader = HttpReader::new(&server.url, config()).unwrap();
    reader.verify_log_blocks(&[(1,block(1).hash), (50000,block(50000).hash)], Commitment::Latest).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    assert!(matches!(reader.verify_log_blocks(&[(2,block(2).hash),(1,block(1).hash)], Commitment::Latest).await, Err(Error::InvalidConfig(_))));
    assert!(matches!(reader.verify_log_blocks(&[(1,block(1).hash),(1,block(1).hash)], Commitment::Latest).await, Err(Error::InvalidConfig(_))));
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn bounded_cache_evicts_old_history_and_refetches_without_trusting_height() {
    let server = Server::new(|request| Reply::Result(header(block(requested(request)))));
    let reader = HttpReader::new(&server.url, config()).unwrap();
    for index in 0..5 {
        reader.verify_log_blocks(&witnesses(index * 512 + 1, (index + 1) * 512), Commitment::Latest).await.unwrap();
    }
    let before = server.requests.lock().unwrap().len();
    reader.verify_log_blocks(&witnesses(1,512), Commitment::Latest).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len() - before, 513);
}

#[tokio::test]
async fn history_fixture_closes_idle_connection_instead_of_leaving_shutdown_clone_open() {
    let server=Server::new(|request|Reply::Result(header(block(requested(request)))));
    let reader=HttpReader::new(&server.url,config()).unwrap();
    reader.block(1,Commitment::Latest).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    reader.block(2,Commitment::Latest).await.unwrap();
}

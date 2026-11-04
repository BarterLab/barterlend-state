//! Actual loopback HTTP checks for explicitly reserved control RPC permits.

use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, Ordering}}, thread,
    time::{Duration, Instant}};

use alloy_primitives::B256;
use barterlend_state::{Commitment, Error, HttpReader, StateReader, rpc::RpcConfig};
use serde_json::{Value, json};
use tokio::sync::Notify;

#[derive(Debug)]
struct Arrival { endpoint: u8, method: String, at: Instant }

type Arrivals = Arc<Mutex<Vec<Arrival>>>;

#[derive(Default)]
struct Gate { released: Mutex<bool>, changed: Condvar, entered: Notify }

impl Gate {
    fn hold(&self) {
        self.entered.notify_one();
        let released = self.released.lock().unwrap();
        let _ = self.changed.wait_timeout_while(released, Duration::from_secs(3), |released| !*released).unwrap();
    }
    fn release(&self) { *self.released.lock().unwrap() = true; self.changed.notify_all(); }
}

struct Server {
    url: String,
    stop: Arc<AtomicBool>,
    gate: Option<Arc<Gate>>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(endpoint: u8, arrivals: Arrivals, gate: Option<Arc<Gate>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap(); listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false)); let stopping = stop.clone();
        let held = gate.clone();
        let listener_thread = thread::spawn(move || {
            let mut workers = Vec::new();
            while !stopping.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(stream) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1)); continue;
                    }
                    Err(error) => panic!("loopback listener: {error}"),
                };
                if stopping.load(Ordering::Acquire) { break; }
                stream.set_nonblocking(false).unwrap();
                let arrivals = arrivals.clone(); let held = held.clone();
                workers.push(thread::spawn(move || {
                    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                    let Some(request) = read_request(&stream) else { return; };
                    let method = request["method"].as_str().unwrap().to_owned();
                    arrivals.lock().unwrap().push(Arrival { endpoint, method: method.clone(), at: Instant::now() });
                    let result = match method.as_str() {
                        "eth_call" => { if let Some(gate) = held { gate.hold(); } json!("0x") }
                        "eth_getBlockByNumber" => json!({"number":"0x42", "hash":B256::repeat_byte(66),
                            "parentHash":B256::repeat_byte(65), "timestamp":"0x64"}),
                        "eth_chainId" => json!("0x8f"),
                        other => panic!("unexpected loopback RPC method: {other}"),
                    };
                    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0", "id":request["id"], "result":result})).unwrap();
                    if write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).is_ok() {
                        let _ = stream.write_all(&body);
                    }
                }));
            }
            for worker in workers { worker.join().unwrap(); }
        });
        Self { url: format!("http://{address}"), stop, gate, listener_thread: Some(listener_thread) }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(gate) = &self.gate { gate.release(); }
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.url.strip_prefix("http://").unwrap());
        if let Some(thread) = self.listener_thread.take() {
            if thread.join().is_err() && !thread::panicking() { panic!("loopback server failed"); }
        }
    }
}

fn read_request(stream: &TcpStream) -> Option<Value> {
    let mut input = BufReader::new(stream.try_clone().ok()?); let mut line = String::new();
    if input.read_line(&mut line).ok()? == 0 || !line.starts_with("POST ") { return None; }
    let mut length = None;
    loop {
        line.clear(); if input.read_line(&mut line).ok()? == 0 { return None; }
        if line == "\r\n" { break; }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") { length = Some(value.trim().parse::<usize>().ok()?); }
    }
    let length = length?; if length > 1024 * 1024 { return None; }
    let mut body = vec![0; length]; input.read_exact(&mut body).ok()?;
    serde_json::from_slice(&body).ok()
}

fn config(interval: Duration) -> RpcConfig {
    RpcConfig { max_concurrent_requests: 1, min_request_interval: interval,
        max_retries: 0, request_timeout: Duration::from_secs(2), ..Default::default() }
}

#[tokio::test]
async fn control_head_finishes_before_held_bulk_call_while_ordinary_clone_waits() {
    let arrivals = Arc::new(Mutex::new(Vec::new())); let gate = Arc::new(Gate::default());
    let server = Server::new(0, arrivals.clone(), Some(gate.clone()));
    let transport = HttpReader::new(&server.url, RpcConfig { max_concurrent_requests:2, ..config(Duration::ZERO) }).unwrap();
    let bulk = transport.control_reader(config(Duration::ZERO)).unwrap();
    let control = transport.control_reader(config(Duration::ZERO)).unwrap();
    let held_bulk = bulk.clone();
    let bulk_task = tokio::spawn(async move { held_bulk.rpc().request("eth_call", json!([{}, "latest"])).await });
    tokio::time::timeout(Duration::from_secs(1), gate.entered.notified()).await.unwrap();
    let ordinary_clone = bulk.clone();
    let ordinary_task = tokio::spawn(async move { ordinary_clone.head(Commitment::Latest).await });
    tokio::task::yield_now().await;
    let priority_result = tokio::time::timeout(Duration::from_secs(1), control.head(Commitment::Latest)).await;
    let ordinary_was_blocked = !ordinary_task.is_finished();
    let methods_before_release: Vec<_> = arrivals.lock().unwrap().iter().map(|request| request.method.clone()).collect();
    // Always release/join before assertions, including a broken clone-based control implementation.
    gate.release(); bulk_task.await.unwrap().unwrap();
    let ordinary_result = tokio::time::timeout(Duration::from_secs(1), ordinary_task).await.unwrap().unwrap().unwrap();
    let head = priority_result.expect("control head must complete before bulk release").unwrap();
    assert_eq!(head.number, 66); assert_eq!(head.commitment, Commitment::Latest);
    assert_eq!(ordinary_result, head); assert!(ordinary_was_blocked, "ordinary clones must retain shared bulk permits");
    assert_eq!(methods_before_release, vec!["eth_call", "eth_getBlockByNumber"]);
}

#[tokio::test]
async fn control_and_bulk_share_real_request_rate_and_bidirectional_endpoint_selection() {
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let first = Server::new(0, arrivals.clone(), None); let second = Server::new(1, arrivals.clone(), None);
    let interval = Duration::from_millis(150);
    let bulk = HttpReader::with_endpoints(&[first.url.clone(), second.url.clone()], config(interval)).unwrap();
    let control = bulk.control_reader(config(interval)).unwrap();
    assert_eq!(bulk.rpc().request("eth_chainId", json!([])).await.unwrap(), json!("0x8f"));
    control.rpc().select_endpoint(1).unwrap(); assert_eq!(bulk.rpc().endpoint_index(), 1);
    assert!(bulk.endpoint_matches(&reqwest::Url::parse(&second.url).unwrap()));
    control.rpc().request("eth_chainId", json!([])).await.unwrap();
    bulk.head(Commitment::Latest).await.unwrap();
    bulk.rpc().select_endpoint(0).unwrap(); assert_eq!(control.rpc().endpoint_index(), 0);
    assert!(control.endpoint_matches(&reqwest::Url::parse(&first.url).unwrap()));
    control.head(Commitment::Latest).await.unwrap();
    let requests = arrivals.lock().unwrap();
    assert_eq!(requests.iter().map(|request| request.endpoint).collect::<Vec<_>>(), vec![0, 1, 1, 0]);
    for adjacent in requests.windows(2) {
        assert!(adjacent[1].at.duration_since(adjacent[0].at) >= interval - Duration::from_millis(30),
            "bulk/control must share the request-start rate budget: {adjacent:?}");
    }
}

#[tokio::test]
async fn control_transport_rejects_mutation_and_incompatible_rate_before_http() {
    let arrivals = Arc::new(Mutex::new(Vec::new())); let server = Server::new(0, arrivals.clone(), None);
    let bulk = HttpReader::new(&server.url, config(Duration::ZERO)).unwrap();
    assert!(matches!(bulk.control_reader(config(Duration::from_millis(1))), Err(Error::InvalidConfig(_))));
    let control = bulk.control_reader(config(Duration::ZERO)).unwrap();
    for reader in [&bulk, &control] {
        for method in ["eth_sendRawTransaction", "eth_sendTransaction", "personal_unlockAccount"] {
            assert!(matches!(reader.rpc().request(method, json!([])).await, Err(Error::InvalidConfig(_))));
        }
    }
    assert!(arrivals.lock().unwrap().is_empty(), "no forbidden request may reach HTTP");
}

#[tokio::test]
async fn cancelled_rate_waiters_do_not_leave_reserved_future_slots() {
    let interval = Duration::from_millis(250);
    let rpc = barterlend_state::rpc::RpcClient::new("http://127.0.0.1:9", RpcConfig {
        min_request_interval:interval, ..Default::default()
    }).unwrap();
    let started = Instant::now();
    drop(rpc.acquire_request_permit().await.unwrap());
    let mut waiting = Vec::new();
    for _ in 0..32 {
        let rpc = rpc.clone();
        waiting.push(tokio::spawn(async move { rpc.acquire_request_permit().await }));
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
    for task in &waiting { task.abort(); }
    for task in waiting { assert!(task.await.unwrap_err().is_cancelled()); }
    drop(tokio::time::timeout(Duration::from_millis(500), rpc.acquire_request_permit()).await
        .expect("cancelled waiters must not reserve seconds of future capacity").unwrap());
    assert!(started.elapsed() >= interval);
}

#[tokio::test]
async fn completion_window_is_shared_by_lanes_and_external_permits() {
    let root = barterlend_state::rpc::RpcClient::new("http://127.0.0.1:9", RpcConfig {
        min_request_interval: Duration::from_secs(10), max_concurrent_requests: 4, ..Default::default()
    }).unwrap().with_completion_rate_limit(2).unwrap();
    let bulk = root.control_transport(root.config().clone()).unwrap();
    let control = root.control_transport(root.config().clone()).unwrap();
    let a = bulk.acquire_request_permit().await.unwrap();
    // The completion window replaces interval pacing, rather than imposing
    // two independent delays on the same quota.
    let b = tokio::time::timeout(Duration::from_millis(100), control.acquire_request_permit()).await.unwrap().unwrap();
    // Dispatch can be delayed arbitrarily: in-flight slots must not expire.
    tokio::time::sleep(Duration::from_millis(1050)).await;
    assert!(tokio::time::timeout(Duration::from_millis(20), root.acquire_request_permit()).await.is_err());
    drop(a);
    assert!(tokio::time::timeout(Duration::from_millis(850), bulk.acquire_request_permit()).await.is_err());
    let c = tokio::time::timeout(Duration::from_millis(400), control.acquire_request_permit()).await.unwrap().unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(20), root.acquire_request_permit()).await.is_err());
    drop((b, c));
    assert!(root.clone().with_completion_rate_limit(3).is_err());
    assert!(bulk.with_completion_rate_limit(3).is_err());
}

#[tokio::test]
async fn completion_window_caps_actual_http_across_endpoint_switches() {
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let first = Server::new(0, arrivals.clone(), None);
    let second = Server::new(1, arrivals.clone(), None);
    let root = HttpReader::with_endpoints(&[first.url.clone(), second.url.clone()], RpcConfig {
        max_concurrent_requests: 4, ..config(Duration::ZERO)
    }).unwrap().with_completion_rate_limit(2).unwrap();
    let control = root.control_reader(root.rpc().config().clone()).unwrap();
    let mut jobs = Vec::new();
    for reader in [root.clone(), control.clone()] {
        jobs.push(tokio::spawn(async move { reader.rpc().request("eth_chainId", json!([])).await.unwrap() }));
    }
    for job in jobs { job.await.unwrap(); }
    control.rpc().select_endpoint(1).unwrap();
    root.rpc().request("eth_chainId", json!([])).await.unwrap();
    let requests = arrivals.lock().unwrap();
    assert_eq!(requests.len(), 3);
    let first_completion_bound = requests[..2].iter().map(|r| r.at).min().unwrap();
    assert!(requests[2].at.duration_since(first_completion_bound) >= Duration::from_secs(1));
    assert_eq!(requests[2].endpoint, 1);
}

#[tokio::test]
async fn lanes_and_external_permits_share_the_total_concurrency_cap() {
    let rpc = barterlend_state::rpc::RpcClient::new("http://127.0.0.1:9", RpcConfig {
        min_request_interval:Duration::ZERO, max_concurrent_requests:3, ..Default::default()
    }).unwrap();
    let control = rpc.control_transport(RpcConfig { max_concurrent_requests:2, ..rpc.config().clone() }).unwrap();
    let rpc = rpc.control_transport(RpcConfig { max_concurrent_requests:2, ..rpc.config().clone() }).unwrap();
    let bulk_a = rpc.acquire_request_permit().await.unwrap();
    let bulk_b = rpc.acquire_request_permit().await.unwrap();
    let control_a = control.acquire_request_permit().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(25), control.acquire_request_permit()).await.is_err(),
        "a separate lane must not bypass the shared total cap");
    drop(bulk_a);
    let control_b = tokio::time::timeout(Duration::from_millis(100), control.acquire_request_permit()).await.unwrap().unwrap();
    drop((bulk_b,control_a,control_b));
    let mut invalid = rpc.config().clone();invalid.max_concurrent_requests=4;
    assert!(rpc.control_transport(invalid).is_err());
}

#[tokio::test]
async fn delayed_runtime_does_not_release_overdue_rate_waiters_as_a_burst() {
    let interval=Duration::from_millis(100);
    let rpc=barterlend_state::rpc::RpcClient::new("http://127.0.0.1:9",RpcConfig {
        min_request_interval:interval,..Default::default()
    }).unwrap();
    drop(rpc.acquire_request_permit().await.unwrap());
    let mut jobs=Vec::new();
    for _ in 0..4 {
        let rpc=rpc.clone();jobs.push(tokio::spawn(async move {
            let _permit=rpc.acquire_request_permit().await.unwrap();Instant::now()
        }));
    }
    tokio::task::yield_now().await;
    // Deliberately stall this single-thread runtime beyond all former future slots.
    std::thread::sleep(Duration::from_millis(450));
    let mut admitted=Vec::new();for job in jobs {admitted.push(job.await.unwrap());}admitted.sort();
    for pair in admitted.windows(2) {
        assert!(pair[1].duration_since(pair[0])>=interval,"overdue timers must not create a request burst");
    }
}

#[tokio::test]
async fn queued_http_requests_do_not_bunch_when_first_admitted_future_is_delayed() {
    use std::{future::Future,task::Poll};
    let arrivals=Arc::new(Mutex::new(Vec::new()));let server=Server::new(0,arrivals.clone(),None);
    let interval=Duration::from_millis(180);
    let rpc=barterlend_state::rpc::RpcClient::new(&server.url,RpcConfig{
        max_concurrent_requests:4,..config(interval)}).unwrap();
    let first=rpc.request("eth_chainId",json!([]));tokio::pin!(first);
    // Stop polling the first request after admission but before its asynchronous
    // connect/write can finish. This models executor starvation after rate admission.
    std::future::poll_fn(|cx|{assert!(first.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
    let mut jobs=Vec::new();for _ in 0..3 {let rpc=rpc.clone();jobs.push(tokio::spawn(async move{rpc.request("eth_chainId",json!([])).await}));}
    tokio::task::yield_now().await;
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(first.await.unwrap(),json!("0x8f"));for job in jobs{assert_eq!(job.await.unwrap().unwrap(),json!("0x8f"));}
    let mut times:Vec<_>=arrivals.lock().unwrap().iter().map(|a|a.at).collect();times.sort();assert_eq!(times.len(),4);
    // A generous50ms server/scheduling margin still detects near-simultaneous writes.
    let gaps:Vec<_>=times.windows(2).map(|p|p[1].duration_since(p[0]).as_millis()).collect();
    assert!(gaps.iter().all(|gap|*gap>=130),"admitted but unpolled requests bunched at actual HTTP server: {gaps:?}");
}

#[tokio::test]
async fn reapplying_coalescing_preserves_shared_queued_checks_but_never_completed_results() {
    let arrivals=Arc::new(Mutex::new(Vec::new()));let server=Server::new(0,arrivals.clone(),None);
    let rpc=barterlend_state::rpc::RpcClient::new(&server.url,config(Duration::ZERO)).unwrap()
        .with_pending_read_coalescing(true);
    let earlier=rpc.clone();let reapplied=rpc.with_pending_read_coalescing(true);
    let hold=reapplied.acquire_request_permit().await.unwrap();
    let a=tokio::spawn(async move{earlier.request("eth_chainId",json!([])).await});
    let later=reapplied.clone();let b=tokio::spawn(async move{later.request("eth_chainId",json!([])).await});
    tokio::time::sleep(Duration::from_millis(50)).await;drop(hold);
    assert_eq!(a.await.unwrap().unwrap(),json!("0x8f"));assert_eq!(b.await.unwrap().unwrap(),json!("0x8f"));
    assert_eq!(arrivals.lock().unwrap().len(),1,"reconfiguration must preserve the pending group shared with existing clones");
    reapplied.request("eth_chainId",json!([])).await.unwrap();
    assert_eq!(arrivals.lock().unwrap().len(),2,"the closing check must perform fresh HTTP");
}

#[tokio::test]
async fn ready_budget_coalesces_concurrent_reads_but_not_the_next_check() {
    let arrivals=Arc::new(Mutex::new(Vec::new()));let server=Server::new(0,arrivals.clone(),None);
    let rpc=barterlend_state::rpc::RpcClient::new(&server.url,RpcConfig {
        max_concurrent_requests:4,..config(Duration::ZERO)
    }).unwrap().with_completion_rate_limit(15).unwrap().with_pending_read_coalescing(true);
    let other=rpc.clone();
    let (a,b)=tokio::join!(rpc.request("eth_chainId",json!([])),other.request("eth_chainId",json!([])));
    assert_eq!(a.unwrap(),json!("0x8f"));assert_eq!(b.unwrap(),json!("0x8f"));
    assert_eq!(arrivals.lock().unwrap().len(),1,"concurrent queued checks should share even when admission is immediately available");
    rpc.request("eth_chainId",json!([])).await.unwrap();
    assert_eq!(arrivals.lock().unwrap().len(),2,"subsequent checks must not reuse a completed result");
}

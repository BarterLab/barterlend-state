//! Read-only HTTP JSON-RPC with shared rate limits and bounded retries.

use std::{sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::Duration};

use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::{sync::{Mutex, Semaphore, OwnedSemaphorePermit}, time::{Instant, sleep, sleep_until}};

use crate::{Error, Result};
mod coalescing;
mod completion_limit;

/// Recognizes specific absent-block errors, never contract reverts or generic
/// invalid parameters. This includes Monad's observed historical-state error.
/// Callers still have to verify positive reads and canonical block identity.
pub fn is_missing_block_error(code:i64, message:&str)->bool {
    let message=message.to_ascii_lowercase();
    if message.contains("execution reverted") {return false;}
    match code {
        -32000|-32001 => ["unknown block","block not found","header not found","block does not exist","block out of range"]
            .iter().any(|specific|message.contains(specific)),
        -32602 => (message.starts_with("blockoutofrangeerror: block height is ") && message.contains(" but requested was "))
            || message.starts_with("block requested not found. request might be querying historical state that is not available."),
        _ => false,
    }
}

/// Explicit eth_call/eth_getCode pinning; unsupported modes never fall back to latest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlockPinning {
    /// EIP1898 blockHash object with requireCanonical.
    Eip1898,
    /// Hash string block parameter for endpoints that support it.
    RawHash,
    /// Default: numbered reads bracketed by canonical-header checks in HttpReader.
    #[default]
    NumberBracket,
}

/// Timeouts, retries, concurrency and response-size limits.
#[derive(Clone, Debug)]
pub struct RpcConfig {
    /// Block parameter format used by HttpReader.
    pub pinning: BlockPinning,
    /// Timeout per HTTP attempt; bounded retries are separate attempts.
    pub request_timeout: Duration,
    /// Body cap enforced on Content-Length and actual received chunks.
    pub max_response_bytes: usize,
    /// Additional attempts for transient or rate-limit errors; maximum 8.
    pub max_retries: usize,
    /// Initial exponential retry delay, capped at 30 seconds.
    pub retry_backoff: Duration,
    /// Shared minimum request-start interval, including clones. An explicitly
    /// configured completion window replaces this pacing policy.
    pub min_request_interval: Duration,
    /// Concurrent requests in this transport lane, including rate waits; maximum 256.
    /// When creating a root client, this also sets the total cap for derived lanes.
    pub max_concurrent_requests: usize,
    /// Reader inclusive log-range cap; align with EngineConfig.log_chunk_size.
    pub max_log_range: u64,
    /// Logs per reader response; excess never returns partially.
    pub max_logs: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            pinning: BlockPinning::NumberBracket,
            request_timeout: Duration::from_secs(15),
            max_response_bytes: 8 * 1024 * 1024,
            max_retries: 2,
            retry_backoff: Duration::from_millis(250),
            min_request_interval: Duration::from_millis(20),
            max_concurrent_requests: 8,
            max_log_range: 100,
            max_logs: 10_000,
        }
    }
}

impl RpcConfig {
    /// Validates finite positive budgets and duration/retry/concurrency bounds.
    pub fn validate(&self) -> Result<()> {
        if self.request_timeout.is_zero() || self.max_response_bytes == 0
            || self.max_concurrent_requests == 0 || self.max_concurrent_requests > 256
            || self.max_log_range == 0 || self.max_logs == 0 || self.max_retries > 8
        {
            return Err(Error::InvalidConfig("RPC limits must be positive; concurrency <= 256, retries <= 8".into()));
        }
        if self.min_request_interval > Duration::from_secs(60)
            || self.request_timeout > Duration::from_secs(300)
            || self.retry_backoff > Duration::from_secs(30)
        {
            return Err(Error::InvalidConfig("RPC duration limits: interval <= 60s, timeout <= 300s, retry backoff <= 30s".into()));
        }
        Ok(())
    }
}

/// Bounded error classification used for retries or log-range subdivision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorKind {
    /// Rate failure eligible for bounded retry.
    RateLimited,
    /// Log range too wide; Engine subdivides it.
    RangeTooWide,
    /// Response too large; Engine attempts smaller log ranges.
    PayloadTooLarge,
    /// Temporary transport/server failure eligible for bounded retry.
    Transient,
    /// No automatic transport retry.
    Permanent,
}

/// Classifies known provider error codes and messages for retry handling.
pub fn classify_rpc_error(error: &Error) -> RpcErrorKind {
    match error {
        Error::Rpc { code, message } => {
            let message = message.to_ascii_lowercase();
            if *code == 429 || message.contains("rate limit") || message.contains("too many requests") {
                RpcErrorKind::RateLimited
            } else if *code == 413 || message.contains("response size") || message.contains("too many results")
                || message.contains("payload too large") || message.contains("more than") && message.contains("results")
            {
                RpcErrorKind::PayloadTooLarge
            } else if message.contains("block range") || message.contains("range too")
                || message.contains("range limit") || message.contains("limit the query")
                || *code == -32005
            {
                RpcErrorKind::RangeTooWide
            } else if matches!(*code, 408 | 500 | 502 | 503 | 504 | -32603)
                || message.contains("timeout") || message.contains("temporarily unavailable")
            {
                RpcErrorKind::Transient
            } else {
                RpcErrorKind::Permanent
            }
        }
        Error::ResourceLimit(message) if message.starts_with("response body exceeds")
            || message.starts_with("log payload exceeds") => RpcErrorKind::PayloadTooLarge,
        Error::Transport(_) => RpcErrorKind::Transient,
        _ => RpcErrorKind::Permanent,
    }
}

struct Inner {
    client: Client,
    urls: Vec<Url>,
    selected: Arc<AtomicUsize>,
    config: RpcConfig,
    request_id: AtomicU64,
    semaphore: Arc<Semaphore>,
    total_semaphore: Arc<Semaphore>,
    total_concurrency: usize,
    next_request: Arc<Mutex<Instant>>,
    completion_limit: Option<Arc<completion_limit::CompletionLimit>>,
}

/// Shared admission for one HTTP attempt. Hold it until the response is consumed.
/// Dropping a waiting future or this permit releases all acquired capacity.
/// External transports must acquire a new permit for each attempt, including retries.
#[derive(Debug)]
pub struct RpcRequestPermit {
    _lane: OwnedSemaphorePermit,
    _total: OwnedSemaphorePermit,
    _completion: Option<completion_limit::CompletionPermit>,
}

/// Read-only client. Clones share endpoint selection, rate limits and concurrency.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<Inner>,
    coalescing: Option<Arc<coalescing::PendingReads>>,
}

impl RpcClient {
    // Session identity includes endpoint selection; switching during a scope fails closed.
    pub(crate) fn session_key(&self) -> (usize, usize) {
        (Arc::as_ptr(&self.inner) as usize, self.inner.selected.load(Ordering::SeqCst))
    }

    /// Creates a client without network requests. HTTP(S) only; redirects and proxy env are disabled.
    pub fn new(url: &str, config: RpcConfig) -> Result<Self> {
        Self::with_endpoints(&[url.to_owned()], config)
    }

    /// Creates a client with explicitly selected endpoints. Validate identity and
    /// canonical anchors after switching; requests never switch endpoints automatically.
    pub fn with_endpoints(urls: &[String], config: RpcConfig) -> Result<Self> {
        config.validate()?;
        if urls.is_empty() || urls.len() > 8 {
            return Err(Error::InvalidConfig("RPC endpoint count must be 1..=8".into()));
        }
        let urls = urls.iter().map(|url| {
            let url = Url::parse(url).map_err(|_| Error::InvalidConfig("invalid RPC URL".into()))?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err(Error::InvalidConfig("RPC URL must use HTTP(S)".into()));
            }
            Ok(url)
        }).collect::<Result<Vec<_>>>()?;
        let client = Client::builder()
            .retry(reqwest::retry::never())
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build().map_err(transport_error)?;
        Ok(Self { coalescing: None, inner: Arc::new(Inner {
            client,
            urls,
            selected: Arc::new(AtomicUsize::new(0)),
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            total_semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            total_concurrency: config.max_concurrent_requests,
            completion_limit: None,
            config,
            request_id: AtomicU64::new(1),
            next_request: Arc::new(Mutex::new(Instant::now())),
        }) })
    }

    /// Adds a conservative per-second cap before creating clones or derived lanes.
    /// Counts in-flight requests plus requests completed in the last second.
    /// Throughput decreases with latency; delayed dispatch cannot release a burst
    /// of free slots. Replaces start-interval pacing; concurrency limits still apply.
    pub fn with_completion_rate_limit(mut self, limit: usize) -> Result<Self> {
        if !(1..=10_000).contains(&limit) || Arc::strong_count(&self.inner.next_request) != 1 {
            return Err(Error::InvalidConfig("completion limit must be 1..=10000 and configured before sharing the transport".into()));
        }
        let inner = Arc::get_mut(&mut self.inner)
            .ok_or_else(|| Error::InvalidConfig("configure completion limit before cloning the transport".into()))?;
        if inner.completion_limit.is_some() {
            return Err(Error::InvalidConfig("completion limit is already configured".into()));
        }
        inner.completion_limit = Some(completion_limit::CompletionLimit::new(limit));
        Ok(self)
    }

    /// Opt in to sharing queued header, chain-ID and code checks among clones.
    /// Dispatched/completed requests are never reused; ordinary calls are unaffected.
    pub fn with_pending_read_coalescing(mut self,enabled:bool) -> Self {
        if enabled {
            if self.coalescing.is_none() {self.coalescing=Some(Arc::default());}
        } else {self.coalescing=None;}
        self
    }

    /// Immutable transport budgets for this client.
    pub fn config(&self) -> &RpcConfig {
        &self.inner.config
    }

    /// Creates a separate lane sharing endpoint selection, total concurrency and rate.
    /// The original client's concurrency is the total cap. Derive both a smaller bulk
    /// lane and a control lane from it to reserve capacity for control requests.
    pub fn control_transport(&self, config: RpcConfig) -> Result<Self> {
        if config.min_request_interval != self.inner.config.min_request_interval {
            return Err(Error::InvalidConfig("control transport must share request interval".into()));
        }
        if config.max_concurrent_requests > self.inner.total_concurrency {
            return Err(Error::InvalidConfig("lane concurrency exceeds shared total cap".into()));
        }
        let urls=self.inner.urls.iter().map(Url::to_string).collect::<Vec<_>>();
        let mut client=Self::with_endpoints(&urls,config)?;
        let inner=Arc::get_mut(&mut client.inner).ok_or_else(||Error::Transport("new control transport unexpectedly shared".into()))?;
        inner.selected=self.inner.selected.clone();
        inner.next_request=self.inner.next_request.clone();
        inner.completion_limit=self.inner.completion_limit.clone();
        inner.total_semaphore=self.inner.total_semaphore.clone();
        inner.total_concurrency=self.inner.total_concurrency;
        if self.coalescing.is_some() {client.coalescing=Some(Arc::default());}
        Ok(client)
    }

    /// Compares the complete configured URL without exposing it.
    pub fn endpoint_matches(&self, candidate: &Url) -> bool {
        self.endpoint_at_matches(self.endpoint_index(), candidate)
    }

    /// Index of the selected endpoint.
    pub fn endpoint_index(&self) -> usize { self.inner.selected.load(Ordering::Acquire) }

    /// Number of configured endpoints.
    pub fn endpoint_count(&self) -> usize { self.inner.urls.len() }

    /// Compares a configured endpoint without exposing its URL.
    pub fn endpoint_at_matches(&self, index: usize, candidate: &Url) -> bool {
        self.inner.urls.get(index).is_some_and(|url| url == candidate)
    }

    /// Explicit consumer-coordinated switch. It does not certify identity or freshness.
    pub fn select_endpoint(&self, index: usize) -> Result<()> {
        if index >= self.inner.urls.len() { return Err(Error::InvalidConfig("RPC endpoint index out of range".into())); }
        self.inner.selected.store(index, Ordering::Release);
        Ok(())
    }

    /// Sends an allowlisted read-only request with array parameters and bounded retries.
    /// Use `HttpReader` for block-pinned reads.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        if let Some(shared)=&self.coalescing {
            if matches!(method,"eth_chainId"|"eth_getBlockByNumber"|"eth_getCode") {
                return shared.request(self,method,params).await;
            }
        }
        self.request_unshared(method,params,None).await
    }

    async fn request_unshared(&self, method:&str, params:Value, seal:Option<(&std::sync::atomic::AtomicBool,usize)>) -> Result<Value> {
        if !matches!(method, "eth_chainId" | "eth_getBlockByNumber" | "eth_getBlockByHash"
            | "eth_getCode" | "eth_call" | "eth_getLogs" | "eth_getTransactionCount")
        {
            return Err(Error::InvalidConfig(format!("RPC method is not read-only allowed: {method}")));
        }
        if !params.is_array() {
            return Err(Error::InvalidConfig("RPC params must be an array".into()));
        }
        for attempt in 0..=self.inner.config.max_retries {
            let result = {
                let _permit = self.acquire_request_permit().await?;
                if let Some((sealed,endpoint))=seal {
                    sealed.store(true,Ordering::Release);
                    if endpoint!=self.endpoint_index() {return Err(Error::IdentityMismatch("endpoint changed before shared dispatch".into()));}
                }
                self.request_once(method, &params).await
            };
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let retry = matches!(classify_rpc_error(&error), RpcErrorKind::RateLimited | RpcErrorKind::Transient);
                    if !retry || attempt == self.inner.config.max_retries {
                        return Err(error);
                    }
                    let delay = self.inner.config.retry_backoff
                        .checked_mul(1u32 << attempt).unwrap_or(Duration::from_secs(30))
                        .min(Duration::from_secs(30));
                    sleep(delay).await;
                }
            }
        }
        unreachable!("bounded request loop always returns")
    }

    /// Admits one request from an external HTTP transport under the same budgets.
    /// This does not send a request or extend the SDK's read-only method allowlist.
    /// Endpoint switches do not reset the budget. No future slots are reserved:
    /// cancellation while waiting cannot delay requests that come afterwards.
    pub async fn acquire_request_permit(&self) -> Result<RpcRequestPermit> {
        let lane = self.inner.semaphore.clone().acquire_owned().await
            .map_err(|_| Error::Transport("RPC lane limiter closed".into()))?;
        let total = self.inner.total_semaphore.clone().acquire_owned().await
            .map_err(|_| Error::Transport("RPC total concurrency limiter closed".into()))?;
        let completion = match &self.inner.completion_limit {
            Some(limit) => Some(limit.acquire().await?),
            None => {
                let mut next = self.inner.next_request.lock().await;
                sleep_until(*next).await;
                // No future reservations: cancellation cannot leave a hole.
                *next = Instant::now() + self.inner.config.min_request_interval;
                None
            },
        };
        Ok(RpcRequestPermit { _lane: lane, _total: total, _completion: completion })
    }

    async fn request_once(&self, method: &str, params: &Value) -> Result<Value> {
        let id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);
        let payload = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let url = self.inner.urls[self.endpoint_index()].clone();
        let mut response = self.inner.client.post(url).json(&payload)
            .send().await.map_err(transport_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Rpc { code: i64::from(status.as_u16()), message: format!("HTTP status {}", status.as_u16()) });
        }
        let max = self.inner.config.max_response_bytes;
        if response.content_length().is_some_and(|length| length > max as u64) {
            return Err(Error::ResourceLimit(format!("response body exceeds {max} byte limit")));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            if body.len().checked_add(chunk.len()).is_none_or(|length| length > max) {
                return Err(Error::ResourceLimit(format!("response body exceeds {max} byte limit")));
            }
            body.extend_from_slice(&chunk);
        }
        let envelope: Value = serde_json::from_slice(&body)
            .map_err(|error| Error::Decode(format!("invalid JSON-RPC response: {error}")))?;
        if envelope.get("jsonrpc") != Some(&json!("2.0")) || envelope.get("id") != Some(&json!(id)) {
            return Err(Error::Decode("JSON-RPC version or response id mismatch".into()));
        }
        match (envelope.get("result"), envelope.get("error")) {
            (Some(result), None) => Ok(result.clone()),
            (None, Some(error)) => {
                let code = error.get("code").and_then(Value::as_i64)
                    .ok_or_else(|| Error::Decode("RPC error missing integer code".into()))?;
                let message = error.get("message").and_then(Value::as_str)
                    .ok_or_else(|| Error::Decode("RPC error missing message".into()))?;
                Err(Error::Rpc { code, message: message.to_owned() })
            }
            _ => Err(Error::Decode("JSON-RPC response must contain exactly result or error".into())),
        }
    }
}

fn transport_error(error: reqwest::Error) -> Error {
    Error::Transport(error.without_url().to_string())
}

//! Block-pinned state reads and the HTTP JSON-RPC adapter.

use std::{collections::BTreeMap, sync::Arc};

mod session;
mod multicall;
mod optimizations;
pub use optimizations::ReadOptimizationConfig;
pub(crate) use session::verified_session;
pub use multicall::{Multicall3, Multicall3Config};

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, sol};
use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Error, Result, abi::{CreditPool, IPriceOracle, PROTOCOL_VERSION}, rpc::{BlockPinning, RpcClient, RpcConfig, RpcErrorKind, classify_rpc_error}, types::*};

sol! {
    interface TokenMetadata {
        function decimals() external view returns (uint8);
    }
}

/// State source for an engine.
///
/// Methods taking a `BlockRef` must read that exact block and return the complete
/// requested set or an error. The engine also verifies identity and canonicality.
#[async_trait]
pub trait StateReader: Send + Sync {
    /// Internal HTTP read coalescing capability. Custom readers keep their existing behavior.
    fn supports_verified_sessions(&self) -> bool { false }
    /// Source chain ID and genesis; must remain stable during synchronization.
    async fn identity(&self) -> Result<ChainIdentity>;
    /// Head for the selected commitment. An unsupported tag is an error.
    async fn head(&self, commitment: Commitment) -> Result<BlockRef>;
    /// Numbered header or None if unavailable. Commitment is metadata here;
    /// Engine checks that the target is no newer than head(tag).
    async fn block(&self, number: u64, commitment: Commitment) -> Result<Option<BlockRef>>;
    /// Verifies strictly increasing (height, hash) pairs from normalized logs.
    /// The default checks each distinct numbered header. HTTP readers may prove
    /// ancestry to a freshly checked canonical header instead; no TTL trust is used.
    async fn verify_log_blocks(&self, blocks: &[(u64, B256)], commitment: Commitment) -> Result<()> {
        validate_log_blocks(blocks)?;
        for &(number, hash) in blocks {
            let header = self.block(number, commitment).await?.ok_or(Error::MissingBlock(number))?;
            if header.hash != hash { return Err(Error::NonCanonical(number)); }
        }
        Ok(())
    }
    /// Version, runtime-code hash, token decimals and oracle at the target.
    async fn describe_pool(&self, spec: &PoolSpec, at: &BlockRef) -> Result<PoolDescriptor>;
    /// Debt/collateral getters for every requested key at the target; no partial map.
    async fn read_positions(&self, pool: Address, keys: &[PositionKey], at: &BlockRef) -> Result<BTreeMap<PositionKey, PositionAmounts>>;
    /// Global liquidation settings at the target.
    async fn read_globals(&self, pool: Address, at: &BlockRef) -> Result<PoolRiskConfig>;
    /// Effective settings for all requested markets at the target.
    async fn read_market_configs(&self, pool: Address, markets: &[u64], at: &BlockRef) -> Result<BTreeMap<u64, MarketRiskConfig>>;
    /// Oracle price/status independently of amounts, with collateral-to-asset pairing.
    async fn read_oracle(&self, pool: &PoolDescriptor, at: &BlockRef) -> Result<OracleStatus>;
    /// Complete inclusive historical logs for the pool over `[from, to]`.
    /// Range/payload failures must be errors, not partial successful responses.
    async fn logs(&self, pool: Address, from: u64, to: u64) -> Result<Vec<RawLog>>;
    /// Page of the complete market user list at the target, including old debt-zero users.
    async fn market_users(&self, pool: Address, market: u64, start: u64, limit: u64, at: &BlockRef) -> Result<Vec<Address>>;
    /// Complete pages in input order at one anchor. The default preserves support
    /// for custom readers; HTTP combines up to64 views in native pool multicall.
    async fn market_user_pages(&self, pool: Address, pages: &[MarketUserPage], at: &BlockRef) -> Result<Vec<Vec<Address>>> {
        if pages.is_empty() || pages.len() > 64 || pages.iter().any(|p| p.limit == 0) {
            return Err(Error::InvalidConfig("user page bundle size/limit".into()));
        }
        let mut values = Vec::with_capacity(pages.len());
        for page in pages { values.push(self.market_users(pool, page.market, page.start, page.limit, at).await?); }
        Ok(values)
    }
    /// Explorer active-market list at the target; not a replacement for arbitrary-market history.
    async fn active_markets(&self, pool: Address, at: &BlockRef) -> Result<Vec<u64>>;
    /// Immutable v5 market schedule. Optional capability for current-state census;
    /// existing readers and the history-based engine do not need to implement it.
    async fn market_schedule(&self, _pool: Address, _at: &BlockRef) -> Result<crate::engine::current::MarketSchedule> {
        Err(Error::InvalidConfig("reader does not support current market census".into()))
    }
}

/// One page of a market's persistent participant registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketUserPage {
    /// Maturity timestamp, including matured markets.
    pub market: u64,
    /// Zero-based registry offset.
    pub start: u64,
    /// Maximum accounts; must be positive.
    pub limit: u64,
}

/// Supported call syntax and commitment tags. Does not test historical retention
/// or change the configured pinning mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcCapabilities {
    /// Successful EIP1898 blockHash call plus rejection of an absent sentinel hash.
    pub eip1898: bool,
    /// Successful raw-hash block parameter plus a negative absent-hash probe.
    pub raw_hash: bool,
    /// Working numbered call; Reader checks canonical headers separately.
    pub number_bracket: bool,
    /// Endpoint responds to the finalized tag.
    pub finalized: bool,
    /// Endpoint responds to the safe tag.
    pub safe: bool,
}

/// Read-only StateReader backed by a bounded RpcClient; clones share transport limits.
#[derive(Clone)]
pub struct HttpReader {
    rpc: RpcClient,
    history: Arc<tokio::sync::Mutex<HistoryHeaders>>,
    multicall: Option<Multicall3Config>,
    optimizations: ReadOptimizationConfig,
}

// Immutable header content is shared across pools and retries, never canonicality.
// A cache hit is usable only while walking parent hashes from a fresh anchor.
// Sparse or long log ranges retain direct numbered-header checks.
const HISTORY_HEADER_CAPACITY: usize = 2048;
const HISTORY_WALK_LIMIT: u64 = 512;

#[derive(Default)]
struct HistoryHeaders(BTreeMap<(u64, B256), BlockRef>);

impl HistoryHeaders {
    fn insert(&mut self, header: BlockRef) -> Result<()> {
        let key = (header.number, header.hash);
        if self.0.get(&key).is_some_and(|old| old.parent_hash != header.parent_hash || old.timestamp != header.timestamp) {
            return Err(Error::Decode("same header hash has inconsistent content".into()));
        }
        self.0.insert(key, header);
        while self.0.len() > HISTORY_HEADER_CAPACITY { self.0.pop_first(); }
        Ok(())
    }
}

fn validate_log_blocks(blocks: &[(u64, B256)]) -> Result<()> {
    if blocks.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(Error::InvalidConfig("log block witnesses must be strictly increasing".into()));
    }
    Ok(())
}

/// One read in a bounded group sharing a canonical block check.
#[derive(Clone, Debug)]
pub enum PinnedRead {
    /// ABI-encoded call; does not persist changes.
    Call { to: Address, data: Bytes },
    /// Runtime bytecode at the anchor.
    Code { address: Address },
}

impl PinnedRead {
    /// Encodes a typed contract call.
    pub fn call<C: SolCall>(to: Address, call: &C) -> Self {
        Self::Call { to, data: Bytes::from(call.abi_encode()) }
    }
}

impl HttpReader {
    /// Creates an HTTP(S) reader and validates transport settings.
    pub fn new(url: &str, config: RpcConfig) -> Result<Self> {
        Ok(Self { rpc: RpcClient::new(url, config)?, history: Arc::default(), multicall: None, optimizations: ReadOptimizationConfig::default() })
    }

    /// Shared explicit endpoint selection; consumers validate identity and freshness
    /// before admitting data after a switch. All block pinning remains unchanged.
    pub fn with_endpoints(urls: &[String], config: RpcConfig) -> Result<Self> {
        Ok(Self { rpc: RpcClient::with_endpoints(urls, config)?, history: Arc::default(), multicall: None, optimizations: ReadOptimizationConfig::default() })
    }

    /// Enables an optional conservative request window before cloning this reader.
    /// See [`RpcClient::with_completion_rate_limit`]. All derived readers share it.
    pub fn with_completion_rate_limit(mut self, limit: usize) -> Result<Self> {
        self.rpc = self.rpc.with_completion_rate_limit(limit)?;
        Ok(self)
    }

    /// Explicit control/recovery reader sharing endpoint selection and request rate,
    /// with separate concurrency permits for control requests.
    pub fn control_reader(&self, config: RpcConfig) -> Result<Self> {
        Ok(Self { rpc:self.rpc.control_transport(config)?, history: self.history.clone(), multicall: self.multicall.clone(), optimizations: self.optimizations.clone() })
    }

    /// Read-only transport for additional allowlisted requests; no signing/submission API.
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc
    }

    /// Compares the complete configured URL without exposing it.
    pub fn endpoint_matches(&self, candidate: &reqwest::Url) -> bool {
        self.rpc.endpoint_matches(candidate)
    }

    /// ABI call at an explicit canonical anchor with validated return decoding.
    ///
    /// Use `crate::abi::CreditPool` for position and orderbook getters.
    /// Any `SolCall` can be simulated through `eth_call`; it does not persist changes.
    pub async fn contract_call<C: SolCall + Sync>(&self, address: Address, call: &C, at: &BlockRef) -> Result<C::Return> {
        let data = Bytes::from(call.abi_encode());
        let result = self.pinned_request("eth_call", json!({ "to": address, "data": data }), at).await?;
        let bytes: Bytes = decode_json(result, "eth_call bytes")?;
        decode_call::<C>(&bytes)
    }

    /// Runtime code at an explicit anchor, checked for canonicality before and after RPC.
    pub async fn get_code(&self, address: Address, at: &BlockRef) -> Result<Bytes> {
        let result = self.pinned_request("eth_getCode", json!(address), at).await?;
        decode_json(result, "eth_getCode bytes")
    }

    /// Reads up to 64 values at one explicit block, in input order.
    ///
    /// The group has one canonical header check before and after its reads.
    /// Every individual request retains the configured pinning, rate, timeout
    /// and concurrency limits. No partial result is returned on failure.
    pub async fn read_bundle(&self, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        if reads.len() > 64 {
            return Err(Error::ResourceLimit("pinned read bundle exceeds 64 reads".into()));
        }
        self.ensure_canonical(at).await?;
        let values = self.read_bundle_unchecked(reads, at).await;
        // Check even on a read error so a reorg cannot masquerade as a revert.
        self.ensure_canonical(at).await?;
        values
    }

    // Only call inside one surrounding canonical bracket. Dependencies may be
    // validated between groups without repeating the same numbered header.
    async fn read_bundle_unchecked(&self, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        stream::iter(reads.to_vec().into_iter().map(|read| async move {
            let (method, first) = match read {
                PinnedRead::Call { to, data } => ("eth_call", json!({ "to": to, "data": data })),
                PinnedRead::Code { address } => ("eth_getCode", json!(address)),
            };
            decode_json(self.raw_pinned_request(method, first, at).await?, "pinned read bytes")
        })).buffered(self.rpc.config().max_concurrent_requests).try_collect::<Vec<Bytes>>().await
    }

    // Only fixed, account-explicit view getters are admitted here. The pool's
    // native multicall uses self-delegatecall and transient price-cache writes;
    // eth_call does not persist either. Arbitrary simulated mutations could alter
    // later reads, so this is deliberately private and selector/shape restricted.
    // No aggregate contract, sender override or fallback. Enlarged position batches
    // use the explicitly configured simulated gas/calldata caps.
    async fn pool_read_bundle(&self, pool: Address, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        if reads.is_empty() || reads.len() > 64.max(2*self.optimizations.position_batch_size) {
            return Err(Error::ResourceLimit("pool read bundle exceeds getter cap".into()));
        }
        let mut data = Vec::with_capacity(reads.len());
        for read in reads {
            let PinnedRead::Call { to, data: call } = read else {
                return Err(Error::InvalidConfig("pool read bundle accepts view calls only".into()));
            };
            let valid = *to == pool && (
                call.len() == 68 && [CreditPool::debtOfCall::SELECTOR, CreditPool::collateralOfCall::SELECTOR]
                    .iter().any(|selector| call.starts_with(selector))
                || *to == pool && call.len() == 4 && [CreditPool::getLiquidationLTVCall::SELECTOR,
                    CreditPool::getStalePriceWindowCall::SELECTOR, CreditPool::getMinDebtRemainingAfterLiquidationCall::SELECTOR]
                    .iter().any(|selector| call.starts_with(selector))
                || *to == pool && call.len() == 36 && [CreditPool::getLiquidationGracePeriodCall::SELECTOR,
                    CreditPool::getLiquidationDiscountMinCall::SELECTOR, CreditPool::getLiquidationDiscountMaxCall::SELECTOR]
                    .iter().any(|selector| call.starts_with(selector)));
            if !valid { return Err(Error::InvalidConfig("unsupported getter in pool read bundle".into())); }
            data.push(call.clone());
        }
        let call = CreditPool::multicallCall { data };
        let encoded=Bytes::from(call.abi_encode());
        let mut request=json!({"to":pool,"data":encoded});
        if self.optimizations.position_batch_size!=32 {
            if encoded.len()>self.optimizations.position_batch_calldata_limit {return Err(Error::ResourceLimit("position calldata limit".into()));}
            request["gas"]=json!(quantity(self.optimizations.position_batch_gas_limit));
        }
        let result = self.pinned_request("eth_call",request,at).await?;
        let bytes: Bytes = decode_json(result, "pool multicall bytes")?;
        let values = decode_call::<CreditPool::multicallCall>(&bytes)?;
        if values.len() != reads.len() || values.iter().any(|value| value.len() != 32)
            || CreditPool::multicallCall::abi_encode_returns(&values).as_slice() != bytes.as_ref()
        {
            return Err(Error::Decode("pool multicall returned noncanonical or incomplete values".into()));
        }
        Ok(values)
    }

    /// Read up to 64 allowlisted, account-explicit CreditPool v5 view calls in
    /// one native pool multicall. The entire result is bracketed by canonical
    /// header checks, including on RPC errors. This does not change StateEngine
    /// refresh policy or cache any mutable state. All requests use the existing
    /// shared RPC budget and response limits.
    ///
    /// Arbitrary calls, mutations, foreign targets and nested multicalls are
    /// rejected before RPC. Quotes are borrower-specific; a successful preview
    /// is not an execution/simulation guarantee. Use `read_bundle` for other
    /// contracts or unsupported views.
    pub async fn read_pool_views(&self, pool: Address, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        if pool.is_zero() || reads.is_empty() || reads.len() > 64 {
            return Err(Error::InvalidConfig("pool views require a nonzero pool and 1..=64 calls".into()));
        }
        let mut data = Vec::with_capacity(reads.len());
        for read in reads {
            let PinnedRead::Call { to, data: call } = read else {
                return Err(Error::InvalidConfig("pool views accept calls only".into()));
            };
            if *to != pool { return Err(Error::InvalidConfig("foreign pool view target".into())); }
            validate_pool_view(call, None)?;
            data.push(call.clone());
        }
        let call = CreditPool::multicallCall { data };
        let value = self.pinned_request("eth_call", json!({"to":pool,"data":Bytes::from(call.abi_encode())}), at).await?;
        let bytes: Bytes = decode_json(value, "pool views bytes")?;
        let values = decode_call::<CreditPool::multicallCall>(&bytes)?;
        if values.len() != reads.len() || CreditPool::multicallCall::abi_encode_returns(&values).as_slice() != bytes.as_ref() {
            return Err(Error::Decode("noncanonical or incomplete pool views".into()));
        }
        for (read, value) in reads.iter().zip(&values) {
            let PinnedRead::Call { data, .. } = read else { unreachable!() };
            validate_pool_view(data, Some(value))?;
        }
        Ok(values)
    }

    async fn uncached_block(&self, number: u64, commitment: Commitment) -> Result<Option<BlockRef>> {
        let value = self.rpc.request("eth_getBlockByNumber", json!([quantity(number), false])).await?;
        if value.is_null() {
            return Ok(None);
        }
        let block = decode_block(value, commitment)?;
        if block.number != number {
            return Err(Error::Decode("RPC returned a different block number".into()));
        }
        Ok(Some(block))
    }


    /// Bounded address/topic-filtered logs. Returns the complete requested range or
    /// an error; callers must verify block hashes and canonical anchors before use.
    pub async fn filtered_logs(&self, pool: Address, from: u64, to: u64, topics: &[Option<B256>]) -> Result<Vec<RawLog>> {
        if topics.len() > 4 { return Err(Error::InvalidConfig("at most four log topics".into())); }
        let count = to.checked_sub(from).and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| Error::InvalidConfig("invalid log block range".into()))?;
        if count > self.rpc.config().max_log_range {
            return Err(Error::Rpc { code: -32005, message: "configured block range limit exceeded".into() });
        }
        let mut filter=json!({"address":pool,"fromBlock":quantity(from),"toBlock":quantity(to)});
        // Keep the existing address-only wire shape for public SDK consumers.
        if !topics.is_empty() { filter["topics"]=json!(topics); }
        let value = self.rpc.request("eth_getLogs", json!([filter])).await?;
        let values = value.as_array().ok_or_else(|| Error::Decode("eth_getLogs result must be an array".into()))?;
        if values.len() > self.rpc.config().max_logs {
            return Err(Error::ResourceLimit("log payload exceeds configured count limit".into()));
        }
        let mut logs = Vec::with_capacity(values.len());
        for value in values {
            let wire: WireLog = decode_json(value.clone(), "historical log")?;
            let block_number = parse_quantity(&wire.block_number, "log blockNumber")?;
            if wire.address != pool || block_number < from || block_number > to || wire.topics.len() > 4
                || wire.topics.len() < topics.len()
                || topics.iter().enumerate().any(|(i,t)| t.is_some_and(|t| wire.topics.get(i) != Some(&t))) {
                return Err(Error::Decode("log address/range/topics do not match request".into()));
            }
            logs.push(RawLog {
                address: wire.address,
                topics: wire.topics,
                data: wire.data,
                block_number,
                block_hash: wire.block_hash,
                transaction_hash: wire.transaction_hash,
                log_index: parse_quantity(&wire.log_index, "log logIndex")?,
                removed: wire.removed,
            });
        }
        Ok(logs)
    }


    /// Rechecks height/hash/parent/time against the current numbered header.
    /// Hash-pinned calls still require canonicality; this does not check age against head.
    pub async fn ensure_canonical(&self, at: &BlockRef) -> Result<()> {
        let current = self.block(at.number, at.commitment).await?
            .ok_or(Error::MissingBlock(at.number))?;
        if current.hash != at.hash || current.parent_hash != at.parent_hash || current.timestamp != at.timestamp {
            return Err(Error::NonCanonical(at.number));
        }
        Ok(())
    }

    async fn pinned_request(&self, method: &str, first_param: Value, at: &BlockRef) -> Result<Value> {
        self.ensure_canonical(at).await?;
        let value = self.raw_pinned_request(method, first_param, at).await;
        // Do not disguise a canonicality race as a revert/decode error: the header check must pass.
        self.ensure_canonical(at).await?;
        value
    }

    // The caller owns canonical checks; pinning, limits and pruning translation
    // remain identical for every individual request, including dependent reads.
    async fn raw_pinned_request(&self, method: &str, first_param: Value, at: &BlockRef) -> Result<Value> {
        let pin = block_parameter(self.rpc.config().pinning, at);
        match self.rpc.request(method, json!([first_param, pin])).await {
            Err(Error::Rpc { code, message }) if code != 3 && (pruned_state_message(&message) || crate::rpc::is_missing_block_error(code, &message)) => Err(Error::MissingBlock(at.number)),
            value => value,
        }
    }

    /// Probes version() on a real pool, absent-hash negative calls and commitment tags.
    /// Rate/transport errors are not disguised as unsupported capabilities. Pinning stays fixed.
    pub async fn probe_capabilities(&self, pool: Address, at: &BlockRef) -> Result<RpcCapabilities> {
        self.ensure_canonical(at).await?;
        let first = json!({ "to": pool, "data": Bytes::from(CreditPool::versionCall {}.abi_encode()) });
        let number_result = self.rpc.request("eth_call", json!([first, quantity(at.number)])).await?;
        let bytes: Bytes = decode_json(number_result.clone(), "probe version bytes")?;
        decode_call::<CreditPool::versionCall>(&bytes)?;
        let mut result = RpcCapabilities { eip1898: false, raw_hash: false, number_bracket: true, finalized: false, safe: false };
        let mut invalid_hash = Vec::from(b"barterlend-state invalid hash capability probe".as_slice());
        invalid_hash.extend_from_slice(at.hash.as_slice());
        let invalid_hash = keccak256(invalid_hash);
        let missing = self.rpc.request("eth_getBlockByHash", json!([invalid_hash, false])).await?;
        if !missing.is_null() {
            return Err(Error::Decode("capability probe expected an absent sentinel block hash".into()));
        }
        for mode in [BlockPinning::Eip1898, BlockPinning::RawHash] {
            match self.rpc.request("eth_call", json!([first, block_parameter(mode, at)])).await {
                Ok(value) if value == number_result => {
                    let mut sentinel = *at;
                    sentinel.hash = invalid_hash;
                    // An RPC that ignores the hash parameter must fail the negative probe.
                    match self.rpc.request("eth_call", json!([first, block_parameter(mode, &sentinel)])).await {
                        Err(Error::Rpc { code, message }) if missing_hash_message(code, &message) => match mode {
                            BlockPinning::Eip1898 => result.eip1898 = true,
                            BlockPinning::RawHash => result.raw_hash = true,
                            BlockPinning::NumberBracket => {}
                        },
                        Err(error @ Error::Rpc { .. }) => { capability_rpc_error(error)?; }
                        Ok(_) => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(_) => return Err(Error::Decode("pinning probe returned inconsistent version() results".into())),
                Err(error @ Error::Rpc { .. }) => { capability_rpc_error(error)?; }
                Err(error) => return Err(error),
            }
        }
        // An unsupported commitment tag does not permit a latest fallback.
        result.finalized = tag_supported(self.head(Commitment::Finalized).await)?;
        result.safe = tag_supported(self.head(Commitment::Safe).await)?;
        self.ensure_canonical(at).await?;
        Ok(result)
    }

    async fn price(&self, pool: &PoolDescriptor, at: &BlockRef) -> Result<OracleStatus> {
        self.ensure_canonical(at).await?;
        let result = async {
            let pair = self.view_bundle_unchecked(&[
                PinnedRead::call(pool.oracle, &IPriceOracle::sourceCall {}),
                PinnedRead::call(pool.oracle, &IPriceOracle::targetCall {}),
            ], at).await?;
            let source = decode_call::<IPriceOracle::sourceCall>(&pair[0])?;
            let target = decode_call::<IPriceOracle::targetCall>(&pair[1])?;
            if source != pool.collateral.address || target != pool.asset.address {
                return Ok(OracleStatus::Unavailable(OracleFailure::PairMismatch {
                    expected_source: pool.collateral.address,
                    expected_target: pool.asset.address,
                    source,
                    target,
                }));
            }
            // Never call a mismatched oracle's potentially expensive price path.
            let values = self.view_bundle_unchecked(&[
                PinnedRead::call(pool.oracle, &IPriceOracle::getPriceCall {}),
            ], at).await?;
            let price = decode_call::<IPriceOracle::getPriceCall>(&values[0])?;
            Ok(OracleStatus::Ready(Price {
                numerator: price.numerator,
                denominator: price.denominator,
                updated_at: price.updatedAt,
            }))
        }.await;
        // This also dominates pair/decode/revert/pruning failures.
        self.ensure_canonical(at).await?;
        result
    }

}

#[async_trait]
impl StateReader for HttpReader {
    fn supports_verified_sessions(&self) -> bool { true }
    async fn market_user_pages(&self, pool: Address, pages: &[MarketUserPage], at: &BlockRef) -> Result<Vec<Vec<Address>>> {
        if pages.is_empty() || pages.len() > 64 || pages.iter().any(|p| p.limit == 0) {
            return Err(Error::InvalidConfig("user page bundle size/limit".into()));
        }
        let reads: Vec<_> = pages.iter().map(|page| PinnedRead::call(pool, &CreditPool::getMarketUsersCall {
            market: page.market, start: U256::from(page.start), limit: U256::from(page.limit) })).collect();
        let values = self.read_pool_views(pool, &reads, at).await?;
        values.iter().zip(pages).map(|(value, page)| {
            let users = CreditPool::getMarketUsersCall::abi_decode_returns(value).map_err(|e| Error::Decode(e.to_string()))?;
            if users.len() as u64 > page.limit || CreditPool::getMarketUsersCall::abi_encode_returns(&users).as_slice() != value.as_ref() {
                return Err(Error::Decode("noncanonical or oversized market user page".into()));
            }
            Ok(users)
        }).collect()
    }
    async fn market_schedule(&self, pool: Address, at: &BlockRef) -> Result<crate::engine::current::MarketSchedule> {
        let value = self.contract_call(pool, &CreditPool::getMarketsConfigCall {}, at).await?;
        Ok(crate::engine::current::MarketSchedule { start: value.start, durations: value.durations,
            active: value.active, extensions: value.extensions })
    }
    async fn identity(&self) -> Result<ChainIdentity> {
        if let Some(identity) = session::identity(self) { return Ok(identity); }
        let value = self.rpc.request("eth_chainId", json!([])).await?;
        let chain_id = parse_quantity_value(&value, "chainId")?;
        let genesis = self.block(0, Commitment::Finalized).await?.ok_or(Error::MissingBlock(0))?;
        let identity = ChainIdentity { chain_id, genesis_hash: genesis.hash };
        session::remember_identity(self, identity);
        Ok(identity)
    }

    async fn head(&self, commitment: Commitment) -> Result<BlockRef> {
        let tag = match commitment { Commitment::Finalized => "finalized", Commitment::Safe => "safe", Commitment::Latest => "latest" };
        let value = self.rpc.request("eth_getBlockByNumber", json!([tag, false])).await?;
        if value.is_null() {
            return Err(Error::Decode(format!("{tag} block is unavailable")));
        }
        let block = decode_block(value, commitment)?;
        session::remember(self, block)?;
        Ok(block)
    }

    async fn block(&self, number: u64, commitment: Commitment) -> Result<Option<BlockRef>> {
        if let Some(mut header) = session::cached(self, number) {
            header.commitment = commitment;
            return Ok(Some(header));
        }
        let result = self.uncached_block(number, commitment).await?;
        if let Some(header) = result { session::remember(self, header)?; }
        Ok(result)
    }

    async fn verify_log_blocks(&self, blocks: &[(u64, B256)], commitment: Commitment) -> Result<()> {
        validate_log_blocks(blocks)?;
        if self.optimizations.batch_log_hashes && blocks.len()>3 {return self.verify_recent_log_blocks(blocks,commitment).await;}
        let Some(&(last, expected_hash)) = blocks.last() else { return Ok(()); };
        let first = blocks[0].0;
        let distance = last - first;
        if blocks.len() < 2 || distance >= HISTORY_WALK_LIMIT
            || u128::from(distance) + 1 > blocks.len() as u128
        {
            // Sparse events do not justify downloading the intervening history.
            // Every witness still receives its own fresh canonical numbered read.
            stream::iter(blocks.to_vec().into_iter().map(|(number, hash)| async move {
                let header = self.uncached_block(number, commitment).await?.ok_or(Error::MissingBlock(number))?;
                if header.hash != hash { return Err(Error::NonCanonical(number)); }
                Ok(())
            })).buffer_unordered(self.rpc.config().max_concurrent_requests).try_collect::<Vec<_>>().await?;
            return Ok(());
        }
        let anchor = self.block(last, commitment).await?.ok_or(Error::MissingBlock(last))?;
        if anchor.hash != expected_hash { return Err(Error::NonCanonical(last)); }
        {
            // Serialize cache misses across pool engines. Every fetched header is
            // retained before the next await, including when a step is cancelled.
            // This lock never guards general block reads, calls, or control traffic.
            let mut cache = self.history.lock().await;
            cache.insert(anchor)?;
            let mut current = anchor;
            let mut expected = blocks.len() - 1;
            // The retained cache can evict old heights while a lagging pool walks
            // them. Keep this bounded fetch window locally as well, never a full
            // second history cache. Only parent-linked content may be consumed.
            let mut prefetched = BTreeMap::new();
            loop {
                if current.number == blocks[expected].0 {
                    if current.hash != blocks[expected].1 { return Err(Error::NonCanonical(current.number)); }
                    if expected == 0 { break; }
                    expected -= 1;
                }
                let number = current.number - 1;
                let parent = if let Some(header) = cache.0.get(&(number, current.parent_hash))
                    .or_else(|| prefetched.get(&(number, current.parent_hash)))
                {
                    *header
                } else {
                    // Fetch only a bounded window of missing numbered headers.
                    // Awaiting each parent separately would serialize all pools
                    // behind one network RTT despite an available shared quota.
                    let concurrency = self.rpc.config().max_concurrent_requests;
                    let lower = number.saturating_sub((concurrency - 1) as u64).max(first);
                    let missing: Vec<_> = (lower..=number).rev().filter(|&height| height == number
                        || cache.0.range((height, B256::ZERO)..=(height, B256::repeat_byte(0xff))).next().is_none()).collect();
                    prefetched.clear();
                    let mut reads = stream::iter(missing.into_iter().map(|height| async move {
                        self.uncached_block(height, commitment).await?.ok_or(Error::MissingBlock(height))
                    })).buffer_unordered(concurrency);
                    while let Some(result) = reads.next().await {
                        let header = result?;
                        cache.insert(header)?;
                        prefetched.insert((header.number, header.hash), header);
                    }
                    *prefetched.get(&(number, current.parent_hash)).ok_or(Error::NonCanonical(number))?
                };
                if parent.timestamp > current.timestamp { return Err(Error::Decode("parent header time exceeds child".into())); }
                current = parent;
            }
        }
        // Cached orphan headers are harmless: the complete chain must terminate
        // at this freshly rechecked canonical anchor. Reorgs during the walk fail.
        self.ensure_canonical(&anchor).await
    }

    async fn describe_pool(&self, spec: &PoolSpec, at: &BlockRef) -> Result<PoolDescriptor> {
        if spec.expected_version != PROTOCOL_VERSION {
            return Err(Error::UnsupportedVersion(spec.expected_version));
        }
        if spec.address.is_zero() || at.number < spec.deployment_block {
            return Err(Error::InvalidConfig("pool address/deployment block is invalid".into()));
        }
        if let Some(hash) = spec.deployment_block_hash {
            let deployment = self.block(spec.deployment_block, at.commitment).await?
                .ok_or(Error::MissingBlock(spec.deployment_block))?;
            if deployment.hash != hash {
                return Err(Error::IdentityMismatch("deployment block hash differs".into()));
            }
        }
        self.ensure_canonical(at).await?;
        let result = async {
            let values = self.view_bundle_unchecked(&[
                PinnedRead::Code { address: spec.address },
                PinnedRead::call(spec.address, &CreditPool::versionCall {}),
                PinnedRead::call(spec.address, &CreditPool::assetCall {}),
                PinnedRead::call(spec.address, &CreditPool::collateralCall {}),
                PinnedRead::call(spec.address, &CreditPool::priceOracleCall {}),
            ], at).await?;
            let code = &values[0];
            if code.is_empty() {
                return Err(Error::IdentityMismatch("pool has no code at target block".into()));
            }
            let version: u64 = decode_call::<CreditPool::versionCall>(&values[1])?
                .try_into().map_err(|_| Error::Decode("pool version exceeds u64".into()))?;
            if version != spec.expected_version {
                return Err(Error::UnsupportedVersion(version));
            }
            let asset = decode_call::<CreditPool::assetCall>(&values[2])?;
            let collateral = decode_call::<CreditPool::collateralCall>(&values[3])?;
            let oracle = decode_call::<CreditPool::priceOracleCall>(&values[4])?;
            if asset.is_zero() || collateral.is_zero() {
                return Err(Error::InvalidConfig("zero/native token address is unsupported".into()));
            }
            let decimals = self.view_bundle_unchecked(&[
                PinnedRead::call(asset, &TokenMetadata::decimalsCall {}),
                PinnedRead::call(collateral, &TokenMetadata::decimalsCall {}),
            ], at).await?;
            let asset_decimals = decode_call::<TokenMetadata::decimalsCall>(&decimals[0])?;
            let collateral_decimals = decode_call::<TokenMetadata::decimalsCall>(&decimals[1])?;
            Ok(PoolDescriptor {
                address: spec.address,
                version,
                asset: TokenDescriptor { address: asset, decimals: asset_decimals },
                collateral: TokenDescriptor { address: collateral, decimals: collateral_decimals },
                oracle,
                code_hash: keccak256(&code),
            })
        }.await;
        self.ensure_canonical(at).await?;
        result
    }

    async fn read_positions(&self, pool: Address, keys: &[PositionKey], at: &BlockRef) -> Result<BTreeMap<PositionKey, PositionAmounts>> {
        if keys.iter().any(|key| key.pool != pool) {
            return Err(Error::InvalidConfig("position key belongs to another pool".into()));
        }
        let mut amounts = BTreeMap::new();
        for chunk in keys.chunks(self.optimizations.position_batch_size) {
            let calls: Vec<_> = chunk.iter().flat_map(|key| [
                PinnedRead::call(pool, &CreditPool::debtOfCall { market: key.market, account: key.borrower }),
                PinnedRead::call(pool, &CreditPool::collateralOfCall { market: key.market, account: key.borrower }),
            ]).collect();
            let values = self.pool_read_bundle(pool, &calls, at).await?;
            for (key, pair) in chunk.iter().zip(values.chunks_exact(2)) {
                amounts.insert(*key, PositionAmounts {
                    debt: decode_call::<CreditPool::debtOfCall>(&pair[0])?,
                    collateral: decode_call::<CreditPool::collateralOfCall>(&pair[1])?,
                });
            }
        }
        // The last nonempty bundle already performed the final header check.
        if keys.is_empty() { self.ensure_canonical(at).await?; }
        Ok(amounts)
    }

    async fn read_globals(&self, pool: Address, at: &BlockRef) -> Result<PoolRiskConfig> {
        let values = self.pool_read_bundle(pool, &[
            PinnedRead::call(pool, &CreditPool::getLiquidationLTVCall {}),
            PinnedRead::call(pool, &CreditPool::getStalePriceWindowCall {}),
            PinnedRead::call(pool, &CreditPool::getMinDebtRemainingAfterLiquidationCall {}),
        ], at).await?;
        Ok(PoolRiskConfig {
            liquidation_ltv: decode_call::<CreditPool::getLiquidationLTVCall>(&values[0])?,
            stale_window: decode_call::<CreditPool::getStalePriceWindowCall>(&values[1])?,
            min_debt_remaining: decode_call::<CreditPool::getMinDebtRemainingAfterLiquidationCall>(&values[2])?,
        })
    }

    async fn read_market_configs(&self, pool: Address, markets: &[u64], at: &BlockRef) -> Result<BTreeMap<u64, MarketRiskConfig>> {
        let mut configs = BTreeMap::new();
        for chunk in markets.chunks(21) {
            let calls: Vec<_> = chunk.iter().flat_map(|&market| [
                PinnedRead::call(pool, &CreditPool::getLiquidationGracePeriodCall { market }),
                PinnedRead::call(pool, &CreditPool::getLiquidationDiscountMinCall { market }),
                PinnedRead::call(pool, &CreditPool::getLiquidationDiscountMaxCall { market }),
            ]).collect();
            let values = self.pool_read_bundle(pool, &calls, at).await?;
            for (market, triple) in chunk.iter().zip(values.chunks_exact(3)) {
                configs.insert(*market, MarketRiskConfig {
                    grace_period: decode_call::<CreditPool::getLiquidationGracePeriodCall>(&triple[0])?,
                    discount_min: decode_call::<CreditPool::getLiquidationDiscountMinCall>(&triple[1])?,
                    discount_max: decode_call::<CreditPool::getLiquidationDiscountMaxCall>(&triple[2])?,
                });
            }
        }
        if markets.is_empty() { self.ensure_canonical(at).await?; }
        Ok(configs)
    }

    async fn read_oracle(&self, pool: &PoolDescriptor, at: &BlockRef) -> Result<OracleStatus> {
        match self.price(pool, at).await {
            Ok(status) => Ok(status),
            Err(Error::Rpc { code, message }) => Ok(OracleStatus::Unavailable(OracleFailure::Rpc(format!("RPC {code}: {message}")))),
            Err(Error::Transport(message)) => Ok(OracleStatus::Unavailable(OracleFailure::Rpc(message))),
            Err(Error::Decode(message)) => Ok(OracleStatus::Unavailable(OracleFailure::Decode(message))),
            Err(Error::ResourceLimit(message)) => Ok(OracleStatus::Unavailable(OracleFailure::Rpc(message))),
            Err(error) => Err(error),
        }
    }

    async fn logs(&self, pool: Address, from: u64, to: u64) -> Result<Vec<RawLog>> {
        self.filtered_logs(pool, from, to, &[]).await
    }

    async fn market_users(&self, pool: Address, market: u64, start: u64, limit: u64, at: &BlockRef) -> Result<Vec<Address>> {
        if limit == 0 {
            return Err(Error::InvalidConfig("pagination limit must be positive".into()));
        }
        Ok(self.contract_call(pool, &CreditPool::getMarketUsersCall {
            market, start: U256::from(start), limit: U256::from(limit),
        }, at).await?)
    }

    async fn active_markets(&self, pool: Address, at: &BlockRef) -> Result<Vec<u64>> {
        Ok(self.contract_call(pool, &CreditPool::getActiveMarketsCall {}, at).await?)
    }
}

pub(crate) fn decode_call<C: SolCall>(bytes: &Bytes) -> Result<C::Return> {
    let value = C::abi_decode_returns_validate(bytes)
        .map_err(|error| Error::Decode(format!("{} return: {error}", C::SIGNATURE)))?;
    if C::abi_encode_returns(&value).as_slice() != bytes.as_ref() {
        return Err(Error::Decode(format!("{} return is not canonical ABI", C::SIGNATURE)));
    }
    Ok(value)
}

fn block_parameter(mode: BlockPinning, block: &BlockRef) -> Value {
    match mode {
        BlockPinning::Eip1898 => json!({ "blockHash": block.hash, "requireCanonical": true }),
        BlockPinning::RawHash => json!(block.hash),
        BlockPinning::NumberBracket => json!(quantity(block.number)),
    }
}

fn pruned_state_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if message.contains("execution reverted") { return false; }
    ["historical state unavailable", "historical state is unavailable", "state is unavailable",
        "missing trie node", "state not available", "state is not available", "state has been pruned",
        "state was pruned", "state is pruned", "pruned state"]
        .iter().any(|specific| message.contains(specific))
}

fn missing_hash_message(code: i64, message: &str) -> bool {
    if crate::rpc::is_missing_block_error(code, message) {return true;}
    let message = message.to_ascii_lowercase();
    if code == 3 || message.contains("execution reverted") { return false; }
    ["unknown block", "block not found", "header not found", "block does not exist",
        "block hash not found", "blockhash not found", "not canonical"]
        .iter().any(|specific| message.contains(specific))
}

fn capability_rpc_error(error: Error) -> Result<()> {
    match classify_rpc_error(&error) {
        RpcErrorKind::Permanent => Ok(()),
        _ => Err(error),
    }
}

fn tag_supported(result: Result<BlockRef>) -> Result<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(error @ Error::Rpc { .. }) => { capability_rpc_error(error)?; Ok(false) }
        Err(Error::Decode(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn quantity(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_quantity_value(value: &Value, context: &str) -> Result<u64> {
    let value = value.as_str().ok_or_else(|| Error::Decode(format!("{context} must be a hex quantity")))?;
    parse_quantity(value, context)
}

fn parse_quantity(value: &str, context: &str) -> Result<u64> {
    let digits = value.strip_prefix("0x").filter(|digits| !digits.is_empty())
        .ok_or_else(|| Error::Decode(format!("{context} must be a hex quantity")))?;
    if digits.len() > 16 || !digits.bytes().all(|digit| digit.is_ascii_hexdigit())
        || digits.len() > 1 && digits.starts_with('0')
    {
        return Err(Error::Decode(format!("{context} is not a canonical u64 hex quantity")));
    }
    u64::from_str_radix(digits, 16).map_err(|_| Error::Decode(format!("{context} invalid or exceeds u64")))
}

fn decode_json<T: serde::de::DeserializeOwned>(value: Value, context: &str) -> Result<T> {
    serde_json::from_value(value).map_err(|error| Error::Decode(format!("{context}: {error}")))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireBlock {
    number: String,
    hash: B256,
    parent_hash: B256,
    timestamp: String,
}

fn decode_block(value: Value, commitment: Commitment) -> Result<BlockRef> {
    let wire: WireBlock = decode_json(value, "block header")?;
    Ok(BlockRef {
        number: parse_quantity(&wire.number, "block number")?,
        hash: wire.hash,
        parent_hash: wire.parent_hash,
        timestamp: parse_quantity(&wire.timestamp, "block timestamp")?,
        commitment,
    })
}

// Keep this explicit: native multicall may execute arbitrary pool mutations,
// which must never be admitted into a view bundle used as a state witness.
fn validate_pool_view(call: &[u8], value: Option<&Bytes>) -> Result<()> {
    macro_rules! view {
        ($($name:ty),+ $(,)?) => { $(
            if call.starts_with(&<$name>::SELECTOR) {
                let decoded = <$name>::abi_decode_validate(call).map_err(|_|Error::Decode("invalid pool view calldata".into()))?;
                if decoded.abi_encode().as_slice() != call { return Err(Error::Decode("noncanonical pool view calldata".into())); }
                if let Some(value) = value {
                    let decoded = <$name>::abi_decode_returns_validate(value).map_err(|_|Error::Decode("invalid pool view result".into()))?;
                    if <$name>::abi_encode_returns(&decoded).as_slice() != value.as_ref() {
                        return Err(Error::Decode("noncanonical pool view result".into()));
                    }
                }
                return Ok(());
            }
        )+ };
    }
    view!(CreditPool::assetCall, CreditPool::collateralCall, CreditPool::versionCall, CreditPool::priceOracleCall,
        CreditPool::debtOfCall, CreditPool::collateralOfCall, CreditPool::loanOfCall, CreditPool::balanceOfCall,
        CreditPool::getMarketBalanceCall, CreditPool::getDelegateActionsCall, CreditPool::getBorrowLTVCall,
        CreditPool::getLiquidationLTVCall, CreditPool::getStalePriceWindowCall, CreditPool::getMinDebtRemainingAfterLiquidationCall,
        CreditPool::getLiquidationGracePeriodCall, CreditPool::getLiquidationDiscountMinCall, CreditPool::getLiquidationDiscountMaxCall,
        CreditPool::getActiveMarketsCall, CreditPool::getMarketUsersCall, CreditPool::getProvideOrderbookCall, CreditPool::previewBorrowCall);
    Err(Error::InvalidConfig("unsupported pool view selector".into()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    block_number: String,
    block_hash: B256,
    transaction_hash: B256,
    log_index: String,
    #[serde(default)]
    removed: bool,
}

//! Explicit opt-in cross-contract read aggregation. No submission or approvals.
use super::*;

sol! {
    /// Minimal ABI of mds1/Multicall3 aggregate3 (sender becomes the aggregator).
    interface Multicall3 {
        struct Call3 { address target; bool allowFailure; bytes callData; }
        struct Result { bool success; bytes returnData; }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
        function getBlockHash(uint256 blockNumber) external view returns (bytes32 blockHash);
    }
}

/// Explicitly trusted deployment and resource bounds for simulated view batches.
/// Disabled on new readers. Existing `read_bundle` semantics never change.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Multicall3Config {
    /// Deployed aggregator address, verified at each exact read anchor.
    pub address: Address,
    /// Keccak-256 of runtime bytecode, not SHA-256 or creation code.
    pub code_hash: B256,
    /// Maximum calls per outer eth_call (1..=64).
    pub max_calls: usize,
    /// Explicit eth_call gas ceiling; this gas is never paid by the reader.
    pub gas_limit: u64,
    /// Maximum encoded calldata bytes, before allocating an RPC body.
    pub max_calldata_bytes: usize,
}
impl Multicall3Config {
    /// Canonical Multicall3 runtime deployed on Monad mainnet and testnet.
    /// Deployment verification is still performed; the address alone is not trusted.
    pub fn canonical() -> Self {
        Self { address: alloy_primitives::address!("ca11bde05977b3631167028862be2a173976ca11"),
            code_hash: alloy_primitives::b256!("d5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891"),
            max_calls: 64, gas_limit: 10_000_000, max_calldata_bytes: 262_144 }
    }
    /// Validates local caps without contacting a node.
    pub fn validate(&self) -> Result<()> {
        if self.address.is_zero() || self.code_hash.is_zero() || !(1..=64).contains(&self.max_calls)
            || !(21_000..=30_000_000).contains(&self.gas_limit)
            || !(256..=1_048_576).contains(&self.max_calldata_bytes) {
            return Err(Error::InvalidConfig("Multicall3 identity or resource bounds".into()));
        }
        Ok(())
    }
}
impl HttpReader {
    /// Enables the explicit `read_views` API and SDK-owned account-explicit getters.
    /// Does not change arbitrary `read_bundle` / `contract_call` sender semantics.
    pub fn with_multicall3(mut self, config: Multicall3Config) -> Result<Self> {
        config.validate()?;
        self.multicall = Some(config);
        Ok(self)
    }

    /// Bounded read-only computation with shared opening headers and fresh closing
    /// checks. Return staged data from the future; do not publish it or perform
    /// external mutations inside the scope. Cancellation discards all memoization.
    /// Nested scopes are rejected. No cache survives the operation.
    pub async fn verified_read<T>(&self, operation: impl std::future::Future<Output=T>) -> Result<T> {
        session::verified_session(async { Ok(operation.await) }).await
    }

    /// Reads explicit, sender-independent view calls using the configured Multicall3.
    /// Without opt-in this retains direct `read_bundle` behavior. Code reads remain
    /// direct. All outputs retain input order; one failed call fails the whole batch.
    ///
    /// The caller MUST only supply views that do not depend on msg.sender or mutate
    /// state: aggregate3 uses CALL, and simulated mutations can affect later calls
    /// in the same batch. No value/from overrides, signatures or approvals are used.
    /// Typed return decoding remains the caller's responsibility.
    pub async fn read_views(&self, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        if reads.len() > 64 { return Err(Error::ResourceLimit("view bundle exceeds 64 reads".into())); }
        self.ensure_canonical(at).await?;
        let values = self.view_bundle_unchecked(reads, at).await;
        self.ensure_canonical(at).await?;
        values
    }

    pub(super) async fn view_bundle_unchecked(&self, reads: &[PinnedRead], at: &BlockRef) -> Result<Vec<Bytes>> {
        let Some(config) = &self.multicall else { return self.read_bundle_unchecked(reads, at).await; };
        // Validate lengths before encoding/allocating the aggregate calldata.
        let mut encoded_size = 68usize;
        for read in reads {
            if let PinnedRead::Call { to, data } = read {
                if to.is_zero() || *to == config.address || data.len() < 4 {
                    return Err(Error::InvalidConfig("invalid or recursive Multicall3 target".into()));
                }
                encoded_size = encoded_size.checked_add(160).and_then(|n| n.checked_add(data.len().div_ceil(32)*32))
                    .ok_or_else(|| Error::ResourceLimit("Multicall3 calldata overflow".into()))?;
                if encoded_size > config.max_calldata_bytes { return Err(Error::ResourceLimit("Multicall3 calldata limit".into())); }
            }
        }
        let calls: Vec<_> = reads.iter().filter_map(|read| match read {
            PinnedRead::Call { to, data } => Some(Multicall3::Call3 { target:*to, allowFailure:true, callData:data.clone() }),
            PinnedRead::Code { .. } => None,
        }).collect();
        if calls.len()<=1 {return self.read_bundle_unchecked(reads,at).await;}
        let call_values = self.aggregate_checked(&calls,at);
        let codes: Vec<_> = reads.iter().filter(|read| matches!(read,PinnedRead::Code { .. })).cloned().collect();
        let (calls, codes) = tokio::try_join!(call_values, self.read_bundle_unchecked(&codes,at))?;
        let mut calls = calls.into_iter(); let mut codes = codes.into_iter();
        Ok(reads.iter().map(|read| match read {
            PinnedRead::Call { .. } => calls.next().expect("validated call count"),
            PinnedRead::Code { .. } => codes.next().expect("validated code count"),
        }).collect())
    }

    // SDK-owned callers validate selector/target semantics before entering here.
    pub(super) async fn aggregate_checked(&self,calls:&[Multicall3::Call3],at:&BlockRef)->Result<Vec<Bytes>> {
        let config=self.multicall.as_ref().ok_or_else(||Error::InvalidConfig("Multicall3 is disabled".into()))?;
            if calls.is_empty() { return Ok(Vec::new()); }
            let code=if let Some(code)=session::code(self,config.address,at.hash) {code} else {
                let code:Bytes=decode_json(self.raw_pinned_request("eth_getCode",json!(config.address),at).await?,"Multicall3 code")?;
                session::remember_code(self,config.address,at.hash,code.clone());code
            };
            if code.is_empty() || keccak256(&code) != config.code_hash {
                return Err(Error::IdentityMismatch("Multicall3 runtime code hash differs".into()));
            }
            let mut out = Vec::with_capacity(calls.len());
            for chunk in calls.chunks(config.max_calls) {
                let data = Bytes::from(Multicall3::aggregate3Call { calls:chunk.to_vec() }.abi_encode());
                if data.len()>config.max_calldata_bytes {return Err(Error::ResourceLimit("Multicall3 calldata limit".into()));}
                let bytes: Bytes = decode_json(self.raw_pinned_request("eth_call",
                    json!({"to":config.address,"data":data,"gas":quantity(config.gas_limit)}),at).await?, "Multicall3 result")?;
                let values = Multicall3::aggregate3Call::abi_decode_returns_validate(&bytes)
                    .map_err(|e| Error::Decode(format!("Multicall3 result: {e}")))?;
                if values.len()!=chunk.len() || Multicall3::aggregate3Call::abi_encode_returns(&values).as_slice()!=bytes.as_ref() {
                    return Err(Error::Decode("noncanonical or incomplete Multicall3 result".into()));
                }
                for value in values {
                    if !value.success { return Err(Error::Decode("Multicall3 subcall failed".into())); }
                    out.push(value.returnData);
                }
            }
            Ok(out)
    }
}

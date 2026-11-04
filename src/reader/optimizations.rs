//! Optional bounded read optimizations; existing SDK defaults remain unchanged.
use super::*;

/// Resource policy for shared checks, recent-log proofs and position batches.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOptimizationConfig {
    /// Share identical checks waiting for transport admission, never completed reads.
    pub coalesce_pending_checks: bool,
    /// Verify recent log hashes through a pinned Multicall3 BLOCKHASH batch.
    pub batch_log_hashes: bool,
    /// Positions per native CreditPool multicall (1..=512; two getters each).
    pub position_batch_size: usize,
    /// Explicit gas ceiling for enlarged native position batches.
    pub position_batch_gas_limit: u64,
    /// Encoded calldata ceiling for enlarged native position batches.
    pub position_batch_calldata_limit: usize,
}
impl Default for ReadOptimizationConfig {
    fn default()->Self {Self {coalesce_pending_checks:false,batch_log_hashes:false,
        position_batch_size:32,position_batch_gas_limit:10_000_000,position_batch_calldata_limit:262_144}}
}
impl ReadOptimizationConfig {
    /// Validate local caps. Hash proofs additionally require an explicit Multicall3 deployment.
    pub fn validate(&self)->Result<()> {
        if !(1..=512).contains(&self.position_batch_size) || !(21_000..=30_000_000).contains(&self.position_batch_gas_limit)
            || !(256..=1_048_576).contains(&self.position_batch_calldata_limit) {
            return Err(Error::InvalidConfig("read optimization resource bounds".into()));
        }
        Ok(())
    }
}
impl HttpReader {
    /// Opt-in only: historical/default readers and arbitrary sender-sensitive calls
    /// retain their existing behavior. Configure Multicall3 before enabling hash proofs.
    pub fn with_read_optimizations(mut self,config:ReadOptimizationConfig)->Result<Self> {
        config.validate()?;
        if config.batch_log_hashes && self.multicall.is_none() {
            return Err(Error::InvalidConfig("batched log proofs require Multicall3".into()));
        }
        self.rpc=self.rpc.with_pending_read_coalescing(config.coalesce_pending_checks);
        self.optimizations=config;
        Ok(self)
    }

    pub(super) async fn verify_recent_log_blocks(&self,blocks:&[(u64,B256)],commitment:Commitment)->Result<()> {
        let Some(&(last,expected))=blocks.last() else {return Ok(());};
        // BLOCKHASH(current) is zero; the last witness needs a real canonical header.
        let anchor=self.block(last,commitment).await?.ok_or(Error::MissingBlock(last))?;
        if anchor.hash!=expected {return Err(Error::NonCanonical(last));}
        let recent=blocks.partition_point(|(n,_)|last-*n>256);
        let result=async {
            // Older witnesses require only headers, never old contract state or
            // downloading the empty blocks between sparse events.
            stream::iter(blocks[..recent].iter().copied().map(|(n,hash)|async move {
                let b=self.uncached_block(n,commitment).await?.ok_or(Error::MissingBlock(n))?;
                if b.hash!=hash {return Err(Error::NonCanonical(n));} Ok(())
            })).buffer_unordered(self.rpc.config().max_concurrent_requests).try_collect::<Vec<_>>().await?;
            let config=self.multicall.as_ref().expect("validated proof configuration");
            let calls:Vec<_>=blocks[recent..blocks.len()-1].iter().map(|(n,_)|Multicall3::Call3 {
                target:config.address,allowFailure:true,
                callData:Multicall3::getBlockHashCall{blockNumber:U256::from(*n)}.abi_encode().into(),
            }).collect();
            let values=self.aggregate_checked(&calls,&anchor).await?;
            for ((n,expected),value) in blocks[recent..blocks.len()-1].iter().zip(values) {
                if value.len()!=32 {return Err(Error::Decode("blockhash return length".into()));}
                let hash=decode_call::<Multicall3::getBlockHashCall>(&value)?;
                if hash.is_zero() || hash!=*expected {return Err(Error::NonCanonical(*n));}
            }
            Ok(())
        }.await;
        self.ensure_canonical(&anchor).await?;
        result
    }
}

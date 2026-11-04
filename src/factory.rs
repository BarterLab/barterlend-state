//! Bounded current-state discovery for the CREATE-based CreditPoolFactory v5.
//! No event history is needed. Pin a reviewed factory implementation: a CREATE2
//! factory or a proxy is not compatible with this enumeration method.
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::sol;
use serde::{Deserialize, Serialize};
use crate::{BlockRef, Error, HttpReader, PinnedRead, Result};

sol! {
    /// Read-only factory identity and membership getters.
    interface CreditPoolFactory {
        function poolVersion() external view returns (uint256);
        function previousFactory() external view returns (address);
        function isPool(address pool) external view returns (bool);
    }
}

/// Reviewed deployment identity. Older factories must be supplied separately.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorySpec {
    /// Factory deployment address.
    pub address: Address,
    /// Keccak-256 of its immutable runtime code.
    pub code_hash: B256,
    /// Pinned predecessor; zero means this is the first factory.
    pub previous_factory: Address,
}
impl FactorySpec {
    /// Rejects absent identities before making any requests.
    pub fn validate(&self) -> Result<()> {
        if self.address.is_zero() || self.code_hash.is_zero() || self.address == self.previous_factory {
            return Err(Error::InvalidConfig("invalid factory identity".into()));
        }
        Ok(())
    }
}

/// One complete, block-pinned page of CREATE addresses filtered by `isPool`.
#[derive(Clone, Debug)]
pub struct FactoryPage {
    /// Pools found in this page, in creation order.
    pub pools: Vec<Address>,
    /// First creation nonce not inspected. Start at one on a fresh process.
    pub next_nonce: u64,
    /// Account nonce at the anchor; the page is complete when next_nonce equals it.
    pub account_nonce: u64,
}
impl HttpReader {
    /// Inspects at most 64 CREATE addresses, through the reader's shared RPC budget.
    /// A regressed nonce, changed code/version/predecessor or reorg is an error;
    /// callers must not advance a cursor on error. The factory must use CREATE.
    pub async fn factory_page(&self, factory: &FactorySpec, next_nonce: u64, page_size: usize, at: &BlockRef) -> Result<FactoryPage> {
        factory.validate()?;
        if next_nonce == 0 || !(1..=64).contains(&page_size) {
            return Err(Error::InvalidConfig("factory cursor/page size".into()));
        }
        self.ensure_canonical(at).await?;
        let reads = self.read_views(&[
            PinnedRead::Code { address: factory.address },
            PinnedRead::call(factory.address, &CreditPoolFactory::poolVersionCall {}),
            PinnedRead::call(factory.address, &CreditPoolFactory::previousFactoryCall {}),
        ], at).await?;
        let version = crate::reader::decode_call::<CreditPoolFactory::poolVersionCall>(&reads[1])?;
        let previous = crate::reader::decode_call::<CreditPoolFactory::previousFactoryCall>(&reads[2])?;
        if keccak256(&reads[0]) != factory.code_hash || version != U256::from(5) || previous != factory.previous_factory {
            return Err(Error::IdentityMismatch("factory code/version/predecessor".into()));
        }
        // Number bracketing also works on nodes that do not accept EIP-1898 for
        // eth_getTransactionCount. The closing canonical check is mandatory.
        let raw = self.rpc().request("eth_getTransactionCount", serde_json::json!([factory.address, format!("0x{:x}", at.number)])).await?;
        let value: U256 = serde_json::from_value(raw)?;
        let account_nonce = u64::try_from(value).map_err(|_| Error::ResourceLimit("factory nonce exceeds u64".into()))?;
        if account_nonce < next_nonce { return Err(Error::IdentityMismatch("factory nonce regressed".into())); }
        let end = next_nonce.saturating_add(page_size as u64).min(account_nonce);
        let candidates: Vec<_> = (next_nonce..end).map(|n| factory.address.create(n)).collect();
        let calls: Vec<_> = candidates.iter().map(|&pool| PinnedRead::call(factory.address, &CreditPoolFactory::isPoolCall { pool })).collect();
        let values = if calls.is_empty() { Vec::new() } else { self.read_views(&calls, at).await? };
        let mut pools = Vec::new();
        for (pool, bytes) in candidates.into_iter().zip(values) {
            if crate::reader::decode_call::<CreditPoolFactory::isPoolCall>(&bytes)? { pools.push(pool); }
        }
        self.ensure_canonical(at).await?;
        Ok(FactoryPage { pools, next_nonce: end, account_nonce })
    }
}

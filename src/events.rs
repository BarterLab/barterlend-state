//! Maps events to borrower positions and configuration that need refreshing.
//! Getters provide balances; event amounts are not accumulated.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::Address;
use alloy_sol_types::SolEvent;

use crate::{abi::CreditPool, Error, Result, types::{PositionKey, RawLog}};

/// Keys/configs to reread after decoding events, not transaction-derived balances.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Invalidations {
    /// Affected borrower positions requiring getter refresh.
    pub positions: BTreeSet<PositionKey>,
    /// Affected markets requiring effective-config refresh or discovery.
    pub markets: BTreeSet<u64>,
    /// A global/effective configuration event was observed.
    pub config_changed: bool,
}

impl Invalidations {
    fn position(&mut self, pool: Address, market: u64, borrower: Address) {
        self.markets.insert(market);
        self.positions.insert(PositionKey { pool, market, borrower });
    }

    /// Merges affected keys and configuration flags.
    pub fn extend(&mut self, other: Self) {
        self.positions.extend(other.positions);
        self.markets.extend(other.markets);
        self.config_changed |= other.config_changed;
    }
}

fn event<E: SolEvent>(log: &RawLog) -> Result<E> {
    let value = E::decode_raw_log_validate(log.topics.iter().copied(), &log.data)
        .map_err(|error| Error::Decode(format!("{}: {error}", E::SIGNATURE)))?;
    // Reject extra topics, dirty indexed padding, trailing data and noncanonical offsets.
    let encoded = value.encode_log_data();
    if encoded.topics() != log.topics || encoded.data != log.data {
        return Err(Error::Decode(format!("noncanonical encoding of {}", E::SIGNATURE)));
    }
    Ok(value)
}

/// Validates a pool event and extracts affected borrower and configuration keys.
/// Rejects malformed known events, foreign addresses and removed logs.
pub fn decode_invalidation(pool: Address, log: &RawLog) -> Result<Invalidations> {
    if log.address != pool {
        return Err(Error::Decode("log address differs from pool".into()));
    }
    if log.removed {
        return Err(Error::NonCanonical(log.block_number));
    }
    let signature = log.topics.first().ok_or_else(|| Error::Decode("event without signature".into()))?;
    let mut result = Invalidations::default();
    macro_rules! position {
        ($name:ident) => {
            if *signature == CreditPool::$name::SIGNATURE_HASH {
                let value = event::<CreditPool::$name>(log)?;
                result.position(pool, value.market, value.account);
                return Ok(result);
            }
        };
    }
    position!(Borrowed);
    position!(CollateralDeposited);
    position!(CollateralWithdrawn);
    position!(DebtRepaid); // account is borrower; payer is deliberately ignored.
    position!(DebtOffsetWithLoan);
    position!(Liquidated);
    position!(BadDebtIncurred);
    if *signature == CreditPool::Provided::SIGNATURE_HASH {
        let value = event::<CreditPool::Provided>(log)?;
        result.markets.insert(value.market);
        for fill in value.fills {
            if !fill.isReloan {
                result.position(pool, value.market, fill.account);
            }
        }
        return Ok(result);
    }
    if *signature == CreditPool::DebtRefinanced::SIGNATURE_HASH {
        let value = event::<CreditPool::DebtRefinanced>(log)?;
        result.position(pool, value.fromMarket, value.account);
        result.position(pool, value.toMarket, value.account);
        return Ok(result);
    }
    if *signature == CreditPool::CollateralMoved::SIGNATURE_HASH {
        let value = event::<CreditPool::CollateralMoved>(log)?;
        result.position(pool, value.fromMarket, value.account);
        result.position(pool, value.toMarket, value.account);
        return Ok(result);
    }
    if *signature == CreditPool::AdminConfigUpdated::SIGNATURE_HASH {
        event::<CreditPool::AdminConfigUpdated>(log)?;
        result.config_changed = true;
        return Ok(result);
    }
    // Decode even irrelevant events: known ABI with malformed data must not be silently skipped.
    macro_rules! ignore {
        ($($name:ident),+ $(,)?) => {$({
            if *signature == CreditPool::$name::SIGNATURE_HASH {
                event::<CreditPool::$name>(log)?;
                return Ok(result);
            }
        })+};
    }
    ignore!(AssetsDeposited, AssetsWithdrawn, BorrowOrderCancelled, BorrowOrderPlaced,
        Claimed, DelegateUpdated, LoanTransferred, OwnershipTransferred,
        ProvideOrderCancelled, ProvideOrderPlaced, ReloanOrderCancelled, ReloanOrderPlaced,
        Reloaned, Swept, UnderlyingPoolDustSettled, UnderlyingPoolIndexUpdated);
    Err(Error::Decode(format!("unknown event signature {signature} for protocol v5")))
}

/// Sorts and deduplicates exact log copies for one pool/range.
/// Conflicting copies at one index, foreign/out-of-range/removed logs reject the range.
/// Engine verifies canonical headers separately.
pub fn normalize_logs(pool: Address, from: u64, to: u64, logs: Vec<RawLog>) -> Result<Vec<RawLog>> {
    let mut ordered = BTreeMap::new();
    let mut block_hashes = BTreeMap::new();
    for log in logs {
        if log.address != pool || log.block_number < from || log.block_number > to {
            return Err(Error::Decode("RPC returned log outside requested pool/range".into()));
        }
        if log.removed || log.block_hash.is_zero() || log.transaction_hash.is_zero() {
            return Err(Error::NonCanonical(log.block_number));
        }
        if let Some(hash) = block_hashes.insert(log.block_number, log.block_hash) {
            if hash != log.block_hash {
                return Err(Error::NonCanonical(log.block_number));
            }
        }
        let index = (log.block_number, log.log_index);
        if let Some(old) = ordered.get(&index) {
            if old != &log {
                return Err(Error::Decode("conflicting logs at one block/log index".into()));
            }
        } else {
            ordered.insert(index, log);
        }
    }
    Ok(ordered.into_values().collect())
}

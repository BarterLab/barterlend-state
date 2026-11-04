//! Snapshots, positions and risk. Token amounts use exact integer base units.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};

/// Network identity: chain ID and genesis hash.
/// Set a trusted value in [`crate::EngineConfig::expected_identity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainIdentity {
    /// Nonzero EVM chain ID.
    pub chain_id: u64,
    /// Block0 hash; distinguishes resets and networks sharing a chain ID.
    pub genesis_hash: B256,
}

/// Head-selection policy. Unsupported tags never fall back to latest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Commitment {
    /// Default: the RPC `finalized` tag.
    #[default]
    Finalized,
    /// Explicit RPC safe tag.
    Safe,
    /// Speculative head; consumers must handle reorgs and freshness.
    Latest,
}

/// Read anchor: height, hashes, chain timestamp and selected commitment.
/// For numbered headers commitment is metadata; Engine checks the target against the selected head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    /// Block height, not wall-clock time.
    pub number: u64,
    /// Hash of this canonical header at verification time.
    pub hash: B256,
    /// Parent header hash used for consistency checks.
    pub parent_hash: B256,
    /// Chain timestamp in seconds, used by oracle/maturity rules.
    pub timestamp: u64,
    /// Tag policy used to select the target.
    pub commitment: Commitment,
}

/// Trusted inputs for one CreditPool v5 deployment.
/// The inclusive floor is genesis or a verified creation block; an arbitrary later
/// canonical block cannot prove complete discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolSpec {
    /// Pool address.
    pub address: Address,
    /// First inclusive discovery block: genesis or the verified pool creation block.
    pub deployment_block: u64,
    /// Expected protocol version; the current ABI supports v5.
    pub expected_version: u64,
    /// Optional independently checked floor hash; bootstrap pins the header it reads.
    pub deployment_block_hash: Option<B256>,
}

/// ERC20 address and decimals read at the target. Amounts stay in raw units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenDescriptor {
    /// ERC20 token address.
    pub address: Address,
    /// Display scale for raw amounts. Oracle ratios already include decimals.
    pub decimals: u8,
}

/// Pool metadata at the snapshot block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolDescriptor {
    /// Described pool contract address.
    pub address: Address,
    /// Protocol version getter result.
    pub version: u64,
    /// Debt/repayment token and its decimals.
    pub asset: TokenDescriptor,
    /// Collateral token and its decimals.
    pub collateral: TokenDescriptor,
    /// Price source for collateral-to-asset conversion.
    pub oracle: Address,
    /// Runtime-code hash of the pool.
    pub code_hash: B256,
}

/// Global protocol risk settings at the snapshot anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolRiskConfig {
    /// Liquidation LTV scaled by `1e18`.
    pub liquidation_ltv: U256,
    /// Oracle stale window in chain-time seconds.
    pub stale_window: u64,
    /// Minimum nonzero debt left after a partial liquidation, in asset raw units.
    #[serde(with = "decimal_u128")]
    pub min_debt_remaining: u128,
}

/// Effective settings for one market, reread at the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRiskConfig {
    /// Liquidation grace after the market timestamp, in seconds.
    pub grace_period: u64,
    /// Minimum liquidation discount scaled by `1e18`.
    pub discount_min: U256,
    /// Maximum liquidation discount scaled by `1e18`.
    pub discount_max: U256,
}

/// Borrower-position identity: pool, market and borrower.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PositionKey {
    /// Pool containing this position.
    pub pool: Address,
    /// Protocol market identifier/timestamp; market0 and arbitrary markets are valid.
    pub market: u64,
    /// Borrower-position owner, not necessarily the transaction sender or payer.
    pub borrower: Address,
}

/// Stored getter amounts without floating point or extra decimals normalization.
/// JSON u128 values use unsigned decimal strings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionAmounts {
    /// Stored debt in asset base units.
    #[serde(with = "decimal_u128")]
    pub debt: u128,
    /// Collateral base units; debt-zero records remain while collateral is positive.
    #[serde(with = "decimal_u128")]
    pub collateral: u128,
}

/// Oracle ratio from collateral raw units to asset raw units, including decimals.
/// Do not apply another powers-of-ten conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Price {
    /// Ratio numerator; protocol risk validation checks admissibility.
    pub numerator: U256,
    /// Ratio denominator; zero is unusable.
    pub denominator: U256,
    /// Raw oracle updatedAt. V5 accepts future timestamps; conservative consumers check them separately.
    pub updated_at: U256,
}

/// Reason a price is unavailable. RPC/decode details may contain provider text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum OracleFailure {
    Rpc(String),
    Decode(String),
    PairMismatch {
        expected_source: Address,
        expected_target: Address,
        source: Address,
        target: Address,
    },
    ZeroNumerator,
    ZeroDenominator,
    RatioTooLarge,
    TimestampOverflow,
    Stale {
        updated_at: U256,
        stale_window: u64,
        block_timestamp: u64,
    },
}

/// Price status at the anchor; an outage does not erase raw position amounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum OracleStatus {
    Ready(Price),
    Unavailable(OracleFailure),
}

/// Reason risk inputs or arithmetic are unusable for this position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum RiskFailure {
    Oracle(OracleFailure),
    Arithmetic(String),
    InvalidConfig(String),
}

/// Protocol risk or a typed failure. Ready does not establish liquidation profitability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum RiskStatus {
    Ready(PositionRisk),
    Unavailable(RiskFailure),
}

/// Protocol-exact HF/eligibility/discount at the snapshot chain timestamp.
/// Debt-zero/collateral-zero cases preserve getter short-circuits even with an unusable oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionRisk {
    /// HF scaled by1e18. Equality at1e18 is not an unhealthy trigger.
    pub health_factor: U256,
    /// Protocol eligibility: unhealthy HF or reaching maturity plus grace.
    pub eligible: bool,
    /// Liquidation discount scaled by1e18.
    pub discount: U256,
    /// Whether the position has positive collateral.
    pub has_collateral: bool,
    /// Eligible with usable risk inputs and collateral. Sizing, profitability and
    /// execution checks belong to the consumer.
    pub execution_ready: bool,
}

/// Amounts and evaluated risk for one borrower position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionRecord {
    /// Record identity; must match the surrounding snapshot-map key.
    pub key: PositionKey,
    /// Raw getter values at snapshot.as_of.
    pub amounts: PositionAmounts,
    /// Risk computed using current configs, oracle and chain time.
    pub risk: RiskStatus,
    /// Last published amounts/risk change, not every transaction or read timestamp.
    pub last_changed: BlockRef,
}

/// One pool at one anchor; separate pools do not form a cross-pool atomic snapshot.
/// Restore requires a sync; after an error, the retained snapshot may be stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// Chain/genesis verified by the engine.
    pub identity: ChainIdentity,
    /// Trusted floor/spec with its pinned deployment header hash.
    pub spec: PoolSpec,
    /// Verified deployment descriptor.
    pub pool: PoolDescriptor,
    /// Pinned ABI fingerprint validated during restore.
    pub abi_fingerprint: B256,
    /// Snapshot generation. Rebuild increments it; invalidate unsigned plans from old epochs.
    pub epoch: u64,
    /// Anchor of the complete snapshot or consolidated batch, including unchanged positions.
    pub as_of: BlockRef,
    /// Global risk settings at as_of.
    pub globals: PoolRiskConfig,
    /// Effective configs of active markets and markets with nonempty positions.
    pub markets: BTreeMap<u64, MarketRiskConfig>,
    /// Shared oracle price or failure reason.
    pub oracle: OracleStatus,
    /// Discovered nonempty borrower positions; only debt-zero plus collateral-zero records are removed.
    #[serde(with = "position_map")]
    pub positions: BTreeMap<PositionKey, PositionRecord>,
}

/// Final-state changes, not a balance replay of every transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StateUpdate {
    PositionUpsert(PositionRecord),
    PositionRemoved(PositionKey),
    PoolConfigChanged(PoolRiskConfig),
    MarketConfigChanged {
        market: u64,
        config: MarketRiskConfig,
    },
    /// An inactive market with no remaining positions was evicted.
    MarketRemoved(u64),
    OracleChanged(OracleStatus),
}

/// Consolidated changes for `(from_exclusive, as_of]`.
/// Cold start/reset provides a full snapshot through [`SyncOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateBatch {
    /// Snapshot generation. Rebuild increments it; invalidate unsigned plans from old epochs.
    pub epoch: u64,
    /// Previous anchor, excluded from the scanned range.
    pub from_exclusive: BlockRef,
    /// Anchor of the complete snapshot or consolidated batch, including unchanged positions.
    pub as_of: BlockRef,
    /// Final position, configuration and oracle changes.
    pub changes: Vec<StateUpdate>,
}

/// Successful [`crate::StateEngine::sync`] result; failures return no partial outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum SyncOutcome {
    /// Initial complete snapshot from an engine with no state.
    Bootstrapped(StateSnapshot),
    /// New canonical target and changes; engine.snapshot() provides the complete state.
    Advanced(UpdateBatch),
    /// Same canonical head; epoch/as_of stay unchanged without repeated raw reads.
    Unchanged {
        epoch: u64,
        as_of: BlockRef,
    },
    /// Complete rebuild with a new epoch after reorg or an unverifiable old cursor.
    Reset(StateSnapshot),
}

/// Log envelope used for address/range/canonical-block and ABI validation.
/// Discovery logs are not summed into position balances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawLog {
    /// Emitting contract address; Engine expects the selected pool.
    pub address: Address,
    /// Event signature and indexed ABI arguments.
    pub topics: Vec<B256>,
    /// Non-indexed ABI event arguments.
    pub data: Bytes,
    /// Log inclusion height.
    pub block_number: u64,
    /// Block hash checked against the historical canonical header.
    pub block_hash: B256,
    /// Transaction hash used for log identity/normalization.
    pub transaction_hash: B256,
    /// Log index; conflicting duplicates reject the range.
    pub log_index: u64,
    /// Removed/orphan logs cannot enter a verified census.
    pub removed: bool,
}

// JSON Number and JavaScript doubles cannot reliably carry ERC20 uint128 amounts.
// Keep the Rust API integer-typed and require exact unsigned decimal strings on the wire.
mod decimal_u128 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() || value.len() > 39 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(serde::de::Error::custom("amount must be an unsigned decimal uint128 string"));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}

mod position_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serializer};

    use super::{PositionKey, PositionRecord};

    pub fn serialize<S>(
        positions: &BTreeMap<PositionKey, PositionRecord>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        for (key, position) in positions {
            if key != &position.key {
                return Err(serde::ser::Error::custom("position map key differs from record key"));
            }
        }
        serializer.collect_seq(positions.values())
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<PositionKey, PositionRecord>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let positions = Vec::<PositionRecord>::deserialize(deserializer)?;
        let mut result = BTreeMap::new();
        for position in positions {
            if result.insert(position.key, position).is_some() {
                return Err(serde::de::Error::custom("duplicate position in snapshot"));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block() -> BlockRef {
        BlockRef {
            number: 7,
            hash: B256::repeat_byte(7),
            parent_hash: B256::repeat_byte(6),
            timestamp: 70,
            commitment: Commitment::Finalized,
        }
    }

    fn snapshot() -> StateSnapshot {
        let address = Address::repeat_byte(1);
        let key = PositionKey { pool: address, market: 100, borrower: Address::repeat_byte(2) };
        let record = PositionRecord {
            key,
            amounts: PositionAmounts { debt: 0, collateral: 100 },
            risk: RiskStatus::Ready(PositionRisk {
                health_factor: U256::from(1_000_000_000_000_000_000_u128),
                eligible: false,
                discount: U256::ZERO,
                has_collateral: true,
                execution_ready: false,
            }),
            last_changed: block(),
        };
        StateSnapshot {
            identity: ChainIdentity { chain_id: 143, genesis_hash: B256::repeat_byte(3) },
            spec: PoolSpec { address, deployment_block: 1, expected_version: 5, deployment_block_hash: None },
            pool: PoolDescriptor {
                address,
                version: 5,
                asset: TokenDescriptor { address: Address::repeat_byte(4), decimals: 6 },
                collateral: TokenDescriptor { address: Address::repeat_byte(5), decimals: 18 },
                oracle: Address::repeat_byte(6),
                code_hash: B256::repeat_byte(8),
            },
            abi_fingerprint: B256::repeat_byte(9),
            epoch: 1,
            as_of: block(),
            globals: PoolRiskConfig { liquidation_ltv: U256::ZERO, stale_window: 10, min_debt_remaining: 0 },
            markets: BTreeMap::new(),
            oracle: OracleStatus::Unavailable(OracleFailure::Rpc("offline".into())),
            positions: BTreeMap::from([(key, record)]),
        }
    }

    #[test]
    fn snapshot_json_roundtrip_preserves_collateral_only_position() {
        let original = snapshot();
        let json = serde_json::to_value(&original).unwrap();
        assert!(json["positions"].is_array());
        assert_eq!(json["positions"].as_array().unwrap().len(), 1);
        assert_eq!(serde_json::from_value::<StateSnapshot>(json).unwrap(), original);
    }

    #[test]
    fn duplicate_position_entries_are_rejected() {
        let mut json = serde_json::to_value(snapshot()).unwrap();
        let position = json["positions"][0].clone();
        json["positions"].as_array_mut().unwrap().push(position);
        assert!(serde_json::from_value::<StateSnapshot>(json).is_err());
    }

    #[test]
    fn mismatched_map_key_is_rejected_before_serialization() {
        let mut snapshot = snapshot();
        let key = *snapshot.positions.keys().next().unwrap();
        snapshot.positions.get_mut(&key).unwrap().key.market = 200;
        assert!(serde_json::to_value(snapshot).is_err());
    }

    #[test]
    fn uint128_amounts_roundtrip_through_json_value_without_precision_loss() {
        let amounts = PositionAmounts { debt: u128::MAX, collateral: 110_000_000_000_000_000_000 };
        let value = serde_json::to_value(amounts).unwrap();
        assert_eq!(value["debt"], "340282366920938463463374607431768211455");
        assert_eq!(value["collateral"], "110000000000000000000");
        assert_eq!(serde_json::from_value::<PositionAmounts>(value).unwrap(), amounts);
        let globals = PoolRiskConfig { liquidation_ltv: U256::ZERO, stale_window: 1, min_debt_remaining: u128::MAX };
        let value = serde_json::to_value(globals).unwrap();
        assert_eq!(value["min_debt_remaining"], u128::MAX.to_string());
        assert_eq!(serde_json::from_value::<PoolRiskConfig>(value).unwrap(), globals);
    }

    #[test]
    fn amount_json_rejects_rounded_numeric_or_invalid_inputs() {
        for debt in [serde_json::json!(1), serde_json::json!("1.5"), serde_json::json!("1e18"),
            serde_json::json!("-1"), serde_json::json!("340282366920938463463374607431768211456")] {
            assert!(serde_json::from_value::<PositionAmounts>(serde_json::json!({
                "debt": debt, "collateral": "0"
            })).is_err());
        }
    }
}

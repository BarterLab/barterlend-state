//! Saved progress for unfinished history discovery, separate from complete snapshots.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, abi::{ABI_FINGERPRINT, PROTOCOL_VERSION}, checkpoint::ReconciliationCursor, types::*};

/// Format of the independently persisted discovery file.
pub const DISCOVERY_SCHEMA: u32 = 2;

/// Initial census and the final tail/read stage use separate anchors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryPhase {
    /// Historical logs and headers; execution state at this target is unnecessary.
    Census,
    /// History is extended to this fixed, freshly chosen getter target.
    Snapshot,
}

/// A diagnostic summary of bounded work. This is not a StateSnapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryStatus {
    /// Discovery restart/reset counter, distinct from the published snapshot epoch.
    pub generation: u64,
    /// Inclusive initial floor or first block after the saved state cursor.
    pub from_block: u64,
    /// Last completely verified history chunk; None before the first chunk completes.
    pub through: Option<BlockRef>,
    /// Fixed endpoint of the current census or getter-tail phase.
    pub target: BlockRef,
    /// Historical census or fresh snapshot-tail/read stage.
    pub phase: DiscoveryPhase,
    /// Number of distinct invalidated/discovered keys, including keys now empty.
    pub discovered_positions: usize,
    /// Number of markets discovered from historical events in this operation.
    pub discovered_markets: usize,
    /// An existing snapshot is being fully rebuilt after reorg or explicit bootstrap.
    pub rebuilding: bool,
}

/// One incremental step either publishes complete state or reports discovery progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum SyncStepOutcome {
    /// Discovery is incomplete; no new snapshot is available.
    Progress(DiscoveryStatus),
    /// The ordinary complete Bootstrap/Advanced/Unchanged/Reset result.
    Complete(SyncOutcome),
}

/// Private-to-discovery getter results, all read at `DiscoveryCheckpoint::target`.
/// These records are incomplete and must never be used as an execution snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetterCheckpoint {
    /// Verified pool descriptor, once read at the target.
    pub pool: Option<PoolDescriptor>,
    /// Complete known/active market set, once read at the target.
    pub markets: Option<BTreeSet<u64>>,
    /// Active calendar at the getter anchor, separate from temporary discovered markets.
    /// Older checkpoints omit it and re-read it before publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_markets: Option<BTreeSet<u64>>,
    /// Event-invalidated and periodically reconciled position keys to read.
    pub keys: BTreeSet<PositionKey>,
    /// Staged pagination offsets; published only with the complete snapshot.
    pub reconciliation: ReconciliationCursor,
    /// Remaining bounded user pages for this snapshot.
    pub user_pages_remaining: usize,
    /// Pool parameters, once read at the target.
    pub globals: Option<PoolRiskConfig>,
    /// Current adaptive batch width, bounded by EngineConfig.read_batch_size.
    pub batch_size: usize,
    /// Interrupted/in-flight batch length. Continuation halves its width down to one.
    /// A successful batch clears this marker before any next await.
    pub pending_batch_size: Option<usize>,
    /// Completed effective-config batches.
    pub configs: BTreeMap<u64, MarketRiskConfig>,
    /// Oracle result, once read at the target.
    pub oracle: Option<OracleStatus>,
    /// Completed position batches, including empty positions to remove at publication.
    #[serde(with = "amount_map")]
    pub amounts: BTreeMap<PositionKey, PositionAmounts>,
}

impl GetterCheckpoint {
    pub(crate) fn continues(&self, previous: &Self) -> bool {
        previous.pool.as_ref().is_none_or(|value| self.pool.as_ref() == Some(value))
            && previous.markets.as_ref().is_none_or(|value| self.markets.as_ref() == Some(value))
            && previous.active_markets.as_ref().is_none_or(|v| self.active_markets.as_ref() == Some(v))
            && previous.globals.as_ref().is_none_or(|value| self.globals.as_ref() == Some(value))
            && previous.oracle.as_ref().is_none_or(|value| self.oracle.as_ref() == Some(value))
            && previous.keys.is_subset(&self.keys)
            && self.batch_size <= previous.batch_size
            && self.user_pages_remaining <= previous.user_pages_remaining
            && (self.user_pages_remaining != previous.user_pages_remaining || self.reconciliation == previous.reconciliation
                // Selecting live markets prunes offsets for empty expired markets.
                || previous.markets.is_none() && self.markets.is_some()
                    && self.reconciliation.last_position == previous.reconciliation.last_position
                    && self.reconciliation.user_offsets.iter().all(|(market, offset)| previous.reconciliation.user_offsets.get(market) == Some(offset))
                    && (self.reconciliation.next_market.is_none() || self.reconciliation.next_market == previous.reconciliation.next_market))
            && previous.configs.iter().all(|(key, value)| self.configs.get(key) == Some(value))
            && previous.amounts.iter().all(|(key, value)| self.amounts.get(key) == Some(value))
    }
}

mod amount_map {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(values: &BTreeMap<PositionKey, PositionAmounts>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(values.iter())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<BTreeMap<PositionKey, PositionAmounts>, D::Error> {
        let values = Vec::<(PositionKey, PositionAmounts)>::deserialize(deserializer)?;
        let mut result = BTreeMap::new();
        for (key, value) in values {
            if result.insert(key, value).is_some() { return Err(serde::de::Error::custom("duplicate getter position")); }
        }
        Ok(result)
    }
}

/// Restart data for unfinished discovery or catch-up. Restore checks its base-state
/// checksum; the next engine step verifies network identity and canonicality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryCheckpoint {
    /// Discovery format version, separate from ordinary checkpoints.
    pub schema_version: u32,
    /// Source chain ID and genesis that must survive continuation.
    pub identity: ChainIdentity,
    /// Trusted pool/history floor, with its pinned floor-header hash.
    pub spec: PoolSpec,
    /// CreditPool ABI compatibility identity.
    pub abi_fingerprint: B256,
    /// Checksum of the unchanged ordinary StateCheckpoint, or None for a cold start.
    pub base_checksum: Option<B256>,
    /// Epoch of the next complete snapshot; not published before completion.
    pub epoch: u64,
    /// Allows an explicit reorg reset to rewind the discovery cursor safely.
    pub generation: u64,
    /// True when replacing ordinary state through a full census, including explicit bootstrap.
    pub rebuilding: bool,
    /// First inclusive block of this discovery operation.
    pub from_block: u64,
    /// Last fully verified history cursor.
    pub through: Option<BlockRef>,
    /// Current fixed log endpoint or the freshly selected getter target.
    pub target: BlockRef,
    /// Stage determining whether execution state at target will be needed.
    pub phase: DiscoveryPhase,
    /// Discovered/invalidated keys; getters determine whether records remain nonempty.
    pub positions: BTreeSet<PositionKey>,
    /// Whether the scanned interval contains an administrative config change.
    /// None in older checkpoints forces a full configuration refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_all_config: Option<bool>,
    /// Historical market keys requiring effective-config reads.
    pub markets: BTreeSet<u64>,
    /// Completed getter batches at target. Schema 1 files omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub getters: Option<GetterCheckpoint>,
    /// Checksum for detecting accidental corruption.
    pub checksum: B256,
}

impl DiscoveryCheckpoint {
    pub(crate) fn seal(&mut self) -> Result<()> {
        self.schema_version = DISCOVERY_SCHEMA;
        self.checksum = self.digest()?;
        self.validate(None, None)
    }

    fn digest(&self) -> Result<B256> {
        let history = (
            self.identity, self.spec, self.abi_fingerprint, self.base_checksum,
            self.epoch, self.generation, self.rebuilding, self.from_block,
            self.through, self.target, self.phase, &self.positions, &self.markets,
        );
        if let Some(refresh) = self.refresh_all_config { crate::encoding::digest(&(history, &self.getters, refresh)) }
        else if self.schema_version == 1 { crate::encoding::digest(&history) }
        else { crate::encoding::digest(&(history, &self.getters)) }
    }

    /// Validates integrity, structure and optional deployment inputs without RPC.
    pub fn validate(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<()> {
        let invalid = |reason: &str| Error::Checkpoint(format!("discovery: {reason}"));
        if !matches!(self.schema_version, 1 | DISCOVERY_SCHEMA) || self.schema_version == 1 && self.getters.is_some()
            || self.checksum != self.digest()?
            || self.abi_fingerprint != ABI_FINGERPRINT
        {
            return Err(invalid("schema/checksum/ABI mismatch"));
        }
        if self.identity.chain_id == 0 || self.identity.genesis_hash.is_zero()
            || self.spec.address.is_zero() || self.spec.expected_version != PROTOCOL_VERSION
            || self.spec.deployment_block_hash.is_none_or(|hash| hash.is_zero())
            || self.epoch == 0 || self.generation == 0
            || self.from_block < self.spec.deployment_block || self.target.number < self.from_block
            || self.target.hash.is_zero()
        {
            return Err(invalid("invalid identity/deployment/range/generation"));
        }
        if identity.is_some_and(|expected| *expected != self.identity) {
            return Err(Error::IdentityMismatch("discovery chain/genesis".into()));
        }
        if let Some(expected) = spec {
            if expected.address != self.spec.address || expected.deployment_block != self.spec.deployment_block
                || expected.expected_version != self.spec.expected_version
                || expected.deployment_block_hash.is_some_and(|hash| Some(hash) != self.spec.deployment_block_hash)
            {
                return Err(Error::IdentityMismatch("discovery PoolSpec".into()));
            }
        }
        if self.through.is_some_and(|cursor| cursor.number < self.from_block
            || cursor.number > self.target.number || cursor.hash.is_zero()
            || cursor.commitment != self.target.commitment || cursor.timestamp > self.target.timestamp)
            || self.positions.iter().any(|key| key.pool != self.spec.address || !self.markets.contains(&key.market))
            || self.through.is_none() && (!self.positions.is_empty() || !self.markets.is_empty())
        {
            return Err(invalid("invalid history cursor/keys"));
        }
        if let Some(getters) = &self.getters {
            if self.phase != DiscoveryPhase::Snapshot || self.through != Some(self.target)
                || getters.batch_size == 0 || getters.pending_batch_size.is_some_and(|size| size == 0 || size > getters.batch_size)
                || !self.positions.is_subset(&getters.keys)
                || getters.keys.iter().any(|key| key.pool != self.spec.address)
                || getters.pool.as_ref().is_some_and(|pool| pool.address != self.spec.address
                    || pool.version != self.spec.expected_version || pool.asset.address.is_zero()
                    || pool.collateral.address.is_zero() || pool.oracle.is_zero() || pool.code_hash.is_zero())
                || getters.markets.as_ref().is_some_and(|markets| getters.active_markets.as_ref().is_some_and(|active| !active.is_subset(markets))
                    || getters.keys.iter().any(|key| !markets.contains(&key.market)
                        && getters.amounts.get(key).is_none_or(|amounts| amounts.debt!=0||amounts.collateral!=0))
                    || getters.configs.keys().any(|market| !markets.contains(market))
                    || getters.reconciliation.user_offsets.keys().any(|market| !markets.contains(market))
                    || getters.reconciliation.next_market.is_some_and(|market| !markets.contains(&market)))
                || getters.reconciliation.last_position.is_some_and(|key| key.pool != self.spec.address)
                || getters.amounts.keys().any(|key| !getters.keys.contains(key))
                || getters.markets.is_none() && !getters.amounts.keys().eq(getters.keys.iter().take(getters.amounts.len()))
                || getters.markets.is_some() && getters.pool.is_none()
                || getters.globals.is_some() && (getters.markets.is_none() || getters.user_pages_remaining != 0)
                || !getters.configs.is_empty() && getters.globals.is_none()
                || getters.oracle.is_some() && (getters.globals.is_none()
                    || getters.markets.as_ref().is_none_or(|markets| markets.len() != getters.configs.len()))
                || !getters.amounts.is_empty() && getters.pool.is_none()
            {
                return Err(invalid("invalid staged getters"));
            }
        }
        Ok(())
    }

    /// A bounded status record suitable for progress reporting, without getter data.
    pub fn status(&self) -> DiscoveryStatus {
        DiscoveryStatus { generation: self.generation, from_block: self.from_block,
            through: self.through, target: self.target, phase: self.phase,
            discovered_positions: self.positions.len(), discovered_markets: self.markets.len(),
            rebuilding: self.rebuilding }
    }
}

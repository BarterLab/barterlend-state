//! Versioned snapshots and reconciliation cursors for restarting an engine.

use std::collections::BTreeMap;

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, abi::{ABI_FINGERPRINT, PROTOCOL_VERSION}, risk::evaluate_risk, types::*};

/// Supported restart-state schema version.
pub const CHECKPOINT_SCHEMA: u32 = 1;

/// Durable rotating offsets for known positions and market users.
/// Pagination traverses the complete list, not just its appended tail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationCursor {
    /// Next known market for round-robin user scanning.
    pub next_market: Option<u64>,
    /// Next user-page offset per known market; scanning wraps after the end.
    pub user_offsets: BTreeMap<u64, u64>,
    /// Last sorted key in rotating position scans, not a discovery floor.
    pub last_position: Option<PositionKey>,
}

/// Snapshot and reconciliation cursors. Synchronize after restoring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// File schema compatible with this crate.
    pub schema_version: u32,
    /// Complete last state of one pool.
    pub snapshot: StateSnapshot,
    /// Restart positions for periodic scans.
    pub reconciliation: ReconciliationCursor,
    /// Checksum for detecting accidental corruption.
    pub checksum: B256,
}

impl Checkpoint {
    /// Builds checksum and validates snapshot/cursors without disk or RPC operations.
    pub fn new(snapshot: StateSnapshot, reconciliation: ReconciliationCursor) -> Result<Self> {
        let checksum = checksum(&snapshot, &reconciliation)?;
        let checkpoint = Self { schema_version: CHECKPOINT_SCHEMA, snapshot, reconciliation, checksum };
        checkpoint.validate(None, None)?;
        Ok(checkpoint)
    }

    /// Checks schema, checksum, ABI, structure and calculated risk without RPC.
    /// Optional identity/spec are checked against the stored deployment.
    pub fn validate(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<()> {
        let invalid = |message: &str| Error::Checkpoint(message.into());
        if self.schema_version != CHECKPOINT_SCHEMA {
            return Err(invalid("unsupported checkpoint schema"));
        }
        if checksum(&self.snapshot, &self.reconciliation)? != self.checksum {
            return Err(invalid("checksum mismatch"));
        }
        let snapshot = &self.snapshot;
        if snapshot.abi_fingerprint != ABI_FINGERPRINT {
            return Err(invalid("ABI fingerprint mismatch"));
        }
        if snapshot.pool.version != PROTOCOL_VERSION || snapshot.spec.expected_version != PROTOCOL_VERSION {
            return Err(Error::UnsupportedVersion(snapshot.pool.version));
        }
        if snapshot.identity.chain_id == 0 || snapshot.identity.genesis_hash.is_zero() {
            return Err(invalid("missing chain identity"));
        }
        if identity.is_some_and(|expected| *expected != snapshot.identity) {
            return Err(Error::IdentityMismatch("checkpoint chain/genesis".into()));
        }
        if let Some(expected) = spec {
            if expected.address != snapshot.spec.address
                || expected.deployment_block != snapshot.spec.deployment_block
                || expected.expected_version != snapshot.spec.expected_version
                || expected.deployment_block_hash.is_some_and(|hash| Some(hash) != snapshot.spec.deployment_block_hash)
            {
                return Err(Error::IdentityMismatch("checkpoint PoolSpec".into()));
            }
        }
        if snapshot.pool.address != snapshot.spec.address || snapshot.pool.address.is_zero()
            || snapshot.pool.asset.address.is_zero() || snapshot.pool.collateral.address.is_zero()
            || snapshot.pool.oracle.is_zero() || snapshot.pool.code_hash.is_zero()
        {
            return Err(invalid("invalid pool descriptor"));
        }
        if snapshot.spec.deployment_block_hash.is_none_or(|hash| hash.is_zero())
            || snapshot.as_of.number < snapshot.spec.deployment_block || snapshot.as_of.hash.is_zero()
            || snapshot.epoch == 0
        {
            return Err(invalid("invalid block/deployment/epoch"));
        }
        for (key, record) in &snapshot.positions {
            if *key != record.key || key.pool != snapshot.pool.address
                || (record.amounts.debt == 0 && record.amounts.collateral == 0)
                || record.last_changed.number > snapshot.as_of.number
                || record.last_changed.timestamp > snapshot.as_of.timestamp
                || record.last_changed.commitment != snapshot.as_of.commitment
                || record.last_changed.hash.is_zero()
            {
                return Err(invalid("invalid position record"));
            }
            let market = snapshot.markets.get(&key.market).ok_or_else(|| invalid("position without market config"))?;
            let expected = evaluate_risk(&record.amounts, key.market, &snapshot.globals, market,
                &snapshot.oracle, snapshot.as_of.timestamp);
            if expected != record.risk {
                return Err(invalid("position risk differs from deterministic model"));
            }
        }
        if self.reconciliation.user_offsets.keys().any(|market| !snapshot.markets.contains_key(market))
            || self.reconciliation.next_market.is_some_and(|market| !snapshot.markets.contains_key(&market))
            || self.reconciliation.last_position.is_some_and(|key| key.pool != snapshot.pool.address)
        {
            return Err(invalid("invalid reconciliation cursor"));
        }
        Ok(())
    }
}

fn checksum(snapshot: &StateSnapshot, reconciliation: &ReconciliationCursor) -> Result<B256> {
    crate::encoding::digest(&(snapshot, reconciliation))
}

//! Synchronizes one pool using events and block-pinned getters.
//! Failed reads retain the previous snapshot. Checkpoint persistence is explicit.

/// Optional scheduled-market census and explicitly scoped restart data.
pub mod current;

use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::Duration};

use crate::{Checkpoint, Error, Result, abi::{ABI_FINGERPRINT, PROTOCOL_VERSION},
    checkpoint::ReconciliationCursor, events::{Invalidations, decode_invalidation, normalize_logs},
    discovery::{DISCOVERY_SCHEMA, DiscoveryCheckpoint, DiscoveryPhase, GetterCheckpoint, SyncStepOutcome},
    reader::StateReader, risk::{evaluate_risk, validate_oracle_status},
    rpc::{RpcErrorKind, classify_rpc_error}, types::*};

/// Head selection, discovery limits and periodic refresh settings for one pool.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Head tag for bootstrap/sync; default Finalized.
    pub commitment: Commitment,
    /// Trusted chain ID and genesis. None accepts the first observed network identity without an independent pin.
    pub expected_identity: Option<ChainIdentity>,
    /// Initial inclusive eth_getLogs range; align with RpcConfig.max_log_range and provider limits.
    pub log_chunk_size: u64,
    /// reader.logs call budget per sync_step or explicit-target operation, including subdivision.
    /// Internal transport retries have their own cap.
    pub max_log_calls: usize,
    /// Maximum returned logs per sync_step or explicit-target operation before normalization.
    pub max_logs_per_sync: usize,
    /// Maximum block range scanned in one step or explicit-target operation.
    pub max_blocks_per_sync: u64,
    /// Maximum nonempty borrower records.
    pub max_positions: usize,
    /// Maximum known markets/configs.
    pub max_markets: usize,
    /// Concurrent key/market-read limit while building staged state.
    pub read_batch_size: usize,
    /// Reuse immutable pool metadata and configuration between event invalidations.
    /// Enable only for the supported v5 deployment and a source returning complete logs.
    pub event_driven_refresh: bool,
    /// Minimum block-time interval between defensive position/user scans. Zero runs each sync.
    /// Event-invalidated positions and oracle/time risk updates are never delayed by this interval.
    pub reconciliation_interval: Duration,
    /// Known-position rotating refresh count per reconciliation round;0 disables this scan.
    pub reconcile_positions_per_sync: usize,
    /// Known-market user pages per reconciliation round;0 disables pagination.
    pub reconcile_user_pages_per_sync: usize,
    /// Users per page; the cursor traverses the full list, not just the appended tail.
    pub reconcile_user_page_size: u64,
    /// Timeout for one sync_step or explicit-target operation, and for the entire
    /// bootstrap()/sync() convenience call including all of its bounded steps.
    pub operation_timeout: Duration,
    /// Extra fresh getter-target attempts per sync_step after execution-state pruning; maximum8.
    pub max_reanchors: usize,
    /// Fully verified history chunks per sync_step. A finite step returns Progress;
    /// persist discovery_checkpoint separately and call again to continue.
    pub history_chunks_per_step: usize,
    /// Completed getter operations/batches per sync_step. Each batch is retained
    /// independently so timeout or restart resumes at the next unfinished batch.
    pub getter_batches_per_step: usize,
    /// Maximum sync_step calls made by bootstrap()/sync() before returning ResourceLimit.
    /// Completed discovery survives the error; call again or use sync_step directly.
    pub max_sync_steps: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            commitment: Commitment::Finalized,
            expected_identity: None,
            log_chunk_size: 100,
            max_log_calls: 50_000,
            max_logs_per_sync: 1_000_000,
            max_blocks_per_sync: 5_000_000,
            max_positions: 1_000_000,
            max_markets: 20_000,
            read_batch_size: 128,
            event_driven_refresh: false,
            reconciliation_interval: Duration::ZERO,
            reconcile_positions_per_sync: 64,
            reconcile_user_pages_per_sync: 2,
            reconcile_user_page_size: 128,
            operation_timeout: Duration::from_secs(600),
            max_reanchors: 2,
            history_chunks_per_step: 8,
            getter_batches_per_step: 64,
            max_sync_steps: 8,
        }
    }
}

impl EngineConfig {
    /// Validates positive budgets and bounded reanchors without RPC.
    pub fn validate(&self) -> Result<()> {
        if self.log_chunk_size == 0 || self.max_log_calls == 0 || self.max_logs_per_sync == 0
            || self.max_blocks_per_sync == 0 || self.max_positions == 0 || self.max_markets == 0
            || self.read_batch_size == 0 || self.reconcile_user_page_size == 0
            || self.operation_timeout.is_zero() || self.max_reanchors > 8
            || self.history_chunks_per_step == 0 || self.history_chunks_per_step > 1024
            || self.getter_batches_per_step == 0 || self.getter_batches_per_step > 1024
            || self.max_sync_steps == 0 || self.max_sync_steps > 1024
        {
            return Err(Error::InvalidConfig("engine resource limits must be positive; reanchors <= 8, history chunks, getter batches and convenience steps <= 1024".into()));
        }
        Ok(())
    }
}

/// Sequential engine for one pool with its own snapshot, epoch and reconciliation cursors.
/// Use separate engines and checkpoint paths for multiple pools.
pub struct StateEngine<R: StateReader + ?Sized> {
    reader: Arc<R>,
    spec: PoolSpec,
    config: EngineConfig,
    snapshot: Option<Arc<StateSnapshot>>,
    last_reconciliation_timestamp: Option<u64>,
    metadata_verified: bool,
    restored_metadata_check: Option<RestoredMetadataCheck>,
    reconciliation: ReconciliationCursor,
    discovery: Option<DiscoveryCheckpoint>,
    current_policy: Option<current::CurrentStateConfig>,
    current_census: Option<current::CurrentCensus>,
}

// A verified HTTP scope cannot publish intermediate state. Drop also covers an
// external timeout/cancellation while its final canonical checks are in flight.
struct StepRollback<'a, R: StateReader + ?Sized> {
    engine: &'a mut StateEngine<R>, committed: bool,
    snapshot: Option<Arc<StateSnapshot>>, discovery: Option<DiscoveryCheckpoint>,
    census: Option<current::CurrentCensus>, reconciliation: ReconciliationCursor,
    last_reconciliation: Option<u64>, metadata: bool, restored: Option<RestoredMetadataCheck>,
}
impl<'a, R: StateReader + ?Sized> StepRollback<'a,R> {
    fn new(engine: &'a mut StateEngine<R>) -> Self {
        Self { snapshot:engine.snapshot.clone(), discovery:engine.discovery.clone(), census:engine.current_census.clone(),
            reconciliation:engine.reconciliation.clone(), last_reconciliation:engine.last_reconciliation_timestamp,
            metadata:engine.metadata_verified, restored:engine.restored_metadata_check, engine, committed:false }
    }
}
impl<R: StateReader + ?Sized> Drop for StepRollback<'_,R> {
    fn drop(&mut self) {
        if !self.committed {
            self.engine.snapshot=self.snapshot.take(); self.engine.discovery=self.discovery.take();
            self.engine.current_census=self.census.take(); self.engine.reconciliation=self.reconciliation.clone();
            self.engine.last_reconciliation_timestamp=self.last_reconciliation;
            self.engine.metadata_verified=self.metadata; self.engine.restored_metadata_check=self.restored;
        }
    }
}

// Revalidate only metadata already present in restored getter batches. Keep all
// borrower amounts and bounded read progress; restarting must not replay them.
#[derive(Clone, Copy)]
enum RestoredMetadataCheck { Pool, Globals, Configs(Option<u64>) }

/// Owned discovery export whose expensive checksum/validation can run on a blocking worker.
/// Captures one immutable base snapshot. Creating it copies only unfinished discovery
/// and reconciliation cursors; large unfinished getter maps still require that bounded copy.
pub struct DiscoveryCheckpointBuilder {
    progress: DiscoveryCheckpoint,
    snapshot: Option<Arc<StateSnapshot>>,
    reconciliation: ReconciliationCursor,
    expected_identity: Option<ChainIdentity>,
    spec: PoolSpec,
}

impl DiscoveryCheckpointBuilder {
    /// Materializes and validates the captured export, without RPC or filesystem writes.
    pub fn build(self) -> Result<DiscoveryCheckpoint> {
        let mut progress = self.progress;
        progress.base_checksum = self.snapshot.map(|snapshot|
            Checkpoint::new((*snapshot).clone(), self.reconciliation).map(|value| value.checksum)).transpose()?;
        progress.seal()?;
        progress.validate(self.expected_identity.as_ref(), Some(&self.spec))?;
        Ok(progress)
    }
}

impl<R: StateReader + ?Sized> StateEngine<R> {
    /// Creates an empty engine without RPC.
    ///
    /// `PoolSpec` must start at genesis or a verified pool creation block.
    pub fn new(reader: Arc<R>, spec: PoolSpec, config: EngineConfig) -> Result<Self> {
        config.validate()?;
        if spec.address.is_zero() {
            return Err(Error::InvalidConfig("pool address is zero".into()));
        }
        if spec.expected_version != PROTOCOL_VERSION {
            return Err(Error::UnsupportedVersion(spec.expected_version));
        }
        Ok(Self { reader, spec, config, snapshot: None, last_reconciliation_timestamp: None, metadata_verified: false, restored_metadata_check: None, reconciliation: ReconciliationCursor::default(), discovery: None,
            current_policy: None, current_census: None })
    }

    /// Validates and restores local checkpoint data without RPC.
    ///
    /// The checkpoint commitment must match the configuration. Synchronize before
    /// using restored data as current state.
    pub fn restore(reader: Arc<R>, spec: PoolSpec, config: EngineConfig, checkpoint: Checkpoint) -> Result<Self> {
        let mut engine = Self::new(reader, spec, config)?;
        checkpoint.validate(engine.config.expected_identity.as_ref(), Some(&engine.spec))?;
        if checkpoint.snapshot.as_of.commitment != engine.config.commitment {
            return Err(Error::Checkpoint("checkpoint commitment differs from engine".into()));
        }
        engine.check_limits(checkpoint.snapshot.positions.len(), checkpoint.snapshot.markets.len())?;
        engine.snapshot = Some(Arc::new(checkpoint.snapshot));
        engine.reconciliation = checkpoint.reconciliation;
        Ok(engine)
    }

    /// Last local snapshot, or None before bootstrap.
    /// After restore or failure it can be old and not currently live-verified.
    pub fn snapshot(&self) -> Option<&StateSnapshot> { self.snapshot.as_deref() }
    /// Shares the immutable completed snapshot without copying its position map.
    pub fn snapshot_shared(&self) -> Option<Arc<StateSnapshot>> { self.snapshot.clone() }
    /// Durable rotating offsets belonging to the current completed snapshot.
    pub fn reconciliation_cursor(&self) -> &ReconciliationCursor { &self.reconciliation }
    /// Shared state source; Arc clones can share the RPC limiter.
    pub fn reader(&self) -> &Arc<R> { &self.reader }
    /// Immutable policy for this engine.
    pub fn config(&self) -> &EngineConfig { &self.config }

    /// Builds validated restart data without writing to disk.
    /// Fails before bootstrap. Persist it before advertising a durable restart cursor.
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        Checkpoint::new(self.snapshot().cloned().ok_or_else(|| Error::Checkpoint("engine is not bootstrapped".into()))?,
            self.reconciliation.clone())
    }

    /// Returns restartable discovery data, including chunks completed before an error.
    /// Persist separately from the complete state checkpoint.
    pub fn discovery_checkpoint(&self) -> Result<Option<DiscoveryCheckpoint>> {
        self.discovery_checkpoint_builder().map(DiscoveryCheckpointBuilder::build).transpose()
    }

    /// Captures unfinished progress for `spawn_blocking(move || builder.build())`.
    /// Later engine updates cannot change the captured base-state binding.
    pub fn discovery_checkpoint_builder(&self) -> Option<DiscoveryCheckpointBuilder> {
        self.discovery.as_ref().map(|progress| DiscoveryCheckpointBuilder {
            progress: progress.clone(), snapshot: self.snapshot.clone(), reconciliation: self.reconciliation.clone(),
            expected_identity: self.config.expected_identity, spec: self.spec,
        })
    }

    /// Restores local discovery without RPC. False means valid but obsolete progress
    /// bound to another state checksum (for example a crash after saving newer state).
    /// Foreign identity/deployment or corrupt data remain errors. sync_step verifies anchors.
    pub fn restore_discovery(&mut self, progress: DiscoveryCheckpoint) -> Result<bool> {
        progress.validate(self.config.expected_identity.as_ref(), Some(&self.spec))?;
        if self.snapshot.as_ref().is_some_and(|snapshot| snapshot.identity != progress.identity || snapshot.spec != progress.spec) {
            return Err(Error::IdentityMismatch("discovery differs from restored chain/deployment".into()));
        }
        if progress.target.commitment != self.config.commitment {
            return Err(Error::Checkpoint("discovery commitment differs from engine".into()));
        }
        let base = self.snapshot.as_ref().map(|_| self.checkpoint().map(|checkpoint| checkpoint.checksum)).transpose()?;
        if progress.base_checksum != base {
            self.discovery = None;
            return Ok(false);
        }
        let expected_epoch = if progress.rebuilding || self.snapshot.is_none() { self.next_epoch()? }
            else { self.snapshot.as_ref().expect("snapshot exists").epoch };
        let expected_from = if progress.rebuilding || self.snapshot.is_none() { self.spec.deployment_block }
            else { self.snapshot.as_ref().expect("snapshot exists").as_of.number.checked_add(1)
                .ok_or_else(|| Error::Arithmetic("state height exhausted".into()))? };
        if progress.epoch != expected_epoch || progress.from_block != expected_from {
            return Err(Error::Checkpoint("discovery epoch/history base differs from engine".into()));
        }
        self.check_limits(progress.positions.len(), 0)?;
        if let Some(getters) = &progress.getters {
            self.check_limits(getters.keys.len(), getters.markets.as_ref().map_or(0, BTreeSet::len))?;
        }
        self.restored_metadata_check = if self.config.event_driven_refresh && progress.getters.is_some() {
            Some(RestoredMetadataCheck::Pool)
        } else { None };
        self.discovery = Some(progress);
        Ok(true)
    }

    /// Performs bounded, restartable log discovery followed by a fresh pinned getter stage.
    ///
    /// `Progress` contains no snapshot. Save `discovery_checkpoint()` even after an
    /// error to retain completed chunks. Final getters all use one block.
    /// Restored state verifies its anchors and scans only missed logs.
    pub async fn sync_step(&mut self) -> Result<SyncStepOutcome> {
        let timeout = self.config.operation_timeout;
        if self.current_policy.is_none() || !self.reader.supports_verified_sessions() {
            return tokio::time::timeout(timeout, self.step_inner()).await
                .map_err(|_| Error::Transport("bounded state step timed out; save discovery progress and continue".into()))?;
        }
        let mut staged = StepRollback::new(self);
        let result = tokio::time::timeout(timeout, crate::reader::verified_session(staged.engine.step_inner())).await
            .map_err(|_| Error::Transport("bounded verified state step timed out; preceding progress retained".into()))?;
        if result.is_ok() { staged.committed = true; }
        result
    }

    async fn start_discovery(&mut self, generation: u64, force_full: bool) -> Result<Option<SyncOutcome>> {
        self.restored_metadata_check = None;
        let identity = self.assert_identity().await?;
        let target = self.reader.head(self.config.commitment).await?;
        self.validate_target(&target)?;
        let old = self.snapshot.as_ref();
        // A current-state rebuild does not depend on retention of the old
        // checkpoint header. Decide the long-gap path before requesting it.
        let long_gap = self.current_policy.as_ref().is_some_and(|policy| old.is_some_and(|state|
            target.number.saturating_sub(state.as_of.number) > policy.max_event_catchup_blocks));
        let rebuilding = if let Some(old) = old {
            force_full || long_gap || self.reader.block(old.as_of.number, self.config.commitment).await? != Some(old.as_of)
        } else { false };
        let fast = self.config.event_driven_refresh && old.is_some() && !rebuilding;
        let spec = if fast { old.expect("existing incremental snapshot").spec } else {
            self.assert_target(&target).await?;
            self.anchored_spec(&target).await?
        };
        if let Some(old) = old {
            if !rebuilding && target.timestamp < old.as_of.timestamp {
                return Err(Error::InvalidConfig("canonical target time precedes saved state".into()));
            }
            if !rebuilding && target.number < old.as_of.number {
                return Err(Error::InvalidConfig("sync target precedes canonical cursor".into()));
            }
            if !rebuilding && target.number == old.as_of.number {
                self.discovery = None;
                return Ok(Some(SyncOutcome::Unchanged { epoch: old.epoch, as_of: old.as_of }));
            }
        }
        if self.current_policy.as_ref().is_some_and(|policy| rebuilding || old.is_none()
            || target.number.saturating_sub(old.unwrap().as_of.number) > policy.max_event_catchup_blocks)
        {
            self.begin_current_census(identity, spec, target, generation)?;
            return Ok(None);
        }
        let from_block = if rebuilding || old.is_none() { spec.deployment_block }
            else { old.expect("snapshot exists").as_of.number.checked_add(1)
                .ok_or_else(|| Error::Arithmetic("state height exhausted".into()))? };
        let epoch = if rebuilding || old.is_none() { self.next_epoch()? }
            else { old.expect("snapshot exists").epoch };
        // The ordinary checkpoint binding is materialized lazily on export.
        let base_checksum = None;
        let mut progress = DiscoveryCheckpoint { schema_version: DISCOVERY_SCHEMA, identity, spec,
            abi_fingerprint: ABI_FINGERPRINT, base_checksum, epoch, generation, rebuilding,
            from_block, through: None, target, phase: if old.is_some() && !rebuilding && self.config.event_driven_refresh { DiscoveryPhase::Snapshot } else { DiscoveryPhase::Census },
            refresh_all_config: Some(rebuilding || old.is_none()),
            positions: BTreeSet::new(), markets: BTreeSet::new(), getters: None, checksum: alloy_primitives::B256::ZERO };
        progress.seal()?;
        self.discovery = Some(progress);
        Ok(None)
    }

    async fn step_inner(&mut self) -> Result<SyncStepOutcome> {
        if self.current_census.is_some() { return self.advance_current_census().await; }
        let started = self.discovery.is_none();
        if started {
            if let Some(outcome) = self.start_discovery(1, false).await? { return Ok(SyncStepOutcome::Complete(outcome)); }
        }
        if self.current_census.is_some() { return self.advance_current_census().await; }
        if !started {
        let identity = self.assert_identity().await?;
        let progress = self.discovery.as_ref().expect("discovery initialized");
        if identity != progress.identity {
            return Err(Error::IdentityMismatch("discovery RPC chain/genesis changed".into()));
        }
        let spec = self.anchored_spec(&progress.target).await?;
        if spec != progress.spec { return Err(Error::IdentityMismatch("discovery deployment anchor changed".into())); }
        let old_valid = if progress.rebuilding && self.current_policy.is_some() { true }
            else if let Some(old) = self.snapshot.as_ref() {
                self.reader.block(old.as_of.number, self.config.commitment).await? == Some(old.as_of)
            } else { true };
        let cursor_valid = if let Some(cursor) = progress.through {
            self.reader.block(cursor.number, cursor.commitment).await? == Some(cursor)
        } else { true };
        if !cursor_valid || !old_valid && !progress.rebuilding {
            let generation = progress.generation.checked_add(1)
                .ok_or_else(|| Error::Arithmetic("discovery generation exhausted".into()))?;
            let force_full = progress.rebuilding;
            self.discovery = None;
            if let Some(outcome) = self.start_discovery(generation, force_full).await? { return Ok(SyncStepOutcome::Complete(outcome)); }
        } else if self.reader.block(progress.target.number, progress.target.commitment).await? != Some(progress.target) {
            // An unscanned endpoint can change while the certified prefix survives.
            // Keep that prefix and scan the new canonical suffix; never keep an orphan cursor.
            let target = self.reader.head(self.config.commitment).await?;
            self.assert_target(&target).await?;
            if progress.through.is_some_and(|cursor| target.number < cursor.number) {
                return Err(Error::NonCanonical(target.number));
            }
            if !progress.rebuilding {
                if let Some(old) = self.snapshot.as_ref() {
                    if target.number < old.as_of.number {
                        return Err(Error::InvalidConfig("sync target precedes canonical cursor".into()));
                    }
                    if target == old.as_of {
                        let outcome = SyncOutcome::Unchanged { epoch: old.epoch, as_of: old.as_of };
                        self.discovery = None;
                        return Ok(SyncStepOutcome::Complete(outcome));
                    }
                }
            }
            let mut staged = progress.clone();
            staged.target = target;
            staged.getters = None;
            staged.seal()?;
            self.discovery = Some(staged);
        }
        }
        let mut chunks = 0;
        let mut calls = 0;
        let mut logs = 0;
        let mut blocks = 0_u64;
        let mut reanchors = 0;
        let mut getter_batches = 0;
        loop {
            if self.current_census.is_some() { return self.advance_current_census().await; }
            let progress = self.discovery.as_ref().expect("discovery initialized");
            // A reorg/reanchor can widen a previously short incremental tail.
            // The opt-in current-state engine must never fall back to old logs.
            if self.current_policy.as_ref().is_some_and(|policy| progress.through != Some(progress.target)
                && progress.target.number.saturating_sub(progress.through.map_or(progress.from_block,
                    |at| at.number.saturating_add(1))).saturating_add(1) > policy.max_event_catchup_blocks)
            {
                self.discovery = None;
                self.start_discovery(1, true).await?;
                return self.advance_current_census().await;
            }
            let next = match progress.through {
                Some(cursor) if cursor.number == progress.target.number => None,
                Some(cursor) => Some(cursor.number.checked_add(1).ok_or_else(|| Error::Arithmetic("history height exhausted".into()))?),
                None => Some(progress.from_block),
            };
            if let Some(left) = next {
                if chunks >= self.config.history_chunks_per_step || blocks >= self.config.max_blocks_per_sync
                    || calls >= self.config.max_log_calls || logs >= self.config.max_logs_per_sync
                {
                    return Ok(SyncStepOutcome::Progress(progress.status()));
                }
                let width = self.config.log_chunk_size.min(self.config.max_blocks_per_sync - blocks);
                let right = left.saturating_add(width - 1).min(progress.target.number);
                let end = if self.config.event_driven_refresh && right == progress.target.number {
                    progress.target
                } else { self.reader.block(right, self.config.commitment).await?
                    .ok_or_else(|| Error::IncompleteHistory(format!("historical header {right} unavailable; select an RPC retaining census headers")))? };
                let previous = progress.through;
                let (invalidations, used_calls, used_logs) = self.collect_invalidations_bounded(left, right,
                    self.config.commitment, self.config.max_log_calls - calls, self.config.max_logs_per_sync - logs).await?;
                self.assert_canonical(&end).await?;
                if let Some(previous) = previous { self.assert_canonical(&previous).await?; }
                if !self.config.event_driven_refresh && self.assert_identity().await? != progress.identity {
                    return Err(Error::IdentityMismatch("discovery RPC chain/genesis changed".into()));
                }
                // The final getter publication checks network identity again. An
                // unfinished prefix is revalidated before its next continuation.
                let mut staged = self.discovery.as_ref().expect("discovery initialized").clone();
                staged.refresh_all_config = Some(staged.refresh_all_config.unwrap_or(true) || invalidations.config_changed);
                staged.positions.extend(invalidations.positions);
                staged.markets.extend(invalidations.markets);
                self.check_limits(staged.positions.len(), 0)?;
                staged.through = Some(end);
                // A reorg of the not-yet-scanned endpoint is harmless only while
                // this fully checked prefix remains canonical. Replace its header.
                if right == staged.target.number { staged.target = end; }
                staged.seal()?;
                self.discovery = Some(staged);
                chunks += 1; calls += used_calls; logs += used_logs; blocks += right - left + 1;
                continue;
            }
            if progress.phase == DiscoveryPhase::Census {
                let target = self.reader.head(self.config.commitment).await?;
                self.assert_target(&target).await?;
                let cursor = progress.through.expect("complete census has a cursor");
                self.assert_canonical(&cursor).await?;
                if target.number < cursor.number { return Err(Error::NonCanonical(cursor.number)); }
                let mut staged = progress.clone();
                staged.phase = DiscoveryPhase::Snapshot;
                staged.target = target;
                staged.seal()?;
                self.discovery = Some(staged);
                continue;
            }
            let at = progress.target;
            if getter_batches >= self.config.getter_batches_per_step {
                return Ok(SyncStepOutcome::Progress(progress.status()));
            }
            getter_batches += 1;
            let result = match self.advance_getters().await {
                Ok(false) => continue,
                Ok(true) => self.finish_discovery(self.discovery.as_ref().expect("discovery initialized")).await,
                Err(error) => Err(error),
            };
            match result {
                Ok((outcome, snapshot, cursor)) => {
                    if self.reconciliation_due(snapshot.as_of) { self.last_reconciliation_timestamp = Some(snapshot.as_of.timestamp); }
                    self.snapshot = Some(Arc::new(snapshot));
                    self.metadata_verified = true;
                    self.restored_metadata_check = None;
                    self.reconciliation = cursor;
                    self.discovery = None;
                    return Ok(SyncStepOutcome::Complete(outcome));
                }
                Err(Error::MissingBlock(number)) if number == at.number && reanchors < self.config.max_reanchors => {
                    if self.current_policy.is_some() {
                        self.discovery = None;
                        self.start_discovery(1, true).await?;
                        return self.advance_current_census().await;
                    }
                    // Execution state disappeared, but the certified log prefix is
                    // retained. Only the new tail and the entire getter stage repeat.
                    self.assert_canonical(&at).await?;
                    let target = self.reader.head(self.config.commitment).await?;
                    self.assert_target(&target).await?;
                    if target.number <= at.number {
                        return Err(Error::MissingBlock(at.number));
                    }
                    let mut staged = self.discovery.as_ref().expect("discovery initialized").clone();
                    staged.target = target;
                    staged.getters = None;
                    staged.seal()?;
                    self.discovery = Some(staged);
                    reanchors += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    // Each successful await commits one unit to discovery before another await.
    // Cancellation loses only the in-flight unit, never previously completed batches.
    async fn advance_getters(&mut self) -> Result<bool> {
        let progress = self.discovery.as_ref().expect("discovery initialized");
        let at = progress.target;
        if progress.through != Some(at) || progress.phase != DiscoveryPhase::Snapshot {
            return Err(Error::Checkpoint("discovery has not reached its getter target".into()));
        }
        if progress.getters.is_none() {
            self.restored_metadata_check = None;
            let old = self.snapshot.as_ref().filter(|_| !progress.rebuilding);
            let mut cursor = if old.is_some() { self.reconciliation.clone() } else { ReconciliationCursor::default() };
            let mut keys = progress.positions.clone();
            if let Some(old) = old {
                if self.reconciliation_due(at) {
                    for key in rotating_keys(&old.positions, cursor.last_position, self.config.reconcile_positions_per_sync) {
                        keys.insert(key); cursor.last_position = Some(key);
                    }
                }
            }
            self.check_limits(keys.len(), 0)?;
            let pool = old.filter(|_| self.config.event_driven_refresh && self.metadata_verified).map(|old| old.pool.clone());
            let getters = GetterCheckpoint { pool, markets: None, active_markets: None, keys, reconciliation: cursor,
                user_pages_remaining: if old.is_some() && self.reconciliation_due(at) { self.config.reconcile_user_pages_per_sync } else { 0 },
                globals: None, batch_size: self.config.read_batch_size, pending_batch_size: None,
                configs: BTreeMap::new(), oracle: None, amounts: BTreeMap::new() };
            let progress = self.discovery.as_mut().expect("discovery initialized");
            progress.getters = Some(getters);
        }
        if self.restored_metadata_check.is_some() {
            self.recheck_restored_metadata(at).await?;
            return Ok(false);
        }
        let getters = self.discovery.as_mut().unwrap().getters.as_mut().unwrap();
        if let Some(interrupted) = getters.pending_batch_size.take() {
            getters.batch_size = getters.batch_size.min((interrupted / 2).max(1));
        }
        getters.batch_size = getters.batch_size.min(self.config.read_batch_size);
        let progress = self.discovery.as_ref().expect("discovery initialized");
        let getters = progress.getters.as_ref().expect("getter stage initialized");
        if getters.pool.is_none() {
            let pool = self.reader.describe_pool(&progress.spec, &at).await?;
            if pool.address != self.spec.address || pool.version != self.spec.expected_version
                || pool.asset.address.is_zero() || pool.collateral.address.is_zero()
                || pool.oracle.is_zero() || pool.code_hash.is_zero()
            { return Err(Error::Decode("invalid pool descriptor".into())); }
            if self.snapshot.as_ref().is_some_and(|old| old.pool != pool) {
                return Err(Error::IdentityMismatch("pool descriptor/code changed".into()));
            }
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().pool = Some(pool);
        } else if getters.amounts.len() < getters.keys.len() {
            let start = getters.amounts.last_key_value().map_or(std::ops::Bound::Unbounded,
                |(key, _)| std::ops::Bound::Excluded(*key));
            // Reconciliation can insert a key before an already read key.
            // Keep the cheap prefix walk during bulk discovery; after market
            // selection fill any holes introduced by bounded user pages.
            let batch: Vec<_> = if getters.markets.is_some() {
                getters.keys.iter().filter(|key| !getters.amounts.contains_key(key))
                    .take(getters.batch_size).copied().collect()
            } else {
                getters.keys.range((start, std::ops::Bound::Unbounded))
                    .take(getters.batch_size).copied().collect()
            };
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().pending_batch_size = Some(batch.len());
            let values = self.reader.read_positions(self.spec.address, &batch, &at).await?;
            if values.len() != batch.len() || batch.iter().any(|key| !values.contains_key(key)) {
                return Err(Error::Decode("primitive read omitted or added position keys".into()));
            }
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().amounts.extend(values);
        } else if getters.markets.is_none() {
            let mut markets = position_markets(self.snapshot.as_ref().filter(|_| !progress.rebuilding).map(Arc::as_ref), &getters.amounts);
            let active: BTreeSet<_> = self.reader.active_markets(self.spec.address, &at).await?.into_iter().collect();
            markets.extend(active.iter().copied());
            self.check_limits(getters.keys.len(), markets.len())?;
            let getters = self.discovery.as_mut().unwrap().getters.as_mut().unwrap();
            if markets.is_empty() { getters.user_pages_remaining = 0; }
            getters.reconciliation.user_offsets.retain(|market,_| markets.contains(market));
            if getters.reconciliation.next_market.is_some_and(|market| !markets.contains(&market)) { getters.reconciliation.next_market = None; }
            getters.active_markets = Some(active); getters.markets = Some(markets);
        } else if getters.user_pages_remaining > 0 {
            let markets = getters.markets.as_ref().unwrap();
            let market = getters.reconciliation.next_market.and_then(|next| markets.range(next..).next().copied())
                .unwrap_or(*markets.first().expect("nonempty markets for pages"));
            let start = *getters.reconciliation.user_offsets.get(&market).unwrap_or(&0);
            let users = self.reader.market_users(self.spec.address, market, start, self.config.reconcile_user_page_size, &at).await?;
            if users.len() as u64 > self.config.reconcile_user_page_size || users.iter().any(|user| user.is_zero()) {
                return Err(Error::Decode("invalid market user page".into()));
            }
            let next_offset = if (users.len() as u64) < self.config.reconcile_user_page_size { 0 }
                else { start.checked_add(users.len() as u64).ok_or_else(|| Error::Arithmetic("user page offset overflow".into()))? };
            let next_market = markets.range((std::ops::Bound::Excluded(market), std::ops::Bound::Unbounded)).next().copied()
                .unwrap_or(*markets.first().unwrap());
            let mut keys = getters.keys.clone();
            keys.extend(users.into_iter().map(|borrower| PositionKey { pool: self.spec.address, market, borrower }));
            self.check_limits(keys.len(), markets.len())?;
            let getters = self.discovery.as_mut().unwrap().getters.as_mut().unwrap();
            getters.keys = keys; getters.reconciliation.user_offsets.insert(market, next_offset);
            getters.reconciliation.next_market = Some(next_market); getters.user_pages_remaining -= 1;
        } else if getters.globals.is_none() {
            let reuse = self.config.event_driven_refresh && !progress.rebuilding
                && !progress.refresh_all_config.unwrap_or(true) && self.metadata_verified;
            let old = self.snapshot.as_ref().filter(|_| reuse);
            let globals = if let Some(old) = old { old.globals } else { self.reader.read_globals(self.spec.address, &at).await? };
            // Activating a market snapshots the same effective configuration;
            // only AdminConfigUpdated can change existing effective values.
            // New markets are absent from this map and still get fresh reads.
            let configs = old.map(|old| old.markets.iter().filter(|(m,_)| getters.markets.as_ref().unwrap().contains(m)).map(|(m,c)| (*m,*c)).collect()).unwrap_or_default();
            let getters = self.discovery.as_mut().unwrap().getters.as_mut().unwrap();
            getters.globals = Some(globals); getters.configs = configs;
        } else if getters.configs.len() < getters.markets.as_ref().unwrap().len() {
            let batch: Vec<_> = getters.markets.as_ref().unwrap().iter().filter(|market| !getters.configs.contains_key(market))
                .take(getters.batch_size).copied().collect();
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().pending_batch_size = Some(batch.len());
            let values = self.reader.read_market_configs(self.spec.address, &batch, &at).await?;
            if values.len() != batch.len() || batch.iter().any(|market| !values.contains_key(market)) {
                return Err(Error::Decode("config read omitted or added markets".into()));
            }
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().configs.extend(values);
        } else if getters.oracle.is_none() {
            let oracle = validate_oracle_status(&self.reader.read_oracle(getters.pool.as_ref().unwrap(), &at).await?,
                getters.globals.as_ref().unwrap(), at.timestamp);
            self.discovery.as_mut().unwrap().getters.as_mut().unwrap().oracle = Some(oracle);
        } else { return Ok(true); }
        self.discovery.as_mut().unwrap().getters.as_mut().unwrap().pending_batch_size = None;
        Ok(false)
    }

    async fn recheck_restored_metadata(&mut self, at: BlockRef) -> Result<()> {
        let progress = self.discovery.as_ref().expect("restored progress exists");
        let getters = progress.getters.as_ref().expect("restored getters exist");
        let next = match self.restored_metadata_check.expect("metadata check pending") {
            RestoredMetadataCheck::Pool => {
                if let Some(expected) = &getters.pool {
                    if self.reader.describe_pool(&progress.spec, &at).await? != *expected {
                        return Err(Error::IdentityMismatch("restored pool descriptor differs at its anchor".into()));
                    }
                }
                Some(RestoredMetadataCheck::Globals)
            }
            RestoredMetadataCheck::Globals => {
                if let Some(expected) = getters.globals {
                    if self.reader.read_globals(self.spec.address, &at).await? != expected {
                        return Err(Error::Checkpoint("restored globals differ at their anchor".into()));
                    }
                }
                Some(RestoredMetadataCheck::Configs(None))
            }
            RestoredMetadataCheck::Configs(last) => {
                let start = last.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                let expected: BTreeMap<_, _> = getters.configs.range((start, std::ops::Bound::Unbounded))
                    .take(self.config.read_batch_size).map(|(market, config)| (*market, *config)).collect();
                if expected.is_empty() { None } else {
                    let markets: Vec<_> = expected.keys().copied().collect();
                    if self.reader.read_market_configs(self.spec.address, &markets, &at).await? != expected {
                        return Err(Error::Checkpoint("restored market configs differ at their anchor".into()));
                    }
                    Some(RestoredMetadataCheck::Configs(markets.last().copied()))
                }
            }
        };
        self.restored_metadata_check = next;
        Ok(())
    }

    async fn finish_discovery(&self, progress: &DiscoveryCheckpoint)
        -> Result<(SyncOutcome, StateSnapshot, ReconciliationCursor)>
    {
        let at = progress.target;
        if progress.through != Some(at) || progress.phase != DiscoveryPhase::Snapshot {
            return Err(Error::Checkpoint("discovery has not reached its getter target".into()));
        }
        let fast = self.config.event_driven_refresh && self.snapshot.is_some() && !progress.rebuilding;
        let spec = if fast {
            // target came from head(), previous state and final target are checked
            // canonically below. The immutable deployment binding survives that chain.
            self.validate_target(&at)?;
            progress.spec
        } else {
            if self.assert_identity().await? != progress.identity {
                return Err(Error::IdentityMismatch("discovery RPC chain/genesis changed".into()));
            }
            self.assert_target(&at).await?;
            let spec = self.anchored_spec(&at).await?;
            if spec != progress.spec { return Err(Error::IdentityMismatch("discovery deployment changed".into())); }
            spec
        };
        let getters = progress.getters.as_ref().ok_or_else(|| Error::Checkpoint("missing getter stage".into()))?;
        let pool = getters.pool.clone().ok_or_else(|| Error::Checkpoint("missing staged pool".into()))?;
        let globals = getters.globals.ok_or_else(|| Error::Checkpoint("missing staged globals".into()))?;
        let oracle = getters.oracle.clone().ok_or_else(|| Error::Checkpoint("missing staged oracle".into()))?;
        let mut configs = getters.configs.clone();
        let mut cursor = getters.reconciliation.clone();
        let active = match &getters.active_markets {
            Some(active) => active.clone(),
            None => self.reader.active_markets(self.spec.address, &at).await?.into_iter().collect(),
        };
        if getters.markets.as_ref().is_none_or(|markets| configs.len() != markets.len())
            || getters.amounts.len() != getters.keys.len() || getters.user_pages_remaining != 0
        { return Err(Error::Checkpoint("incomplete staged getters".into())); }
        let old = self.snapshot.as_ref().filter(|_| !progress.rebuilding);
        if old.is_some_and(|old| at.timestamp < old.as_of.timestamp) {
            return Err(Error::InvalidConfig("canonical target time precedes saved state".into()));
        }
        let mut changes = Vec::new();
        if let Some(old) = old {
            if old.globals != globals { changes.push(StateUpdate::PoolConfigChanged(globals)); }
            for (&market, config) in &configs {
                if old.markets.get(&market) != Some(config) {
                    changes.push(StateUpdate::MarketConfigChanged { market, config: *config });
                }
            }
            if old.oracle != oracle { changes.push(StateUpdate::OracleChanged(oracle.clone())); }
        }
        let positions = merged_positions(old.map(Arc::as_ref), &getters.amounts, &configs, &globals, &oracle,
            at, self.config.max_positions, &mut changes)?;
        prune_markets(&mut configs, &positions, &active, &mut cursor, old.map(Arc::as_ref), &mut changes);
        self.check_limits(positions.len(), configs.len())?;
        if let Some(old) = old {
            self.assert_canonical(&old.as_of).await?;
        }
        self.assert_canonical(&at).await?;
        if self.assert_identity().await? != progress.identity {
            return Err(Error::IdentityMismatch("discovery RPC chain/genesis changed".into()));
        }
        let snapshot = StateSnapshot { identity: progress.identity, spec, pool, abi_fingerprint: ABI_FINGERPRINT,
            epoch: progress.epoch, as_of: at, globals, markets: configs, oracle, positions };
        let outcome = if let Some(old) = old {
            SyncOutcome::Advanced(UpdateBatch { epoch: snapshot.epoch, from_exclusive: old.as_of, as_of: at, changes })
        } else if self.snapshot.is_some() { SyncOutcome::Reset(snapshot.clone()) }
        else { SyncOutcome::Bootstrapped(snapshot.clone()) };
        Ok((outcome, snapshot, cursor))
    }

    /// Builds a full snapshot using bounded, restartable discovery and a fresh getter target.
    /// An existing snapshot is rebuilt at a new epoch even when the head has not changed.
    /// Timeout or max_sync_steps exhaustion retains discovery_checkpoint; another call
    /// resumes that full rebuild. No incomplete snapshot is returned or committed.
    pub async fn bootstrap(&mut self) -> Result<StateSnapshot> {
        let timeout = self.config.operation_timeout;
        tokio::time::timeout(timeout, self.drive_sync(true)).await
            .map_err(|_| Error::Transport("state bootstrap timed out; save discovery progress and continue".into()))??;
        self.snapshot().cloned().ok_or_else(|| Error::Checkpoint("completed bootstrap has no snapshot".into()))
    }

    /// Selects a head and updates state, bootstrapping if the engine is empty.
    /// Returns Bootstrapped/Advanced/Unchanged/Reset; failures retain the previous snapshot
    /// and completed discovery. The entire call has one timeout and max_sync_steps budget.
    pub async fn sync(&mut self) -> Result<SyncOutcome> {
        let timeout = self.config.operation_timeout;
        tokio::time::timeout(timeout, self.drive_sync(false)).await
            .map_err(|_| Error::Transport("state sync timed out; save discovery progress and continue".into()))?
    }

    async fn drive_sync(&mut self, force_bootstrap: bool) -> Result<SyncOutcome> {
        if force_bootstrap && !self.discovery.as_ref().is_some_and(|progress| self.snapshot.is_none() || progress.rebuilding) {
            let generation = self.discovery.as_ref().map_or(Ok(1), |progress| progress.generation.checked_add(1)
                .ok_or_else(|| Error::Arithmetic("discovery generation exhausted".into())))?;
            self.discovery = None;
            self.start_discovery(generation, true).await?;
        }
        for _ in 0..self.config.max_sync_steps {
            if let SyncStepOutcome::Complete(outcome) = self.sync_step().await? { return Ok(outcome); }
        }
        Err(Error::ResourceLimit("bootstrap/sync exhausted max_sync_steps; save discovery progress and call again or use sync_step".into()))
    }

    /// Full rebuild at an explicit target matching commitment policy.
    ///
    /// All reads use this fixed target; long scans may require archive state.
    /// Success replaces the snapshot, increments the epoch and resets refresh cursors.
    /// Use `bootstrap()` or `sync_step()` to select a fresh target after discovery.
    pub async fn bootstrap_at(&mut self, at: BlockRef) -> Result<StateSnapshot> {
        let timeout = self.config.operation_timeout;
        let next_epoch = self.next_epoch()?;
        let staged = tokio::time::timeout(timeout, self.build_snapshot(at, next_epoch)).await
            .map_err(|_| Error::Transport("state bootstrap operation timed out".into()))??;
        self.last_reconciliation_timestamp = Some(staged.as_of.timestamp);
        self.metadata_verified = true;
        self.restored_metadata_check = None;
        self.snapshot = Some(Arc::new(staged.clone()));
        self.reconciliation = ReconciliationCursor::default();
        self.discovery = None;
        Ok(staged)
    }

    /// Advances to an explicit target and verifies the previous cursor.
    /// Reorg/unavailable old headers cause rebuild/Reset. An unverifiable rebuild fails
    /// without partial commit. The target cannot be newer than the selected head.
    /// Explicit-target calls require that target's execution state throughout the operation;
    /// they do not reanchor or create resumable discovery progress on failure.
    pub async fn sync_to(&mut self, at: BlockRef) -> Result<SyncOutcome> {
        let timeout = self.config.operation_timeout;
        let (outcome, staged, cursor) = tokio::time::timeout(timeout, self.stage_sync(at)).await
            .map_err(|_| Error::Transport("state sync operation timed out".into()))??;
        if let Some(snapshot) = staged {
            if self.reconciliation_due(snapshot.as_of) { self.last_reconciliation_timestamp = Some(snapshot.as_of.timestamp); }
            self.snapshot = Some(Arc::new(snapshot));
            self.metadata_verified = true;
            self.restored_metadata_check = None;
            self.reconciliation = cursor;
        }
        self.discovery = None;
        Ok(outcome)
    }

    fn reconciliation_due(&self, at: BlockRef) -> bool {
        self.config.reconciliation_interval.is_zero() || self.last_reconciliation_timestamp.is_none_or(|last|
            at.timestamp.saturating_sub(last) >= self.config.reconciliation_interval.as_secs().max(1))
    }

    fn next_epoch(&self) -> Result<u64> {
        self.snapshot.as_ref().map_or(Ok(1), |snapshot| snapshot.epoch.checked_add(1)
            .ok_or_else(|| Error::Arithmetic("state epoch exhausted".into())))
    }

    async fn assert_identity(&self) -> Result<ChainIdentity> {
        let identity = self.reader.identity().await?;
        if identity.chain_id == 0 || identity.genesis_hash.is_zero() {
            return Err(Error::IdentityMismatch("RPC returned an empty chain identity".into()));
        }
        if self.config.expected_identity.is_some_and(|expected| expected != identity)
            || self.snapshot.as_ref().is_some_and(|snapshot| snapshot.identity != identity)
        {
            return Err(Error::IdentityMismatch("RPC chain/genesis changed".into()));
        }
        Ok(identity)
    }

    async fn assert_canonical(&self, at: &BlockRef) -> Result<()> {
        let canonical = self.reader.block(at.number, at.commitment).await?.ok_or(Error::MissingBlock(at.number))?;
        if canonical != *at {
            return Err(Error::NonCanonical(at.number));
        }
        Ok(())
    }

    fn validate_target(&self, at: &BlockRef) -> Result<()> {
        if at.commitment != self.config.commitment || at.number < self.spec.deployment_block || at.hash.is_zero() {
            return Err(Error::InvalidConfig("target commitment/block differs from engine policy".into()));
        }
        Ok(())
    }

    async fn assert_target(&self, at: &BlockRef) -> Result<()> {
        self.validate_target(at)?;
        let head = self.reader.head(self.config.commitment).await?;
        if at.number > head.number {
            return Err(Error::InvalidConfig("target is newer than selected commitment head".into()));
        }
        if at.number == head.number && at.hash != head.hash {
            return Err(Error::NonCanonical(at.number));
        }
        self.assert_canonical(at).await
    }

    async fn anchored_spec(&self, at: &BlockRef) -> Result<PoolSpec> {
        let deployment = self.reader.block(self.spec.deployment_block, at.commitment).await?
            .ok_or(Error::MissingBlock(self.spec.deployment_block))?;
        if deployment.hash.is_zero() {
            return Err(Error::IdentityMismatch("deployment block hash is zero".into()));
        }
        let expected_hash = self.snapshot.as_ref().and_then(|snapshot| snapshot.spec.deployment_block_hash)
            .or(self.spec.deployment_block_hash);
        if expected_hash.is_some_and(|hash| hash != deployment.hash) {
            return Err(Error::IdentityMismatch("deployment block changed".into()));
        }
        Ok(PoolSpec { deployment_block_hash: Some(deployment.hash), ..self.spec })
    }

    fn check_limits(&self, positions: usize, markets: usize) -> Result<()> {
        if positions > self.config.max_positions || markets > self.config.max_markets {
            return Err(Error::ResourceLimit("position/market count exceeds engine limit".into()));
        }
        Ok(())
    }

    async fn collect_invalidations(&self, from: u64, to: u64, commitment: Commitment) -> Result<Invalidations> {
        self.collect_invalidations_bounded(from, to, commitment,
            self.config.max_log_calls, self.config.max_logs_per_sync).await.map(|(values, _, _)| values)
    }

    async fn collect_invalidations_bounded(&self, from: u64, to: u64, commitment: Commitment,
        calls_limit: usize, logs_limit: usize) -> Result<(Invalidations, usize, usize)>
    {
        let blocks = to.checked_sub(from).and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| Error::InvalidConfig("invalid history range".into()))?;
        if blocks > self.config.max_blocks_per_sync {
            return Err(Error::ResourceLimit("history range exceeds max_blocks_per_sync; increase explicitly or advance in steps".into()));
        }
        let mut start = from;
        let mut calls = 0usize;
        let mut count = 0usize;
        let mut result = Invalidations::default();
        loop {
            let end = start.saturating_add(self.config.log_chunk_size - 1).min(to);
            let mut ranges = vec![(start, end)];
            while let Some((left, right)) = ranges.pop() {
                calls += 1;
                if calls > calls_limit {
                    return Err(Error::ResourceLimit("history exhausted max_log_calls".into()));
                }
                let logs = match self.reader.logs(self.spec.address, left, right).await {
                    Ok(logs) => logs,
                    Err(error) if matches!(classify_rpc_error(&error), RpcErrorKind::RangeTooWide | RpcErrorKind::PayloadTooLarge) => {
                        if left == right {
                            return Err(Error::IncompleteHistory(format!("block {left} exceeds RPC log limit; cannot skip it: {error}")));
                        }
                        let midpoint = left + (right - left) / 2;
                        ranges.push((midpoint + 1, right));
                        ranges.push((left, midpoint));
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let logs = normalize_logs(self.spec.address, left, right, logs)?;
                count = count.checked_add(logs.len()).ok_or_else(|| Error::ResourceLimit("log count overflow".into()))?;
                if count > logs_limit {
                    return Err(Error::ResourceLimit("history exceeds max_logs_per_sync".into()));
                }
                let mut witnesses = Vec::new();
                for log in &logs {
                    if witnesses.last().is_none_or(|&(number, _)| number != log.block_number) {
                        witnesses.push((log.block_number, log.block_hash));
                    }
                }
                self.reader.verify_log_blocks(&witnesses, commitment).await?;
                for log in logs {
                    result.extend(decode_invalidation(self.spec.address, &log)?);
                    self.check_limits(result.positions.len(), 0)?;
                }
            }
            if end == to { break; }
            start = end + 1;
        }
        Ok((result, calls, count))
    }

    async fn read_amounts(&self, keys: &BTreeSet<PositionKey>, at: &BlockRef) -> Result<BTreeMap<PositionKey, PositionAmounts>> {
        let keys: Vec<_> = keys.iter().copied().collect();
        let mut result = BTreeMap::new();
        for batch in keys.chunks(self.config.read_batch_size) {
            let values = self.reader.read_positions(self.spec.address, batch, at).await?;
            if values.len() != batch.len() || batch.iter().any(|key| !values.contains_key(key)) {
                return Err(Error::Decode("primitive read omitted or added position keys".into()));
            }
            result.extend(values);
        }
        Ok(result)
    }

    async fn read_configs(&self, markets: &BTreeSet<u64>, at: &BlockRef) -> Result<BTreeMap<u64, MarketRiskConfig>> {
        let markets: Vec<_> = markets.iter().copied().collect();
        let mut result = BTreeMap::new();
        for batch in markets.chunks(self.config.read_batch_size) {
            let values = self.reader.read_market_configs(self.spec.address, batch, at).await?;
            if values.len() != batch.len() || batch.iter().any(|market| !values.contains_key(market)) {
                return Err(Error::Decode("config read omitted or added markets".into()));
            }
            result.extend(values);
        }
        Ok(result)
    }

    async fn build_snapshot(&self, at: BlockRef, epoch: u64) -> Result<StateSnapshot> {
        let identity = self.assert_identity().await?;
        self.assert_target(&at).await?;
        let spec = self.anchored_spec(&at).await?;
        let pool = self.reader.describe_pool(&spec, &at).await?;
        if self.snapshot.as_ref().is_some_and(|old| old.pool != pool) {
            return Err(Error::IdentityMismatch("pool descriptor/code changed".into()));
        }
        let invalidations = self.collect_invalidations(spec.deployment_block, at.number, at.commitment).await?;
        let amounts = self.read_amounts(&invalidations.positions, &at).await?;
        let mut markets = position_markets(None,&amounts);
        let active: BTreeSet<_> = self.reader.active_markets(spec.address, &at).await?.into_iter().collect();
        markets.extend(active.iter().copied());
        self.check_limits(0, markets.len())?;
        let globals = self.reader.read_globals(spec.address, &at).await?;
        let mut configs = self.read_configs(&markets, &at).await?;
        let oracle = validate_oracle_status(&self.reader.read_oracle(&pool, &at).await?, &globals, at.timestamp);
        let mut positions = BTreeMap::new();
        for (key, amounts) in amounts {
            if amounts.debt == 0 && amounts.collateral == 0 { continue; }
            let config = configs.get(&key.market).ok_or_else(|| Error::Decode("missing market config".into()))?;
            let risk = evaluate_risk(&amounts, key.market, &globals, config, &oracle, at.timestamp);
            positions.insert(key, PositionRecord { key, amounts, risk, last_changed: at });
        }
        prune_markets(&mut configs, &positions, &active, &mut ReconciliationCursor::default(), None, &mut Vec::new());
        self.check_limits(positions.len(), configs.len())?;
        self.assert_canonical(&at).await?;
        self.assert_identity().await?;
        let snapshot = StateSnapshot { identity, spec, pool, abi_fingerprint: ABI_FINGERPRINT, epoch,
            as_of: at, globals, markets: configs, oracle, positions };
        Checkpoint::new(snapshot.clone(), ReconciliationCursor::default())?;
        Ok(snapshot)
    }

    async fn reconcile_keys(&self, old: &StateSnapshot, markets: &BTreeSet<u64>, at: &BlockRef,
        cursor: &mut ReconciliationCursor) -> Result<BTreeSet<PositionKey>>
    {
        let mut keys = BTreeSet::new();
        if !self.reconciliation_due(*at) { return Ok(keys); }
        for key in rotating_keys(&old.positions, cursor.last_position, self.config.reconcile_positions_per_sync) {
            keys.insert(key); cursor.last_position = Some(key);
        }
        let markets: Vec<_> = markets.iter().copied().collect();
        if !markets.is_empty() {
            let mut index = cursor.next_market.map_or(0, |next| markets.partition_point(|market| *market < next));
            for _ in 0..self.config.reconcile_user_pages_per_sync {
                index %= markets.len();
                let market = markets[index];
                let start = *cursor.user_offsets.get(&market).unwrap_or(&0);
                let users = self.reader.market_users(self.spec.address, market, start,
                    self.config.reconcile_user_page_size, at).await?;
                if users.len() as u64 > self.config.reconcile_user_page_size {
                    return Err(Error::Decode("market user page exceeds requested limit".into()));
                }
                for borrower in &users {
                    if borrower.is_zero() {
                        return Err(Error::Decode("market user page contains zero address".into()));
                    }
                    keys.insert(PositionKey { pool: self.spec.address, market, borrower: *borrower });
                }
                let next_offset = if (users.len() as u64) < self.config.reconcile_user_page_size { 0 }
                    else { start.checked_add(users.len() as u64).ok_or_else(|| Error::Arithmetic("user page offset overflow".into()))? };
                cursor.user_offsets.insert(market, next_offset);
                index = (index + 1) % markets.len();
                cursor.next_market = Some(markets[index]);
            }
        }
        self.check_limits(keys.len(), 0)?;
        Ok(keys)
    }

    async fn stage_sync(&self, at: BlockRef) -> Result<(SyncOutcome, Option<StateSnapshot>, ReconciliationCursor)> {
        let Some(old) = self.snapshot.as_ref() else {
            let snapshot = self.build_snapshot(at, 1).await?;
            return Ok((SyncOutcome::Bootstrapped(snapshot.clone()), Some(snapshot), ReconciliationCursor::default()));
        };
        self.assert_identity().await?;
        self.assert_target(&at).await?;
        let old_canonical = self.reader.block(old.as_of.number, self.config.commitment).await?;
        if old_canonical != Some(old.as_of) {
            let snapshot = self.build_snapshot(at, self.next_epoch()?).await?;
            return Ok((SyncOutcome::Reset(snapshot.clone()), Some(snapshot), ReconciliationCursor::default()));
        }
        if at.timestamp < old.as_of.timestamp {
            return Err(Error::InvalidConfig("canonical target time precedes saved state".into()));
        }
        if at.number < old.as_of.number {
            return Err(Error::InvalidConfig("sync target precedes canonical cursor".into()));
        }
        if at.number == old.as_of.number {
            return Ok((SyncOutcome::Unchanged { epoch: old.epoch, as_of: old.as_of }, None, self.reconciliation.clone()));
        }
        let spec = self.anchored_spec(&at).await?;
        let reuse = self.config.event_driven_refresh && self.metadata_verified;
        let pool = if reuse { old.pool.clone() } else { self.reader.describe_pool(&spec, &at).await? };
        if pool != old.pool { return Err(Error::IdentityMismatch("pool descriptor/code changed".into())); }
        let invalidations = self.collect_invalidations(old.as_of.number + 1, at.number, at.commitment).await?;
        let reuse_config = reuse && !invalidations.config_changed;
        let mut markets: BTreeSet<_> = old.positions.keys().map(|key| key.market).collect();
        markets.extend(invalidations.markets);
        let active: BTreeSet<_> = self.reader.active_markets(spec.address, &at).await?.into_iter().collect();
        markets.extend(active.iter().copied());
        self.check_limits(old.positions.len(), 0)?;
        let mut cursor = self.reconciliation.clone();
        let mut keys = invalidations.positions;
        keys.extend(self.reconcile_keys(old, &markets, &at, &mut cursor).await?);
        self.check_limits(keys.len(), 0)?;
        let refreshed = self.read_amounts(&keys, &at).await?;
        markets = position_markets(Some(old),&refreshed); markets.extend(active.iter().copied());
        self.check_limits(0,markets.len())?;
        let globals = if reuse_config { old.globals } else { self.reader.read_globals(spec.address, &at).await? };
        // Admin events refresh all effective configs, including arbitrary unactivated markets.
        let mut configs = if reuse_config {
            let mut configs: BTreeMap<_,_> = old.markets.iter().filter(|(m,_)| markets.contains(m)).map(|(m,c)| (*m,*c)).collect();
            let refresh = markets.iter().filter(|market| !configs.contains_key(market)).copied().collect();
            configs.extend(self.read_configs(&refresh, &at).await?); configs
        } else { self.read_configs(&markets, &at).await? };
        let oracle = validate_oracle_status(&self.reader.read_oracle(&pool, &at).await?, &globals, at.timestamp);
        let mut changes = Vec::new();
        if globals != old.globals { changes.push(StateUpdate::PoolConfigChanged(globals)); }
        for (&market, config) in &configs {
            if old.markets.get(&market) != Some(config) {
                changes.push(StateUpdate::MarketConfigChanged { market, config: *config });
            }
        }
        if oracle != old.oracle { changes.push(StateUpdate::OracleChanged(oracle.clone())); }
        let positions = merged_positions(Some(old), &refreshed, &configs, &globals, &oracle,
            at, self.config.max_positions, &mut changes)?;
        prune_markets(&mut configs, &positions, &active, &mut cursor, Some(old), &mut changes);
        self.check_limits(positions.len(), configs.len())?;
        self.assert_canonical(&old.as_of).await?;
        self.assert_canonical(&at).await?;
        self.assert_identity().await?;
        let snapshot = StateSnapshot { identity: old.identity, spec, pool, abi_fingerprint: ABI_FINGERPRINT,
            epoch: old.epoch, as_of: at, globals, markets: configs, oracle, positions };
        let batch = UpdateBatch { epoch: old.epoch, from_exclusive: old.as_of, as_of: at, changes };
        Ok((SyncOutcome::Advanced(batch), Some(snapshot), cursor))
    }
}

// Merge two ordered maps directly into the published map. There is no full-sized
// intermediate amount map and no lookup of every old key in the new map. Risk is
// reused only when all of its inputs, including block time, are exactly equal.
fn merged_positions(old: Option<&StateSnapshot>, refreshed: &BTreeMap<PositionKey, PositionAmounts>,
    configs: &BTreeMap<u64, MarketRiskConfig>, globals: &PoolRiskConfig, oracle: &OracleStatus,
    at: BlockRef, max_positions: usize, changes: &mut Vec<StateUpdate>) -> Result<BTreeMap<PositionKey, PositionRecord>>
{
    let mut previous = old.into_iter().flat_map(|snapshot| snapshot.positions.iter()).peekable();
    let mut updates = refreshed.iter().peekable();
    let stable = old.is_some_and(|snapshot| snapshot.as_of.timestamp == at.timestamp
        && snapshot.globals == *globals && snapshot.oracle == *oracle);
    let mut positions = BTreeMap::new();
    let mut removed = Vec::new();
    loop {
        let (key, amounts, prior) = match (previous.peek(), updates.peek()) {
            (Some((old_key, _)), Some((new_key, _))) if old_key == new_key => {
                let (_, prior) = previous.next().unwrap();
                let (key, amounts) = updates.next().unwrap(); (*key, *amounts, Some(prior))
            }
            (Some((old_key, _)), Some((new_key, _))) if old_key < new_key => {
                let (key, prior) = previous.next().unwrap(); (*key, prior.amounts, Some(prior))
            }
            (_, Some(_)) => { let (key, amounts) = updates.next().unwrap(); (*key, *amounts, None) }
            (Some(_), None) => { let (key, prior) = previous.next().unwrap(); (*key, prior.amounts, Some(prior)) }
            (None, None) => break,
        };
        if amounts.debt == 0 && amounts.collateral == 0 {
            if prior.is_some() { removed.push(StateUpdate::PositionRemoved(key)); }
            continue;
        }
        if positions.len() >= max_positions { return Err(Error::ResourceLimit("position count exceeds max_positions".into())); }
        let config = configs.get(&key.market).ok_or_else(|| Error::Decode("missing effective market config".into()))?;
        let unchanged_inputs = stable && prior.is_some_and(|prior| prior.amounts == amounts)
            && old.is_some_and(|snapshot| snapshot.markets.get(&key.market) == Some(config));
        let risk = if unchanged_inputs { prior.unwrap().risk.clone() }
            else { evaluate_risk(&amounts, key.market, globals, config, oracle, at.timestamp) };
        let changed = prior.is_none_or(|prior| prior.amounts != amounts || prior.risk != risk);
        let record = PositionRecord { key, amounts, risk,
            last_changed: if changed { at } else { prior.unwrap().last_changed } };
        if changed { changes.push(StateUpdate::PositionUpsert(record.clone())); }
        positions.insert(key, record);
    }
    changes.extend(removed);
    Ok(positions)
}

// Rotate in O(log N + limit), without allocating every known borrower key.
fn rotating_keys(positions: &BTreeMap<PositionKey, PositionRecord>, last: Option<PositionKey>, limit: usize) -> Vec<PositionKey> {
    use std::ops::Bound::{Excluded, Unbounded};
    let start = last.map_or(Unbounded, Excluded);
    positions.range((start, Unbounded)).chain(positions.range((Unbounded, last.map_or(Unbounded, std::ops::Bound::Included))))
        .take(limit.min(positions.len())).map(|(key, _)| *key).collect()
}

// Keep every nonempty position, including mature debt and collateral-only state.
// Events rediscover an evicted market; old completed snapshots are immutable.
fn prune_markets(configs: &mut BTreeMap<u64, MarketRiskConfig>, positions: &BTreeMap<PositionKey, PositionRecord>,
    active: &BTreeSet<u64>, cursor: &mut ReconciliationCursor, old: Option<&StateSnapshot>, changes: &mut Vec<StateUpdate>) {
    let mut keep = active.clone(); keep.extend(positions.keys().map(|key| key.market));
    configs.retain(|market,_| keep.contains(market));
    changes.retain(|change| !matches!(change,StateUpdate::MarketConfigChanged {market,..} if !configs.contains_key(market)));
    cursor.user_offsets.retain(|market,_| configs.contains_key(market));
    if cursor.next_market.is_some_and(|market| !configs.contains_key(&market)) { cursor.next_market = None; }
    if let Some(old) = old {
        changes.extend(old.markets.keys().filter(|market| !configs.contains_key(market)).map(|market| StateUpdate::MarketRemoved(*market)));
    }
}

fn position_markets(old: Option<&StateSnapshot>, refreshed: &BTreeMap<PositionKey,PositionAmounts>) -> BTreeSet<u64> {
    let mut markets=BTreeSet::new();
    if let Some(old)=old {
        for (key,record) in &old.positions {
            let amounts=refreshed.get(key).unwrap_or(&record.amounts);
            if amounts.debt!=0||amounts.collateral!=0 {markets.insert(key.market);}
        }
    }
    markets.extend(refreshed.iter().filter(|(_,a)|a.debt!=0||a.collateral!=0).map(|(key,_)|key.market));
    markets
}

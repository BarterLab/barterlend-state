//! Current-state census for every market that can contain v5 debt.
//!
//! The immutable schedule enumerates mature markets; current active markets cover
//! future maturities. Paginated user lists are read at one block. No historical
//! logs are needed to bootstrap or recover from a long gap. This does NOT promise
//! discovery of isolated collateral-only positions outside that schedule.

use super::*;
use std::path::{Path, PathBuf};
use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// Immutable three-tier v5 market schedule, as returned by getMarketsConfig.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSchedule {
    /// Pool initialization timestamp.
    pub start: u64,
    /// Nonzero durations; each next tier is a multiple of the preceding tier.
    pub durations: [u64; 3],
    /// Number of simultaneously visible markets per tier.
    pub active: [u8; 3],
    /// Early visibility window, strictly smaller than its tier duration.
    pub extensions: [u64; 3],
}

impl MarketSchedule {
    /// Validates the on-chain schedule and lists every mature grid point plus
    /// the supplied active markets. Checks the count before allocating/iterating.
    fn census_count(&self, timestamp: u64, active: &[u64], cap: usize) -> Result<u64> {
        if self.start == 0 || (0..3).any(|i| self.durations[i] == 0 || self.active[i] == 0
            || self.extensions[i] >= self.durations[i]
            || self.durations[i].checked_mul(u64::from(self.active[i])).and_then(|v| v.checked_add(self.extensions[i])).is_none())
            || (1..3).any(|i| self.durations[i] < self.durations[i-1] || self.durations[i] % self.durations[i-1] != 0)
        { return Err(Error::Decode("invalid immutable market schedule".into())); }
        let mut supplied = BTreeSet::new();
        for &market in active {
            if timestamp < self.start || market <= timestamp || market <= self.start
                || !(0..3).any(|i| (market-self.start) % self.durations[i] == 0
                    && market-timestamp <= u64::from(self.active[i])*self.durations[i]+self.extensions[i])
                || !supplied.insert(market)
            { return Err(Error::Decode("active market list differs from schedule".into())); }
        }
        // A truncated active list must not silently omit debt-bearing markets.
        let mut expected = BTreeSet::new();
        if timestamp >= self.start {
            for i in 0..3 {
                let duration = self.durations[i];
                let window = u64::from(self.active[i])*duration+self.extensions[i];
                let mut next = u128::from(timestamp)-u128::from((timestamp-self.start)%duration)+u128::from(duration);
                while next <= u128::from(u64::MAX) && next-u128::from(timestamp) <= u128::from(window) {
                    expected.insert(next as u64);
                    if expected.len() > cap { return Err(Error::ResourceLimit("active schedule exceeds market cap".into())); }
                    next += u128::from(duration);
                }
            }
        }
        if expected != active.iter().copied().collect() {
            return Err(Error::Decode("incomplete active market list".into()));
        }
        Ok(timestamp.saturating_sub(self.start) / self.durations[0])
    }

    /// Materializes a bounded calendar for callers that need the entire list.
    /// The engine itself walks the calendar lazily, independently of pool age.
    pub fn census_markets(&self, timestamp: u64, active: &[u64], cap: usize) -> Result<Vec<u64>> {
        let count = self.census_count(timestamp, active, cap)?;
        if u128::from(count) + active.len() as u128 > cap as u128 {
            return Err(Error::ResourceLimit("current census exceeds max_calendar_markets".into()));
        }
        Ok((0..count).map(|i| self.start+(i+1)*self.durations[0]).chain(active.iter().copied()).collect::<BTreeSet<_>>().into_iter().collect())
    }

    fn market_at(&self, index: u64, count: u64, active: &[u64]) -> Option<u64> {
        if index < count { self.start.checked_add(index.checked_add(1)?.checked_mul(self.durations[0])?) }
        else { active.get(usize::try_from(index-count).ok()?).copied() }
    }
}

/// Additional bounds for the opt-in current-state engine. Existing EngineConfig
/// and the default StateEngine retain their original full-history semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateConfig {
    /// A larger missed event span causes a fresh current census, never log replay.
    pub max_event_catchup_blocks: u64,
    /// Maximum calendar entries held in a census batch; not a lifetime limit.
    pub max_calendar_markets: usize,
}
impl Default for CurrentStateConfig {
    fn default() -> Self { Self { max_event_catchup_blocks: 1000, max_calendar_markets: 20_000 } }
}
impl CurrentStateConfig {
    fn validate(&self) -> Result<()> {
        if self.max_event_catchup_blocks == 0 || self.max_calendar_markets == 0 || self.max_calendar_markets > 1_000_000 {
            return Err(Error::InvalidConfig("current census requires positive tail/calendar bounds; calendar <= 1000000".into()));
        }
        Ok(())
    }
}

/// Explicit completeness guarantee; never equivalent to arbitrary-market census.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentCoverage {
    /// All debt positions in supported v5, plus discovered collateral positions.
    /// Isolated collateral-only positions outside the schedule may be absent.
    AllDebtPositionsV5,
}

/// Shared snapshot with its deliberately explicit coverage.
#[derive(Clone, Debug)]
pub struct CurrentSnapshot {
    /// Data at one canonical anchor; freshness still belongs to the consumer.
    pub state: Arc<StateSnapshot>,
    /// Completeness of the discovered universe.
    pub coverage: CurrentCoverage,
}

/// Bounded synchronization result with explicit coverage for every outcome.
#[derive(Debug)]
pub struct CurrentStep {
    /// Completeness guarantee of completed state.
    pub coverage: CurrentCoverage,
    /// Progress is never execution authority.
    pub outcome: SyncStepOutcome,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CensusStream {
    next_index: u64,
    batch_start: u64,
    // One digest per unfinished market, never one entry per historical user.
    last_pages: BTreeMap<u64, B256>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CurrentCensus {
    identity: ChainIdentity, spec: PoolSpec, target: BlockRef, epoch: u64, generation: u64,
    schedule: Option<MarketSchedule>, active: Option<Vec<u64>>, markets: Option<Vec<u64>>,
    market_cursor: usize, pending_users: BTreeMap<u64,u64>, keys: BTreeSet<PositionKey>,
    // Absent in legacy checkpoints. Skipping None preserves their checksum bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream: Option<CensusStream>,
}
impl CurrentCensus {
    fn status(&self) -> crate::DiscoveryStatus {
        crate::DiscoveryStatus { generation: self.generation, from_block: self.target.number,
            through: None, target: self.target, phase: DiscoveryPhase::Snapshot,
            discovered_positions: self.keys.len(), discovered_markets: self.market_cursor, rebuilding: true }
    }
    fn validate(&self, config: &EngineConfig, policy: &CurrentStateConfig) -> Result<()> {
        if self.target.commitment != config.commitment || self.target.number < self.spec.deployment_block
            || self.target.hash.is_zero() || self.epoch == 0 || self.generation == 0
            || self.keys.len() > config.max_positions
            || self.keys.iter().any(|k| k.pool != self.spec.address || k.borrower.is_zero())
        { return Err(Error::Checkpoint("invalid current census anchor/keys".into())); }
        if let Some(stream) = &self.stream {
            if self.markets.is_some() || self.pending_users.len() > policy.max_calendar_markets.min(64)
                || stream.last_pages.keys().any(|m| !self.pending_users.contains_key(m)) {
                return Err(Error::Checkpoint("invalid streaming census buffer".into()));
            }
            if let (Some(schedule), Some(active)) = (&self.schedule, &self.active) {
                let count = schedule.census_count(self.target.timestamp, active, 1024)?;
                let total = count.checked_add(active.len() as u64).ok_or_else(|| Error::Arithmetic("calendar count overflow".into()))?;
                let contains = |market: u64| active.contains(&market) || market > schedule.start
                    && market <= self.target.timestamp && (market-schedule.start)%schedule.durations[0] == 0;
                if stream.next_index > total || stream.next_index < self.pending_users.len() as u64 || stream.batch_start > stream.next_index
                    || stream.next_index-stream.batch_start > policy.max_calendar_markets.min(64) as u64
                    || self.market_cursor as u64 != stream.next_index-self.pending_users.len() as u64
                    || self.keys.iter().any(|k| !contains(k.market))
                    || self.pending_users.keys().any(|m| !(stream.batch_start..stream.next_index)
                        .any(|i| schedule.market_at(i,count,active) == Some(*m))) {
                    return Err(Error::Checkpoint("invalid streaming census cursor".into()));
                }
            } else if self.active.is_some() || stream.next_index != 0 || stream.batch_start != 0
                || self.market_cursor != 0 || !self.pending_users.is_empty() || !self.keys.is_empty() {
                return Err(Error::Checkpoint("invalid streaming census stages".into()));
            }
            return Ok(());
        }
        match (&self.schedule, &self.active, &self.markets) {
            (Some(schedule), Some(active), Some(markets)) => {
                if *markets != schedule.census_markets(self.target.timestamp, active, policy.max_calendar_markets)?
                    || self.market_cursor > markets.len() || self.pending_users.len() != markets.len()-self.market_cursor
                    || self.pending_users.iter().any(|(market, offset)| markets.binary_search(market).is_err()
                        || *offset != self.keys.range(PositionKey { pool: self.spec.address, market: *market, borrower: alloy_primitives::Address::ZERO }
                            ..=PositionKey { pool: self.spec.address, market: *market, borrower: alloy_primitives::Address::repeat_byte(0xff) }).count() as u64)
                    || self.keys.iter().any(|key| markets.binary_search(&key.market).is_err())
                { return Err(Error::Checkpoint("invalid current census cursor".into())); }
            }
            (_, None, None) if self.market_cursor == 0 && self.pending_users.is_empty() && self.keys.is_empty() => {},
            _ => return Err(Error::Checkpoint("invalid current census stages".into())),
        }
        Ok(())
    }
}

/// Optional engine for consumers needing complete debt coverage without old logs.
/// Uses the same exact getters, event updates, risk model and reorg checks as
/// StateEngine. Its checkpoint type cannot be restored by StateEngine/FileStore.
pub struct CurrentStateEngine<R: StateReader + ?Sized> { inner: StateEngine<R> }
impl<R: StateReader + ?Sized> CurrentStateEngine<R> {
    /// Creates an empty current-state engine. The first sync enumerates live storage.
    pub fn new(reader: Arc<R>, spec: PoolSpec, config: EngineConfig, policy: CurrentStateConfig) -> Result<Self> {
        policy.validate()?;
        let mut inner = StateEngine::new(reader, spec, config)?;
        inner.current_policy = Some(policy);
        Ok(Self { inner })
    }
    /// Imports a full SDK checkpoint without weakening its completeness. A long
    /// gap automatically switches to current census on the next synchronization.
    pub fn from_full_checkpoint(reader: Arc<R>, spec: PoolSpec, config: EngineConfig,
        policy: CurrentStateConfig, checkpoint: Checkpoint) -> Result<Self>
    {
        policy.validate()?;
        let mut inner = StateEngine::restore(reader, spec, config, checkpoint)?;
        inner.current_policy = Some(policy);
        Ok(Self { inner })
    }
    /// Restores trusted local state/progress; live anchors are rechecked by sync.
    pub fn restore(reader: Arc<R>, spec: PoolSpec, config: EngineConfig, policy: CurrentStateConfig,
        checkpoint: CurrentCheckpoint) -> Result<Self>
    {
        policy.validate()?;
        checkpoint.validate(config.expected_identity.as_ref(), Some(&spec))?;
        let mut engine = match checkpoint.state {
            Some(state) => Self::from_full_checkpoint(reader, spec, config, policy, state)?,
            None => Self::new(reader, spec, config, policy)?,
        };
        if let Some(progress) = checkpoint.discovery { engine.inner.restore_discovery(progress)?; }
        if let Some(census) = checkpoint.census {
            census.validate(&engine.inner.config, engine.inner.current_policy.as_ref().unwrap())?;
            if census.epoch != engine.inner.next_epoch()? {
                return Err(Error::Checkpoint("current census epoch differs from completed state".into()));
            }
            // Completed snapshots remain usable. Legacy unfinished census stored every
            // historical key; restart that one derived stage with bounded streaming.
            if census.stream.is_some() { engine.inner.current_census = Some(census); }
            else { engine.inner.begin_current_census(census.identity, census.spec, census.target, census.generation)?; }
        }
        Ok(engine)
    }
    /// Last completed data; may be stale/unverified after restore or an error.
    pub fn snapshot_shared(&self) -> Option<CurrentSnapshot> {
        self.inner.snapshot_shared().map(|state| CurrentSnapshot { state, coverage: CurrentCoverage::AllDebtPositionsV5 })
    }
    /// One bounded unit. A stale/orphan/pruned target never authorizes a snapshot.
    /// HTTP readers share header checks within one bounded step. Its staged
    /// changes commit only after fresh closing checks; cancellation retains the
    /// preceding completed step. The default history engine is unchanged.
    pub async fn sync_step(&mut self) -> Result<CurrentStep> {
        let timeout = self.inner.config.operation_timeout;
        tokio::time::timeout(timeout, self.step_inner()).await
            .map_err(|_| Error::Transport("bounded current state step timed out".into()))?
    }
    async fn step_inner(&mut self) -> Result<CurrentStep> {
        let target = self.inner.current_census.as_ref().map(|c| c.target)
            .or_else(|| self.inner.discovery.as_ref().map(|p| p.target));
        if let Some(target) = target {
            let head = self.inner.reader.head(self.inner.config.commitment).await?;
            if head.number.saturating_sub(target.number) > self.inner.current_policy.as_ref().unwrap().max_event_catchup_blocks {
                self.inner.current_census = None; self.inner.discovery = None;
            }
        }
        match self.inner.sync_step().await {
            Ok(outcome) => Ok(CurrentStep { coverage: CurrentCoverage::AllDebtPositionsV5, outcome }),
            Err(error @ (Error::NonCanonical(_) | Error::MissingBlock(_))) => {
                self.inner.current_census = None; self.inner.discovery = None;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
    /// Captures export state for checksum/serialization on a blocking worker.
    pub fn checkpoint_builder(&self) -> Option<CurrentCheckpointBuilder> {
        if self.inner.snapshot.is_none() && self.inner.discovery.is_none() && self.inner.current_census.is_none() { return None; }
        Some(CurrentCheckpointBuilder { state: self.inner.snapshot.clone(), cursor: self.inner.reconciliation.clone(),
            discovery: self.inner.discovery_checkpoint_builder(), census: self.inner.current_census.clone() })
    }
}

/// Captured scoped checkpoint export; build does no network or filesystem I/O.
pub struct CurrentCheckpointBuilder {
    state: Option<Arc<StateSnapshot>>, cursor: ReconciliationCursor,
    discovery: Option<DiscoveryCheckpointBuilder>, census: Option<CurrentCensus>,
}
impl CurrentCheckpointBuilder {
    /// Validates/checksums the captured completed and unfinished state.
    pub fn build(self) -> Result<CurrentCheckpoint> {
        let state = self.state.map(|s| Checkpoint::new((*s).clone(), self.cursor)).transpose()?;
        let discovery = self.discovery.map(DiscoveryCheckpointBuilder::build).transpose()?;
        let mut value = CurrentCheckpoint { schema_version: 1, abi_fingerprint: ABI_FINGERPRINT, coverage: CurrentCoverage::AllDebtPositionsV5,
            state, discovery, census: self.census, checksum: B256::ZERO };
        value.checksum = value.digest()?;
        value.validate(None, None)?;
        Ok(value)
    }
}

/// Separate persisted envelope. Full-history Checkpoint/FileStore reject this
/// format instead of mistaking debt coverage for arbitrary-market completeness.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentCheckpoint {
    schema_version: u32, abi_fingerprint: B256, coverage: CurrentCoverage,
    state: Option<Checkpoint>, discovery: Option<DiscoveryCheckpoint>, census: Option<CurrentCensus>, checksum: B256,
}
impl CurrentCheckpoint {
    fn digest(&self) -> Result<B256> {
        crate::encoding::digest(&(self.schema_version, self.abi_fingerprint, self.coverage, &self.state, &self.discovery, &self.census))
    }
    fn binding(&self) -> Result<(ChainIdentity, PoolSpec)> {
        if let Some(s) = &self.state { return Ok((s.snapshot.identity, s.snapshot.spec)); }
        if let Some(p) = &self.discovery { return Ok((p.identity, p.spec)); }
        if let Some(c) = &self.census { return Ok((c.identity, c.spec)); }
        Err(Error::Checkpoint("empty current checkpoint".into()))
    }
    /// Checks local integrity and scope; cannot prove authenticity of third-party data.
    pub fn validate(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<()> {
        if self.schema_version != 1 || self.abi_fingerprint != ABI_FINGERPRINT || self.checksum != self.digest()? || self.discovery.is_some() && self.census.is_some() {
            return Err(Error::Checkpoint("invalid current checkpoint schema/checksum/stages".into()));
        }
        let (id, deployment) = self.binding()?;
        if id.chain_id == 0 || id.genesis_hash.is_zero() || deployment.address.is_zero()
            || deployment.deployment_block_hash.is_none_or(|h| h.is_zero()) || deployment.expected_version != PROTOCOL_VERSION
            || identity.is_some_and(|expected| *expected != id)
            || spec.is_some_and(|expected| expected.address != deployment.address || expected.deployment_block != deployment.deployment_block
                || expected.expected_version != deployment.expected_version || expected.deployment_block_hash.is_some_and(|h| Some(h) != deployment.deployment_block_hash))
        { return Err(Error::IdentityMismatch("current checkpoint chain/deployment".into())); }
        if let Some(state) = &self.state { state.validate(Some(&id), Some(&deployment))?; }
        if let Some(progress) = &self.discovery {
            progress.validate(Some(&id), Some(&deployment))?;
            if progress.base_checksum != self.state.as_ref().map(|s| s.checksum) {
                return Err(Error::Checkpoint("current discovery base differs from completed state".into()));
            }
        }
        if let Some(census) = &self.census {
            if census.identity != id || census.spec != deployment || census.epoch == 0 || census.target.hash.is_zero() {
                return Err(Error::Checkpoint("current census binding".into()));
            }
            let epoch = self.state.as_ref().map_or(Some(1), |s| s.snapshot.epoch.checked_add(1));
            if epoch != Some(census.epoch) { return Err(Error::Checkpoint("current census epoch".into())); }
            census.validate(&EngineConfig { commitment: census.target.commitment, max_positions: usize::MAX,
                ..EngineConfig::default() }, &CurrentStateConfig { max_event_catchup_blocks: 1, max_calendar_markets: 1_000_000 })?;
        }
        Ok(())
    }
}

/// Atomic scoped state/progress file; shares FileStore's byte caps, inode checks,
/// advisory locking, fsync, temporary-file cleanup and quarantine implementation.
#[derive(Clone, Debug)]
pub struct CurrentFileStore { inner: crate::storage::FileStore }
impl CurrentFileStore {
    /// Uses a separate filename from full SDK checkpoints.
    pub fn new(path: impl Into<PathBuf>) -> Self { Self { inner: crate::storage::FileStore::new(path) } }
    /// Positive load/save byte bound.
    pub fn with_max_bytes(mut self, cap: usize) -> Result<Self> { self.inner = self.inner.with_max_bytes(cap)?; Ok(self) }
    /// Scoped checkpoint filename.
    pub fn path(&self) -> &Path { self.inner.path() }
    /// Validates local bytes. Synchronize after restore before using the data.
    pub fn load(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<CurrentCheckpoint>> {
        let Some(bytes) = self.inner.read_bytes()? else { return Ok(None); };
        let value: CurrentCheckpoint = serde_json::from_slice(&bytes)?;
        value.validate(identity, spec)?;
        Ok(Some(value))
    }
    /// Preserves corrupt cache in a quarantine file; never touches a financial ledger.
    pub fn quarantine_invalid(&self, identity: Option<&ChainIdentity>, spec: Option<&PoolSpec>) -> Result<Option<PathBuf>> {
        self.inner.quarantine_bytes(|| self.load(identity, spec).map(|_| ()))
    }
    /// Saves completed state and unfinished units atomically; rejects a rollback
    /// of the last completed state or a foreign chain/deployment.
    pub fn save(&self, value: &CurrentCheckpoint) -> Result<()> {
        value.validate(None, None)?;
        let bytes = self.inner.encode(value)?;
        self.inner.save_bytes(&bytes, || {
            if let Some(old) = self.load(None, None)? {
                if old.binding()? != value.binding()? { return Err(Error::IdentityMismatch("current checkpoint file identity".into())); }
                if let Some(previous) = old.state {
                    let next = value.state.as_ref().ok_or_else(|| Error::Checkpoint("cannot discard completed current state".into()))?;
                    let a = previous.snapshot; let b = &next.snapshot;
                    if a.pool != b.pool { return Err(Error::IdentityMismatch("current checkpoint pool descriptor".into())); }
                    if a.epoch > b.epoch || a.epoch == b.epoch && (a.as_of.number > b.as_of.number
                        || a.as_of.number == b.as_of.number && a.as_of.hash != b.as_of.hash)
                    { return Err(Error::Checkpoint("refusing current checkpoint rollback/conflict".into())); }
                }
            }
            Ok(())
        })
    }
}

impl<R: StateReader + ?Sized> StateEngine<R> {
    pub(super) fn begin_current_census(&mut self, identity: ChainIdentity, spec: PoolSpec, target: BlockRef, generation: u64) -> Result<()> {
        let epoch = self.next_epoch()?;
        self.discovery = None;
        self.restored_metadata_check = None;
        self.current_census = Some(CurrentCensus { identity, spec, target, epoch, generation,
            schedule: None, active: None, markets: None, market_cursor: 0, pending_users: BTreeMap::new(), keys: BTreeSet::new(), stream: Some(CensusStream::default()) });
        Ok(())
    }

    pub(super) async fn advance_current_census(&mut self) -> Result<SyncStepOutcome> {
        let c = self.current_census.as_ref().ok_or_else(|| Error::Checkpoint("missing current census".into()))?;
        if self.assert_identity().await? != c.identity { return Err(Error::IdentityMismatch("current census network".into())); }
        if self.reader.block(c.target.number, c.target.commitment).await? != Some(c.target) {
            let number = c.target.number;
            self.current_census = None;
            return Err(Error::NonCanonical(number));
        }
        if self.anchored_spec(&c.target).await? != c.spec { return Err(Error::IdentityMismatch("current census deployment".into())); }
        for _ in 0..self.config.getter_batches_per_step {
            let c = self.current_census.as_ref().unwrap(); let at = c.target;
            if c.schedule.is_none() {
                let schedule = self.reader.market_schedule(self.spec.address, &at).await?;
                self.current_census.as_mut().unwrap().schedule = Some(schedule);
            } else if c.active.is_none() {
                let active = self.reader.active_markets(self.spec.address, &at).await?;
                c.schedule.as_ref().unwrap().census_count(at.timestamp, &active, 1024)?;
                self.current_census.as_mut().unwrap().active = Some(active);
            } else if c.pending_users.is_empty() && c.stream.as_ref().unwrap().next_index
                < c.schedule.as_ref().unwrap().census_count(at.timestamp, c.active.as_ref().unwrap(), 1024)? + c.active.as_ref().unwrap().len() as u64 {
                let cap = self.current_policy.as_ref().unwrap().max_calendar_markets.min(64).min(self.config.read_batch_size);
                let c = self.current_census.as_mut().unwrap();
                let count = c.schedule.as_ref().unwrap().census_count(at.timestamp, c.active.as_ref().unwrap(), 1024)?;
                let stream = c.stream.as_mut().unwrap(); stream.batch_start = stream.next_index;
                for _ in 0..cap {
                    let Some(market) = c.schedule.as_ref().unwrap().market_at(stream.next_index,count,c.active.as_ref().unwrap()) else { break; };
                    c.pending_users.insert(market,0); stream.next_index += 1;
                }
            } else if !c.pending_users.is_empty() {
                let requests: Vec<_> = c.pending_users.iter().take(self.config.read_batch_size.min(64))
                    .map(|(&market, &start)| crate::MarketUserPage { market, start, limit: self.config.reconcile_user_page_size }).collect();
                let pages = self.reader.market_user_pages(self.spec.address, &requests, &at).await?;
                if pages.len() != requests.len() { return Err(Error::Decode("incomplete current user page bundle".into())); }
                let mut added = BTreeSet::new();
                for (request, page) in requests.iter().zip(&pages) {
                    if page.len() as u64 > request.limit || page.iter().any(|a| a.is_zero()) {
                        return Err(Error::Decode("invalid current census user page".into()));
                    }
                    for &borrower in page {
                        let key = PositionKey { pool: self.spec.address, market: request.market, borrower };
                        if c.keys.contains(&key) || !added.insert(key) {
                            return Err(Error::Decode("repeated current census user".into()));
                        }
                    }
                }
                let mut digests = BTreeMap::new();
                for (request, page) in requests.iter().zip(&pages) {
                    let digest = crate::encoding::digest(page)?;
                    if !page.is_empty() && c.stream.as_ref().unwrap().last_pages.get(&request.market) == Some(&digest) {
                        return Err(Error::Decode("repeated current census user page".into()));
                    }
                    digests.insert(request.market,digest);
                }
                // Registry membership is permanent; amounts decide whether a key
                // consumes the live-position budget. No unbounded empty-key set.
                let amounts = self.read_amounts(&added, &at).await?;
                added.retain(|key| amounts.get(key).is_some_and(|v| v.debt != 0 || v.collateral != 0));
                self.assert_canonical(&at).await?;
                let c = self.current_census.as_ref().unwrap();
                if c.keys.len().saturating_add(added.len()) > self.config.max_positions {
                    return Err(Error::ResourceLimit("current census exceeds nonempty position limit".into()));
                }
                let c = self.current_census.as_mut().unwrap();
                c.keys.extend(added);
                for (request, page) in requests.iter().zip(&pages) {
                    if (page.len() as u64) < request.limit { c.pending_users.remove(&request.market); c.stream.as_mut().unwrap().last_pages.remove(&request.market); c.market_cursor += 1; }
                    else { c.stream.as_mut().unwrap().last_pages.insert(request.market,digests[&request.market]); c.pending_users.insert(request.market, request.start.checked_add(page.len() as u64)
                        .ok_or_else(|| Error::Arithmetic("current user cursor overflow".into()))?); }
                }
            } else {
                self.assert_canonical(&at).await?;
                // Enumeration can take time. Read amounts at a fresh target and
                // ingest only the bounded events produced during this census.
                let target = self.reader.head(self.config.commitment).await?;
                self.assert_target(&target).await?;
                if target.number < at.number { return Err(Error::NonCanonical(at.number)); }
                if target.number-at.number > self.current_policy.as_ref().unwrap().max_event_catchup_blocks {
                    return Err(Error::ResourceLimit("current census outlasted its event catch-up bound".into()));
                }
                let c = self.current_census.as_ref().unwrap();
                let markets: BTreeSet<_> = c.keys.iter().map(|k| k.market).chain(c.active.as_ref().unwrap().iter().copied()).collect();
                self.check_limits(c.keys.len(), markets.len())?;
                let mut progress = DiscoveryCheckpoint { schema_version: DISCOVERY_SCHEMA, identity: c.identity, spec: c.spec,
                    abi_fingerprint: ABI_FINGERPRINT, base_checksum: None, epoch: c.epoch, generation: c.generation,
                    rebuilding: self.snapshot.is_some(), from_block: c.spec.deployment_block, through: Some(at), target,
                    phase: DiscoveryPhase::Snapshot, positions: c.keys.clone(), markets, refresh_all_config: Some(true), getters: None, checksum: B256::ZERO };
                progress.seal()?;
                self.discovery = Some(progress); self.current_census = None;
                return Ok(SyncStepOutcome::Progress(self.discovery.as_ref().unwrap().status()));
            }
        }
        Ok(SyncStepOutcome::Progress(self.current_census.as_ref().unwrap().status()))
    }
}

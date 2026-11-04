//! Polls borrower positions and saves restart checkpoints.

use std::{env, io::Write, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use barterlend_state::{*, rpc::{BlockPinning, RpcConfig}, storage::{FileStore, DiscoveryFileStore}};

fn required(name: &str) -> Result<String> {
    env::var(name).map_err(|_| Error::InvalidConfig(format!("missing {name}")))
}
fn number(name: &str, default: Option<u64>) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|_| Error::InvalidConfig(format!("invalid {name}"))),
        Err(_) => default.ok_or_else(|| Error::InvalidConfig(format!("missing {name}"))),
    }
}

#[tokio::main]
async fn main() {
    if !run_with_reporting().await { std::process::exit(1); }
}

/// Summarizes errors without exposing raw provider messages.
pub(crate) async fn run_with_reporting() -> bool {
    match run().await {
        Ok(()) => true,
        Err(error) => {
            let code = match error {
                Error::Rpc { .. } => "rpc_error", Error::Transport(_) => "transport_error",
                Error::Decode(_) => "decode_error", Error::InvalidConfig(_) => "invalid_config",
                Error::MissingBlock(_) => "missing_block", Error::NonCanonical(_) => "noncanonical_block",
                Error::IdentityMismatch(_) => "identity_mismatch", Error::UnsupportedVersion(_) => "unsupported_version",
                Error::IncompleteHistory(_) => "incomplete_history", Error::ResourceLimit(_) => "resource_limit",
                Error::Arithmetic(_) => "arithmetic_error", Error::Checkpoint(_) => "checkpoint_error",
                Error::Io(_) => "io_error", Error::Json(_) => "json_error",
            };
            let _ = writeln!(std::io::stderr().lock(), "{{\"event\":\"state_watch_failed\",\"code\":\"{code}\"}}");
            false
        }
    }
}

fn redact_oracle(oracle: &mut OracleStatus) {
    if let OracleStatus::Unavailable(failure) = oracle {
        match failure {
            OracleFailure::Rpc(message) => *message = "rpc details omitted".into(),
            OracleFailure::Decode(message) => *message = "decode details omitted".into(),
            _ => {}
        }
    }
}

fn redact_position(position: &mut PositionRecord) {
    if let RiskStatus::Unavailable(failure) = &mut position.risk {
        match failure {
            RiskFailure::Oracle(OracleFailure::Rpc(message)) => *message = "rpc details omitted".into(),
            RiskFailure::Oracle(OracleFailure::Decode(message)) => *message = "decode details omitted".into(),
            RiskFailure::Arithmetic(message) => *message = "arithmetic details omitted".into(),
            RiskFailure::InvalidConfig(message) => *message = "configuration details omitted".into(),
            _ => {}
        }
    }
}

fn redact_outcome(outcome: &mut SyncOutcome) {
    match outcome {
        SyncOutcome::Bootstrapped(snapshot) | SyncOutcome::Reset(snapshot) => {
            redact_oracle(&mut snapshot.oracle);
            for position in snapshot.positions.values_mut() { redact_position(position); }
        }
        SyncOutcome::Advanced(batch) => for change in &mut batch.changes {
            match change {
                StateUpdate::OracleChanged(oracle) => redact_oracle(oracle),
                StateUpdate::PositionUpsert(position) => redact_position(position),
                _ => {}
            }
        },
        SyncOutcome::Unchanged { .. } => {}
    }
}

async fn run() -> Result<()> {
    let rpc_url = required("BL_STATE_RPC_URL")?;
    let pool: Address = required("BL_STATE_POOL")?.parse().map_err(|_| Error::InvalidConfig("invalid BL_STATE_POOL".into()))?;
    let expected_chain_id = number("BL_STATE_CHAIN_ID", None)?;
    let deployment_block = number("BL_STATE_DEPLOYMENT_BLOCK", None)?;
    let deployment_block_hash = env::var("BL_STATE_DEPLOYMENT_BLOCK_HASH").ok()
        .map(|hash| hash.parse::<B256>().map_err(|_| Error::InvalidConfig("invalid deployment block hash".into())))
        .transpose()?;
    let commitment = match env::var("BL_STATE_COMMITMENT").as_deref().unwrap_or("finalized") {
        "finalized" => Commitment::Finalized,
        "safe" => Commitment::Safe,
        "latest" => Commitment::Latest,
        _ => return Err(Error::InvalidConfig("commitment must be finalized, safe or latest".into())),
    };
    let pinning = match env::var("BL_STATE_PINNING").as_deref().unwrap_or("number_bracket") {
        "number_bracket" => BlockPinning::NumberBracket,
        "eip1898" => BlockPinning::Eip1898,
        "raw_hash" => BlockPinning::RawHash,
        _ => return Err(Error::InvalidConfig("invalid BL_STATE_PINNING".into())),
    };
    let polls = number("BL_STATE_POLLS", Some(1))?; // zero means continuous watch
    let interval = number("BL_STATE_POLL_MS", Some(1000))?;
    if interval < 200 { return Err(Error::InvalidConfig("poll interval must be >= 200 ms".into())); }
    let rpc_config = RpcConfig { pinning, ..Default::default() };
    let reader = Arc::new(HttpReader::new(&rpc_url, rpc_config)?);
    let identity = reader.identity().await?;
    if identity.chain_id != expected_chain_id { return Err(Error::IdentityMismatch("chain id differs from explicit config".into())); }
    if let Ok(expected) = env::var("BL_STATE_GENESIS_HASH") {
        let hash: B256 = expected.parse().map_err(|_| Error::InvalidConfig("invalid BL_STATE_GENESIS_HASH".into()))?;
        if identity.genesis_hash != hash { return Err(Error::IdentityMismatch("genesis hash differs".into())); }
    }
    let spec = PoolSpec { address: pool, deployment_block, expected_version: 5, deployment_block_hash };
    let config = EngineConfig { commitment, expected_identity: Some(identity), ..Default::default() };
    let path = env::var("BL_STATE_CHECKPOINT").unwrap_or_else(|_| format!("state/{}-{pool}.json", identity.chain_id));
    let store = FileStore::new(path);
    let discovery_path = env::var("BL_STATE_DISCOVERY_CHECKPOINT")
        .unwrap_or_else(|_| format!("{}.discovery.json", store.path().display()));
    if std::path::Path::new(&discovery_path) == store.path() {
        return Err(Error::InvalidConfig("state and discovery checkpoint paths must differ".into()));
    }
    let discovery_store = DiscoveryFileStore::new(discovery_path);
    let mut engine = match store.load(Some(&identity), Some(&spec))? {
        Some(checkpoint) => StateEngine::restore(reader, spec, config, checkpoint)?,
        None => StateEngine::new(reader, spec, config)?,
    };
    if let Some(progress) = discovery_store.load(Some(&identity), Some(&spec))? {
        if !engine.restore_discovery(progress)? { discovery_store.remove()?; }
    }
    let mut completed = 0;
    loop {
        // Save completed discovery chunks even if this step failed.
        let result = engine.sync_step().await;
        if let Some(progress) = engine.discovery_checkpoint()? { discovery_store.save(&progress)?; }
        let output = match result? {
            SyncStepOutcome::Progress(status) => serde_json::to_string(&SyncStepOutcome::Progress(status))?,
            SyncStepOutcome::Complete(mut outcome) => {
                store.save(&engine.checkpoint()?)?;
                discovery_store.remove()?;
                redact_outcome(&mut outcome);
                serde_json::to_string(&outcome)?
            }
        };
        if let Err(error) = writeln!(std::io::stdout().lock(), "{output}") {
            if error.kind() == std::io::ErrorKind::BrokenPipe { return Ok(()); }
            return Err(error.into());
        }
        completed += 1;
        if polls != 0 && completed >= polls { break; }
        tokio::time::sleep(Duration::from_millis(interval)).await;
    }
    Ok(())
}

//! Borrower positions, liquidation risk and restart checkpoints for BarterLend v5.

#![doc = include_str!("../README.md")]

/// Pinned CreditPool v5/oracle Solidity ABI for on-demand calls.
pub mod abi;
/// Restart-state format and local validation.
pub mod checkpoint;
mod encoding;
/// Restartable history discovery.
pub mod discovery;
/// Sequential synchronization of one pool with bounded resources.
pub mod engine;
/// Typed SDK errors.
pub mod error;
/// Events mapped to positions and configuration that need refreshing.
pub mod events;
/// Current factory pool enumeration without historical log replay.
pub mod factory;
/// Block-pinned reads and the contract for alternative state sources.
pub mod reader;
/// Protocol risk calculations with exact integer arithmetic.
pub mod risk;
/// Read-only HTTP JSON-RPC with explicit timeout/rate/concurrency limits.
pub mod rpc;
/// Synchronous atomic checkpoint persistence on a local filesystem.
pub mod storage;
/// Snapshots, positions, risk and updates.
pub mod types;

pub use checkpoint::Checkpoint;
pub use discovery::{DiscoveryCheckpoint, DiscoveryPhase, DiscoveryStatus, SyncStepOutcome};
pub use engine::{EngineConfig, StateEngine};
pub use engine::current::{CurrentCheckpoint, CurrentCheckpointBuilder, CurrentCoverage,
    CurrentFileStore, CurrentSnapshot, CurrentStateConfig, CurrentStateEngine, CurrentStep, MarketSchedule};
pub use error::{Error, Result};
pub use reader::{HttpReader, MarketUserPage, Multicall3Config, ReadOptimizationConfig, PinnedRead, StateReader};
pub use types::*;

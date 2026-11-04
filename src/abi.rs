//! CreditPool v5 and oracle ABI. Source fingerprints: `abi/PROVENANCE.json`.

alloy_sol_types::sol!(CreditPool, "abi/CreditPool.v5.json");
alloy_sol_types::sol!(IPriceOracle, "abi/IPriceOracle.v5.json");

// Solidity imports these structs into ICreditPoolTypes without a namespace. The JSON
// macro emits their definitions in CreditPool; expose the original names to generated types.
pub use CreditPool::{MarketsConfigParams, UnderlyingPoolConfig};

/// SHA-256 of `abi/CreditPool.v5.json`, used to identify compatible checkpoints.
pub const ABI_FINGERPRINT: alloy_primitives::B256 = alloy_primitives::b256!(
    "0665f58ce965b0d79117fa7411d653d2698b883d3913ed597f60021a02b4c2f7"
);
/// Protocol version supported by the pinned ABI and deterministic risk model.
pub const PROTOCOL_VERSION: u64 = 5;

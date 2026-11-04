//! SDK errors. Provider messages may contain endpoint URLs or credentials;
//! redact them before writing public logs.

/// Synchronization, configuration or storage failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP or JSON-RPC code/message; raw provider messages can contain secrets.
    #[error("RPC {code}: {message}")]
    Rpc { code: i64, message: String },
    /// Transport or timeout failure; provider text may contain sensitive details.
    #[error("transport: {0}")]
    Transport(String),
    /// Malformed JSON/ABI or inconsistent response shape.
    #[error("decode: {0}")]
    Decode(String),
    /// Invalid local settings or unsupported request policy.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// Unavailable target/header; convenience Engine methods can reanchor the lost target.
    #[error("block {0} is unavailable")]
    MissingBlock(u64),
    /// Anchor differs from the canonical header; partial state is not published.
    #[error("block {0} is not canonical")]
    NonCanonical(u64),
    /// Chain, genesis or deployment identity differs from the expected value.
    #[error("identity mismatch: {0}")]
    IdentityMismatch(String),
    /// Contract version differs from the pinned protocol v5 ABI.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u64),
    /// Required history is missing or cannot be verified.
    #[error("incomplete history: {0}")]
    IncompleteHistory(String),
    /// A configured resource budget was exhausted.
    #[error("resource limit: {0}")]
    ResourceLimit(String),
    /// Checked arithmetic/configuration intermediate is not representable.
    #[error("arithmetic: {0}")]
    Arithmetic(String),
    /// Restart schema/integrity/cursor/file policy failed validation.
    #[error("checkpoint: {0}")]
    Checkpoint(String),
    /// Filesystem failure. A failed save may have already replaced the file.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid persisted-state JSON or failed serialization.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// SDK result alias using [`Error`].
pub type Result<T> = std::result::Result<T, Error>;


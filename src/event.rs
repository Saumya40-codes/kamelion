use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Backend-neutral envelope passed from a CDC source to a publisher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub source: String,
    pub table: String,
    pub operation: Operation,
    pub key: Value,
    pub before: Option<Value>,
    pub after: Option<Value>,
    /// Source-specific position used later for checkpointing and replay.
    pub position: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

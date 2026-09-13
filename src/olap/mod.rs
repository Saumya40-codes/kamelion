mod hudi;

use anyhow::Result;
use async_trait::async_trait;

use crate::event::ChangeEvent;

pub use hudi::HudiSource;

/// Implement this contract for Hudi, Iceberg, Delta Lake, or other OLAP stores.
#[async_trait]
pub trait ChangeSource {
    async fn poll(&mut self) -> Result<Vec<ChangeEvent>>;
}

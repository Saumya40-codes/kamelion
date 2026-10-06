mod hudi;

use std::future::Future;

use anyhow::Result;

use crate::event::ChangeEvent;

pub use hudi::HudiSource;

/// Implement this contract for Hudi, Iceberg, Delta Lake, or other OLAP stores.
pub trait ChangeSource {
    fn poll(&mut self) -> impl Future<Output = Result<Vec<ChangeEvent>>> + Send;
}

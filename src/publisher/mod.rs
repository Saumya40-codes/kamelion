mod kafka;

use anyhow::Result;
use async_trait::async_trait;

use crate::event::ChangeEvent;

pub use kafka::KafkaPublisher;

#[async_trait]
pub trait ChangePublisher {
    async fn publish(&self, event: &ChangeEvent) -> Result<()>;
}

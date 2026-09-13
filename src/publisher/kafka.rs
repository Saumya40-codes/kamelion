use anyhow::Result;
use async_trait::async_trait;

use super::ChangePublisher;
use crate::{config::KafkaConfig, event::ChangeEvent};

pub struct KafkaPublisher {
    #[allow(dead_code)]
    config: KafkaConfig,
}

impl KafkaPublisher {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ChangePublisher for KafkaPublisher {
    async fn publish(&self, _event: &ChangeEvent) -> Result<()> {
        // TODO: serialize the envelope and publish it with a Kafka client.
        Ok(())
    }
}

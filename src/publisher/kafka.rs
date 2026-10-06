use anyhow::{Context, Result};
use rdkafka::{ClientConfig, producer::FutureProducer};

use super::ChangePublisher;
use crate::{config::KafkaConfig, event::ChangeEvent};

pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaPublisher {
    pub fn new(config: KafkaConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .create()
            .context("failed to create kafka producer")?;

        Ok(Self {
            producer,
            topic: config.topic,
        })
    }
}

impl ChangePublisher for KafkaPublisher {
    async fn publish(&self, _event: &ChangeEvent) -> Result<()> {
        // TODO: serialize the envelope and publish it with a Kafka client.
        Ok(())
    }
}

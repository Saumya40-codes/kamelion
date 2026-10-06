use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use serde_json::Value;

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
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("delivery.timeout.ms", "30000")
            .create()
            .context("failed to create kafka producer")?;

        Ok(Self {
            producer,
            topic: config.topic,
        })
    }
}

impl ChangePublisher for KafkaPublisher {
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        let message = encode_event(event)?;
        let record = FutureRecord::to(&self.topic)
            .key(message.key.as_slice())
            .payload(message.payload.as_slice());

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(error, _message)| error)
            .with_context(|| format!("failed to deliver CDC event to topic {}", self.topic))?;

        Ok(())
    }
}

struct EncodedEvent {
    key: Vec<u8>,
    payload: Vec<u8>,
}

#[derive(Serialize)]
struct RecordKey<'a> {
    source: &'a str,
    table: &'a str,
    key: &'a Value,
}

fn encode_event(event: &ChangeEvent) -> Result<EncodedEvent> {
    anyhow::ensure!(
        !event.key.is_null(),
        "CDC event requires a non-null record key"
    );

    Ok(EncodedEvent {
        key: serde_json::to_vec(&RecordKey {
            source: &event.source,
            table: &event.table,
            key: &event.key,
        })
        .context("failed to encode Kafka record key")?,
        payload: serde_json::to_vec(event).context("failed to encode CDC event")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Operation;
    use serde_json::json;

    fn event() -> ChangeEvent {
        ChangeEvent {
            source: "hudi".into(),
            table: "orders".into(),
            operation: Operation::Delete,
            key: json!({"id": 42}),
            before: Some(json!({"id": 42, "status": "cancelled"})),
            after: None,
            position: "commit-1".into(),
        }
    }

    #[test]
    fn encodes_delete_as_an_event_envelope() {
        let encoded = encode_event(&event()).unwrap();
        let payload: Value = serde_json::from_slice(&encoded.payload).unwrap();
        assert_eq!(
            payload,
            json!({
                "source": "hudi", "table": "orders", "operation": "delete",
                "key": {"id": 42}, "before": {"id": 42, "status": "cancelled"},
                "after": null, "position": "commit-1"
            })
        );
    }

    #[test]
    fn key_is_stable_across_changes_and_scoped_to_source_and_table() {
        let mut event = event();
        let original = encode_event(&event).unwrap().key;
        event.position = "commit-2".into();
        event.operation = Operation::Update;
        assert_eq!(original, encode_event(&event).unwrap().key);
        event.table = "customers".into();
        assert_ne!(original, encode_event(&event).unwrap().key);
        event.table = "orders".into();
        event.source = "another-source".into();
        assert_ne!(original, encode_event(&event).unwrap().key);
    }

    #[test]
    fn rejects_missing_record_keys() {
        let mut event = event();
        event.key = Value::Null;
        assert!(encode_event(&event).is_err());
    }
}

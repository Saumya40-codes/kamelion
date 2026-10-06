mod kafka;

use std::future::Future;

use anyhow::Result;

use crate::event::ChangeEvent;

pub use kafka::KafkaPublisher;

pub trait ChangePublisher {
    fn publish(&self, event: &ChangeEvent) -> impl Future<Output = Result<()>> + Send;
}

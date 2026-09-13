pub mod config;
pub mod event;
pub mod olap;
pub mod publisher;

use anyhow::Result;
use olap::ChangeSource;
use publisher::ChangePublisher;

/// Connects a source to a publisher without coupling either implementation.
pub struct Pipeline<S, P> {
    source: S,
    publisher: P,
}

impl<S, P> Pipeline<S, P>
where
    S: ChangeSource,
    P: ChangePublisher,
{
    pub fn new(source: S, publisher: P) -> Self {
        Self { source, publisher }
    }

    pub async fn run_once(mut self) -> Result<()> {
        for event in self.source.poll().await? {
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}

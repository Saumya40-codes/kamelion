use anyhow::Result;
use kamelion::{Pipeline, config::Config, olap::HudiSource, publisher::KafkaPublisher};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let source = HudiSource::new(config.hudi);
    let publisher = KafkaPublisher::new(config.kafka);

    Pipeline::new(source, publisher).run_once().await
}

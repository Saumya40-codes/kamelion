use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub hudi: HudiConfig,
    pub kafka: KafkaConfig,
}

#[derive(Debug, Clone)]
pub struct HudiConfig {
    pub table_uri: String,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            hudi: HudiConfig {
                table_uri: required_env("KAMELION_HUDI_TABLE_URI")?,
            },
            kafka: KafkaConfig {
                brokers: required_env("KAMELION_KAFKA_BROKERS")?,
                topic: required_env("KAMELION_KAFKA_TOPIC")?,
            },
        })
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

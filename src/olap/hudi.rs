use anyhow::Result;
use async_trait::async_trait;

use super::ChangeSource;
use crate::{config::HudiConfig, event::ChangeEvent};

pub struct HudiSource {
    #[allow(dead_code)]
    config: HudiConfig,
}

impl HudiSource {
    pub fn new(config: HudiConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ChangeSource for HudiSource {
    async fn poll(&mut self) -> Result<Vec<ChangeEvent>> {
        // TODO: read the Hudi timeline incrementally from the last checkpoint.
        Ok(Vec::new())
    }
}

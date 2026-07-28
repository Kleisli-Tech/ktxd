use crate::domain::{Session, TaggedItem, TurnRecord};
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait NodeSink: Send + Sync {
    async fn on_turn_committed(&self, session: &Session, record: &TurnRecord) -> Result<()>;
}

#[async_trait]
pub trait SeedResolver: Send + Sync {
    async fn resolve_seed_items(&self, _session: Option<&Session>) -> Result<Vec<TaggedItem>>;
}

#[derive(Debug, Default)]
pub struct NullSink;

#[async_trait]
impl NodeSink for NullSink {
    async fn on_turn_committed(&self, _session: &Session, _record: &TurnRecord) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NullSeedResolver;

#[async_trait]
impl SeedResolver for NullSeedResolver {
    async fn resolve_seed_items(&self, _session: Option<&Session>) -> Result<Vec<TaggedItem>> {
        Ok(Vec::new())
    }
}

use crate::domain::{Session, TurnOutcome, TurnRecord};
use crate::error::{ProxyError, Result};
use crate::ids::{ResponseId, TurnId};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn get(&self, response_id: &ResponseId) -> Result<Option<Session>>;
    async fn put(&self, session: Session) -> Result<()>;
}

#[async_trait]
pub trait TurnRecordStore: Send + Sync {
    async fn put(&self, record: TurnRecord) -> Result<()>;
    async fn get(&self, turn_id: &TurnId) -> Result<Option<TurnRecord>>;
    async fn count(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct MemoryStore {
    sessions: RwLock<BTreeMap<ResponseId, Session>>,
    responses: RwLock<BTreeMap<ResponseId, Value>>,
    turns: RwLock<BTreeMap<TurnId, TurnRecord>>,
}

impl MemoryStore {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn commit_completed(&self, session: Session, record: TurnRecord) -> Result<()> {
        if session.response_id != record.response_id {
            return Err(ProxyError::Internal(
                "session and turn response IDs do not match".to_string(),
            ));
        }
        if session.parent_response_id != record.parent_response_id {
            return Err(ProxyError::Internal(
                "session and turn parent response IDs do not match".to_string(),
            ));
        }
        if record.outcome != TurnOutcome::Completed {
            return Err(ProxyError::Internal(
                "completed commit requires a completed turn record".to_string(),
            ));
        }

        let mut sessions = self.sessions.write().await;
        let mut responses = self.responses.write().await;
        let mut turns = self.turns.write().await;
        if turns.contains_key(&record.turn_id) {
            return Err(ProxyError::Internal("duplicate turn record".to_string()));
        }
        if sessions.contains_key(&session.response_id)
            || responses.contains_key(&session.response_id)
        {
            return Err(ProxyError::Internal(
                "duplicate response record".to_string(),
            ));
        }
        responses.insert(
            session.response_id.clone(),
            session.final_response_json.clone(),
        );
        sessions.insert(session.response_id.clone(), session);
        turns.insert(record.turn_id.clone(), record);
        Ok(())
    }

    pub async fn commit_terminal(
        &self,
        response_id: ResponseId,
        response: Value,
        record: TurnRecord,
    ) -> Result<()> {
        if response_id != record.response_id {
            return Err(ProxyError::Internal(
                "response JSON and turn response IDs do not match".to_string(),
            ));
        }
        if record.outcome == TurnOutcome::Completed {
            return Err(ProxyError::Internal(
                "terminal commit cannot store a completed turn record".to_string(),
            ));
        }

        let sessions = self.sessions.read().await;
        let mut responses = self.responses.write().await;
        let mut turns = self.turns.write().await;
        if turns.contains_key(&record.turn_id) {
            return Err(ProxyError::Internal("duplicate turn record".to_string()));
        }
        if sessions.contains_key(&response_id) || responses.contains_key(&response_id) {
            return Err(ProxyError::Internal(
                "duplicate response record".to_string(),
            ));
        }
        responses.insert(response_id, response);
        turns.insert(record.turn_id.clone(), record);
        Ok(())
    }

    pub async fn get_response_json(&self, response_id: &ResponseId) -> Result<Option<Value>> {
        Ok(self.responses.read().await.get(response_id).cloned())
    }

    pub async fn put_response_json(&self, response_id: ResponseId, response: Value) -> Result<()> {
        self.responses.write().await.insert(response_id, response);
        Ok(())
    }
}

#[async_trait]
impl SessionStore for MemoryStore {
    async fn get(&self, response_id: &ResponseId) -> Result<Option<Session>> {
        Ok(self.sessions.read().await.get(response_id).cloned())
    }

    async fn put(&self, session: Session) -> Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.response_id.clone(), session);
        Ok(())
    }
}

#[async_trait]
impl TurnRecordStore for MemoryStore {
    async fn put(&self, record: TurnRecord) -> Result<()> {
        let mut turns = self.turns.write().await;
        if turns.contains_key(&record.turn_id) {
            return Err(ProxyError::Internal("duplicate turn record".to_string()));
        }
        turns.insert(record.turn_id.clone(), record);
        Ok(())
    }

    async fn get(&self, turn_id: &TurnId) -> Result<Option<TurnRecord>> {
        Ok(self.turns.read().await.get(turn_id).cloned())
    }

    async fn count(&self) -> usize {
        self.turns.read().await.len()
    }
}

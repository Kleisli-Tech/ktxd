use crate::config::AppConfig;
use crate::driver::TurnDriver;
use crate::session::MemoryStore;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub store: Arc<MemoryStore>,
    pub driver: Arc<TurnDriver>,
}

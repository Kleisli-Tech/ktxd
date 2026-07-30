use crate::error::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub models: BTreeMap<String, ModelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub public_model: String,
    #[serde(default = "default_upstream_family")]
    pub upstream_family: String,
    pub upstream_deployment: String,
    pub upstream_model: String,
    pub chat_completions_url: String,
    pub auth_header: AuthHeaderKind,
    pub auth_env_var: String,
    #[serde(default)]
    pub send_model_in_body: bool,
    #[serde(default = "default_true")]
    pub include_stream_usage: bool,
    #[serde(default = "default_true")]
    pub retry_without_stream_options_on_4xx: bool,
    #[serde(default)]
    pub instruction_role: InstructionRole,
    #[serde(default = "default_display_name")]
    pub display_name: String,
    #[serde(default = "default_description")]
    pub description: String,
    #[serde(default = "default_context_window")]
    pub context_window: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthHeaderKind {
    #[serde(rename = "api-key", alias = "api_key")]
    ApiKey,
    #[serde(rename = "authorization_bearer", alias = "authorization-bearer")]
    AuthorizationBearer,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstructionRole {
    #[default]
    System,
    Developer,
}

impl Default for AppConfig {
    fn default() -> Self {
        let model = ModelConfig::default_deepseek();
        let mut models = BTreeMap::new();
        models.insert(model.public_model.clone(), model);
        Self {
            server: ServerConfig::default(),
            models,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

impl ModelConfig {
    pub fn default_deepseek() -> Self {
        Self {
            public_model: "DeepSeek-V4-Pro".to_string(),
            upstream_family: default_upstream_family(),
            upstream_deployment: "DeepSeek-V4-Pro".to_string(),
            upstream_model: "DeepSeek-V4-Pro".to_string(),
            chat_completions_url: "http://127.0.0.1:9/openai/deployments/DeepSeek-V4-Pro/chat/completions?api-version=2024-05-01-preview".to_string(),
            auth_header: AuthHeaderKind::ApiKey,
            auth_env_var: "AZURE_AI_FOUNDRY_API_KEY".to_string(),
            send_model_in_body: false,
            include_stream_usage: true,
            retry_without_stream_options_on_4xx: true,
            instruction_role: InstructionRole::System,
            display_name: default_display_name(),
            description: default_description(),
            context_window: default_context_window(),
        }
    }

    pub fn auth_value(&self) -> Result<String> {
        env::var(&self.auth_env_var).map_err(|_| {
            ProxyError::Config(format!(
                "missing secret environment variable {}",
                self.auth_env_var
            ))
        })
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(config_path) => {
                let contents = fs::read_to_string(config_path)
                    .map_err(|error| ProxyError::Config(error.to_string()))?;
                toml::from_str(&contents).map_err(|error| ProxyError::Config(error.to_string()))
            }
            None => Ok(Self::default()),
        }
    }

    pub fn model(&self, slug: &str) -> Result<&ModelConfig> {
        self.models
            .get(slug)
            .or_else(|| {
                self.models
                    .values()
                    .find(|model| model.public_model == slug)
            })
            .ok_or_else(|| ProxyError::UnknownModel(slug.to_string()))
    }
}

fn default_true() -> bool {
    true
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:3000"
        .parse()
        .expect("valid default bind address")
}

fn default_upstream_family() -> String {
    "chat_completions".to_string()
}

fn default_display_name() -> String {
    "DeepSeek V4 Pro".to_string()
}

fn default_description() -> String {
    "DeepSeek-V4-Pro via Azure AI Foundry Chat Completions".to_string()
}

fn default_context_window() -> i64 {
    1_000_000
}

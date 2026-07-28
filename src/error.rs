use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("unsupported input item: {0}")]
    UnsupportedInputItem(String),
    #[error("unsupported tool: {0}")]
    UnsupportedTool(String),
    #[error("unknown previous_response_id: {0}")]
    PreviousResponseNotFound(String),
    #[error("unknown model: {0}")]
    UnknownModel(String),
    #[error("upstream authentication failed")]
    UpstreamAuth,
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("malformed upstream stream: {0}")]
    MalformedStream(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Debug, Serialize)]
pub struct ResponsesErrorBody {
    pub error: ResponsesError,
}

#[derive(Debug, Serialize)]
pub struct ResponsesError {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: String,
}

impl ProxyError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::UnsupportedInputItem(_) | Self::UnsupportedTool(_) | Self::UnknownModel(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::PreviousResponseNotFound(_) => StatusCode::NOT_FOUND,
            Self::UpstreamAuth => StatusCode::BAD_GATEWAY,
            Self::Upstream(_) | Self::MalformedStream(_) => StatusCode::BAD_GATEWAY,
            Self::Config(_) | Self::Serialization(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedInputItem(_) => "unsupported_input_item",
            Self::UnsupportedTool(_) => "unsupported_tool",
            Self::PreviousResponseNotFound(_) => "previous_response_not_found",
            Self::UnknownModel(_) => "unknown_model",
            Self::UpstreamAuth => "upstream_authentication_failed",
            Self::Upstream(_) => "upstream_error",
            Self::MalformedStream(_) => "malformed_stream",
            Self::Config(_) => "config_error",
            Self::Serialization(_) => "serialization_error",
            Self::Internal(_) => "internal_error",
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = ResponsesErrorBody {
            error: ResponsesError {
                message: self.to_string(),
                error_type: "invalid_request_error".to_string(),
                code: self.code().to_string(),
            },
        };
        (status, Json(body)).into_response()
    }
}

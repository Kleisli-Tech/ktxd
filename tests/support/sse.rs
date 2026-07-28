#![allow(dead_code)]

use axum::body::to_bytes;
use axum::response::Response;
use http::{HeaderMap, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct SseFrame {
    pub event: Option<String>,
    pub data: String,
    pub payload: Option<Value>,
    pub raw: String,
    pub terminated: bool,
}

impl SseFrame {
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, SseError> {
        serde_json::from_str(&self.data).map_err(SseError::Json)
    }

    pub fn is_done(&self) -> bool {
        self.data == "[DONE]"
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectedSseResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
    pub frames: Vec<SseFrame>,
    pub terminal_blank_line: bool,
}

#[derive(Debug)]
pub enum SseError {
    Body(axum::Error),
    Utf8(std::str::Utf8Error),
    Json(serde_json::Error),
    MissingData { raw: String },
}

impl fmt::Display for SseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(error) => write!(formatter, "failed to collect response body: {error}"),
            Self::Utf8(error) => write!(formatter, "SSE body was not UTF-8: {error}"),
            Self::Json(error) => write!(formatter, "invalid SSE JSON data: {error}"),
            Self::MissingData { raw } => {
                write!(formatter, "SSE record has no data field: {raw:?}")
            }
        }
    }
}

impl std::error::Error for SseError {}

pub async fn collect_sse_response(response: Response) -> Result<CollectedSseResponse, SseError> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(SseError::Body)?
        .to_vec();
    let frames = parse_sse_frames(&body)?;
    let terminal_blank_line = has_terminal_blank_line(&body)?;
    Ok(CollectedSseResponse {
        status,
        headers,
        body,
        frames,
        terminal_blank_line,
    })
}

pub async fn collect_sse_frames(response: Response) -> Result<Vec<SseFrame>, SseError> {
    Ok(collect_sse_response(response).await?.frames)
}

pub fn parse_sse_frames(body: &[u8]) -> Result<Vec<SseFrame>, SseError> {
    let text = std::str::from_utf8(body).map_err(SseError::Utf8)?;
    split_records(text)
        .into_iter()
        .filter_map(|(raw, terminated)| {
            if raw.trim().is_empty() || is_comment_only(raw) {
                return None;
            }
            Some(parse_frame(raw, terminated))
        })
        .collect()
}

pub fn has_terminal_blank_line(body: &[u8]) -> Result<bool, SseError> {
    let text = std::str::from_utf8(body).map_err(SseError::Utf8)?;
    Ok(split_records(text)
        .last()
        .is_some_and(|(_, terminated)| *terminated))
}

fn split_records(text: &str) -> Vec<(&str, bool)> {
    let bytes = text.as_bytes();
    let mut records = Vec::new();
    let mut record_start = 0;
    let mut cursor = 0;
    while cursor < bytes.len() {
        let Some(first_len) = line_ending_len(bytes, cursor) else {
            cursor += 1;
            continue;
        };
        let next_line = cursor + first_len;
        if let Some(second_len) = line_ending_len(bytes, next_line) {
            records.push((&text[record_start..cursor], true));
            record_start = next_line + second_len;
            cursor = record_start;
        } else {
            cursor = next_line;
        }
    }
    if record_start < text.len() {
        records.push((&text[record_start..], false));
    }
    records
}

fn line_ending_len(bytes: &[u8], index: usize) -> Option<usize> {
    match bytes.get(index) {
        Some(b'\n') | Some(b'\r') => {
            if bytes.get(index) == Some(&b'\r') && bytes.get(index + 1) == Some(&b'\n') {
                Some(2)
            } else {
                Some(1)
            }
        }
        _ => None,
    }
}

fn parse_frame(raw: &str, terminated: bool) -> Result<SseFrame, SseError> {
    let normalized = raw.replace("\r\n", "\n").replace('\r', "\n");
    let mut event = None;
    let mut data_lines = Vec::new();
    for line in normalized.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.strip_prefix(' ').unwrap_or(value).to_string());
        }
    }
    if data_lines.is_empty() {
        return Err(SseError::MissingData {
            raw: raw.to_string(),
        });
    }
    let data = data_lines.join("\n");
    let payload = if data == "[DONE]" {
        None
    } else {
        Some(serde_json::from_str(&data).map_err(SseError::Json)?)
    };
    Ok(SseFrame {
        event,
        data,
        payload,
        raw: raw.to_string(),
        terminated,
    })
}

fn is_comment_only(raw: &str) -> bool {
    let normalized = raw.replace("\r\n", "\n").replace('\r', "\n");
    normalized
        .lines()
        .filter(|line| !line.trim().is_empty())
        .all(|line| line.starts_with(':'))
}

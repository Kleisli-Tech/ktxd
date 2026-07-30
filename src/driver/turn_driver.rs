use crate::config::AppConfig;
use crate::domain::{
    Session, TaggedItem, TurnOutcome, TurnRecord, UsageTotals, blake3_hash, sha256_hex_of_canonical,
};
use crate::error::Result;
use crate::ids::{ArtifactHash, ResponseId, SessionVersion, TurnId};
use crate::responses::{
    ResponseEvent, ResponseEventSink, base_response, completed_event, completed_response_object,
    created_event, failed_event, with_sequence_number,
};
use crate::session::MemoryStore;
use crate::stream::{StreamTerminal, translate_non_streaming_response, translate_stream_chunks};
use crate::substrate::{NodeSink, SeedResolver};
use crate::translator::{NormalizedTurnInput, compile_chat_request};
use crate::upstream::ChatCompletions;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct TurnDriver {
    config: Arc<AppConfig>,
    upstream: Arc<dyn ChatCompletions>,
    store: Arc<MemoryStore>,
    node_sink: Arc<dyn NodeSink>,
    seed_resolver: Arc<dyn SeedResolver>,
}

impl TurnDriver {
    pub fn new(
        config: Arc<AppConfig>,
        upstream: Arc<dyn ChatCompletions>,
        store: Arc<MemoryStore>,
        node_sink: Arc<dyn NodeSink>,
        seed_resolver: Arc<dyn SeedResolver>,
    ) -> Self {
        Self {
            config,
            upstream,
            store,
            node_sink,
            seed_resolver,
        }
    }

    pub async fn drive(
        &self,
        parent: Option<Session>,
        normalized: NormalizedTurnInput,
        sink: &mut dyn ResponseEventSink,
    ) -> Result<TurnRecord> {
        let response_id = ResponseId::new();
        let turn_id = TurnId::new();
        let parent_response_id = parent.as_ref().map(|session| session.response_id.clone());
        let mut sequence_number = 0_u64;
        let model_config = match self.config.model(&normalized.model) {
            Ok(model_config) => model_config,
            Err(error) => {
                let code = error.code().to_string();
                if normalized.stream {
                    emit_sequenced(
                        sink,
                        &mut sequence_number,
                        failed_event(&response_id, &normalized.model, &code, &error.to_string()),
                    )
                    .await;
                }
                return self
                    .put_terminal_record(
                        response_id,
                        turn_id,
                        &normalized.model,
                        parent_response_id,
                        TurnOutcome::Failed,
                        normalized.request_items,
                        Vec::new(),
                        UsageTotals::default(),
                        Some(code),
                        Some(error.to_string()),
                        None,
                    )
                    .await;
            }
        };
        if normalized.stream {
            emit_sequenced(
                sink,
                &mut sequence_number,
                created_event(&response_id, &normalized.model),
            )
            .await;
        }

        let mut transcript = parent
            .as_ref()
            .map(|session| session.committed_items.clone())
            .unwrap_or_default();
        let seed_items = match self.seed_resolver.resolve_seed_items(parent.as_ref()).await {
            Ok(seed_items) => seed_items,
            Err(error) => {
                let code = error.code().to_string();
                if normalized.stream {
                    emit_sequenced(
                        sink,
                        &mut sequence_number,
                        failed_event(&response_id, &normalized.model, &code, &error.to_string()),
                    )
                    .await;
                }
                return self
                    .put_terminal_record(
                        response_id,
                        turn_id,
                        &normalized.model,
                        parent_response_id,
                        TurnOutcome::Failed,
                        normalized.request_items,
                        Vec::new(),
                        UsageTotals::default(),
                        Some(code),
                        Some(error.to_string()),
                        None,
                    )
                    .await;
            }
        };
        transcript.extend(seed_items);
        transcript.extend(normalized.request_items.clone());

        let chat_request =
            match compile_chat_request(model_config, &transcript, &normalized, normalized.stream) {
                Ok(chat_request) => chat_request,
                Err(error) => {
                    let code = error.code().to_string();
                    if normalized.stream {
                        emit_sequenced(
                            sink,
                            &mut sequence_number,
                            failed_event(
                                &response_id,
                                &normalized.model,
                                &code,
                                &error.to_string(),
                            ),
                        )
                        .await;
                    }
                    return self
                        .put_terminal_record(
                            response_id,
                            turn_id,
                            &normalized.model,
                            parent_response_id,
                            TurnOutcome::Failed,
                            normalized.request_items,
                            Vec::new(),
                            UsageTotals::default(),
                            Some(code),
                            Some(error.to_string()),
                            None,
                        )
                        .await;
                }
            };

        if normalized.stream {
            match self.upstream.stream(model_config, chat_request).await {
                Ok(chunks) => {
                    let translation =
                        match translate_stream_chunks(&response_id, &normalized.model, chunks) {
                            Ok(translation) => translation,
                            Err(error) => {
                                let code = error.code().to_string();
                                emit_sequenced(
                                    sink,
                                    &mut sequence_number,
                                    failed_event(
                                        &response_id,
                                        &normalized.model,
                                        &code,
                                        &error.to_string(),
                                    ),
                                )
                                .await;
                                return self
                                    .put_terminal_record(
                                        response_id,
                                        turn_id,
                                        &normalized.model,
                                        parent_response_id,
                                        TurnOutcome::Failed,
                                        normalized.request_items,
                                        Vec::new(),
                                        UsageTotals::default(),
                                        Some(code),
                                        Some(error.to_string()),
                                        None,
                                    )
                                    .await;
                            }
                        };
                    for event in translation.events {
                        emit_sequenced(sink, &mut sequence_number, event).await;
                    }
                    match translation.terminal {
                        StreamTerminal::Completed => {
                            let commit_result = self
                                .commit_completed(
                                    &normalized.model,
                                    normalized.instructions.clone(),
                                    normalized.tools.clone(),
                                    parent,
                                    response_id.clone(),
                                    turn_id.clone(),
                                    parent_response_id.clone(),
                                    normalized.request_items.clone(),
                                    transcript,
                                    translation.output_items,
                                    translation.usage,
                                )
                                .await;
                            match commit_result {
                                Ok(record) => {
                                    emit_sequenced(
                                        sink,
                                        &mut sequence_number,
                                        completed_event(
                                            &response_id,
                                            &normalized.model,
                                            &record.output_items,
                                            &record.usage,
                                        ),
                                    )
                                    .await;
                                    Ok(record)
                                }
                                Err(error) => {
                                    let code = error.code().to_string();
                                    emit_sequenced(
                                        sink,
                                        &mut sequence_number,
                                        failed_event(
                                            &response_id,
                                            &normalized.model,
                                            &code,
                                            &error.to_string(),
                                        ),
                                    )
                                    .await;
                                    self.put_terminal_record(
                                        response_id,
                                        turn_id,
                                        &normalized.model,
                                        parent_response_id,
                                        TurnOutcome::Failed,
                                        normalized.request_items,
                                        Vec::new(),
                                        UsageTotals::default(),
                                        Some(code),
                                        Some(error.to_string()),
                                        None,
                                    )
                                    .await
                                }
                            }
                        }
                        StreamTerminal::Incomplete(reason) => {
                            self.put_terminal_record(
                                response_id,
                                turn_id,
                                &normalized.model,
                                parent_response_id,
                                TurnOutcome::Incomplete,
                                normalized.request_items,
                                translation.output_items,
                                translation.usage,
                                Some(reason),
                                None,
                                None,
                            )
                            .await
                        }
                        StreamTerminal::Failed(reason) => {
                            emit_sequenced(
                                sink,
                                &mut sequence_number,
                                failed_event(
                                    &response_id,
                                    &normalized.model,
                                    "stream_failed",
                                    &reason,
                                ),
                            )
                            .await;
                            self.put_terminal_record(
                                response_id,
                                turn_id,
                                &normalized.model,
                                parent_response_id,
                                TurnOutcome::Failed,
                                normalized.request_items,
                                Vec::new(),
                                UsageTotals::default(),
                                Some("stream_failed".to_string()),
                                Some(reason),
                                None,
                            )
                            .await
                        }
                    }
                }
                Err(error) => {
                    let code = error.code().to_string();
                    emit_sequenced(
                        sink,
                        &mut sequence_number,
                        failed_event(&response_id, &normalized.model, &code, &error.to_string()),
                    )
                    .await;
                    self.put_terminal_record(
                        response_id,
                        turn_id,
                        &normalized.model,
                        parent_response_id,
                        TurnOutcome::Failed,
                        normalized.request_items,
                        Vec::new(),
                        UsageTotals::default(),
                        Some(code),
                        Some(error.to_string()),
                        None,
                    )
                    .await
                }
            }
        } else {
            match self.upstream.complete(model_config, chat_request).await {
                Ok(response) => {
                    let (output_items, usage, terminal) =
                        match translate_non_streaming_response(response) {
                            Ok(translated) => translated,
                            Err(error) => {
                                let code = error.code().to_string();
                                return self
                                    .put_terminal_record(
                                        response_id,
                                        turn_id,
                                        &normalized.model,
                                        parent_response_id,
                                        TurnOutcome::Failed,
                                        normalized.request_items,
                                        Vec::new(),
                                        UsageTotals::default(),
                                        Some(code),
                                        Some(error.to_string()),
                                        None,
                                    )
                                    .await;
                            }
                        };
                    match terminal {
                        StreamTerminal::Completed => {
                            let commit_result = self
                                .commit_completed(
                                    &normalized.model,
                                    normalized.instructions.clone(),
                                    normalized.tools.clone(),
                                    parent,
                                    response_id.clone(),
                                    turn_id.clone(),
                                    parent_response_id.clone(),
                                    normalized.request_items.clone(),
                                    transcript,
                                    output_items,
                                    usage,
                                )
                                .await;
                            match commit_result {
                                Ok(record) => Ok(record),
                                Err(error) => {
                                    let code = error.code().to_string();
                                    self.put_terminal_record(
                                        response_id,
                                        turn_id,
                                        &normalized.model,
                                        parent_response_id,
                                        TurnOutcome::Failed,
                                        normalized.request_items,
                                        Vec::new(),
                                        UsageTotals::default(),
                                        Some(code),
                                        Some(error.to_string()),
                                        None,
                                    )
                                    .await
                                }
                            }
                        }
                        StreamTerminal::Incomplete(reason) => {
                            self.put_terminal_record(
                                response_id,
                                turn_id,
                                &normalized.model,
                                parent_response_id,
                                TurnOutcome::Incomplete,
                                normalized.request_items,
                                output_items,
                                usage,
                                Some(reason),
                                None,
                                None,
                            )
                            .await
                        }
                        StreamTerminal::Failed(reason) => {
                            self.put_terminal_record(
                                response_id,
                                turn_id,
                                &normalized.model,
                                parent_response_id,
                                TurnOutcome::Failed,
                                normalized.request_items,
                                Vec::new(),
                                usage,
                                Some(reason),
                                None,
                                None,
                            )
                            .await
                        }
                    }
                }
                Err(error) => {
                    let code = error.code().to_string();
                    self.put_terminal_record(
                        response_id,
                        turn_id,
                        &normalized.model,
                        parent_response_id,
                        TurnOutcome::Failed,
                        normalized.request_items,
                        Vec::new(),
                        UsageTotals::default(),
                        Some(code),
                        Some(error.to_string()),
                        None,
                    )
                    .await
                }
            }
        }
    }

    async fn commit_completed(
        &self,
        model: &str,
        instructions: String,
        tools: Vec<Value>,
        parent: Option<Session>,
        response_id: ResponseId,
        turn_id: TurnId,
        parent_response_id: Option<ResponseId>,
        request_items: Vec<TaggedItem>,
        mut transcript: Vec<TaggedItem>,
        mut output_items: Vec<TaggedItem>,
        usage: UsageTotals,
    ) -> Result<TurnRecord> {
        for output_item in &mut output_items {
            output_item.artifact_hash =
                Some(ArtifactHash::from_string(blake3_hash(&output_item.item)?));
        }
        transcript.extend(output_items.clone());
        let fingerprint = sha256_hex_of_canonical(&json!({
            "instructions": instructions,
            "tools": tools,
            "items": transcript
                .iter()
                .map(|item| json!({
                    "item": &item.item,
                    "provenance": &item.provenance,
                }))
                .collect::<Vec<_>>(),
        }))?;
        let response_object = completed_response_object(&response_id, model, &output_items, &usage);
        let final_response_json = serde_json::to_value(response_object)
            .map_err(|error| crate::error::ProxyError::Serialization(error.to_string()))?;
        let record = TurnRecord {
            turn_id,
            response_id: response_id.clone(),
            parent_response_id: parent_response_id.clone(),
            outcome: TurnOutcome::Completed,
            request_items,
            output_items,
            usage,
            error_code: None,
            error_message: None,
            deterministic_fingerprint: Some(fingerprint.clone()),
        };
        let (tenant_id, version) = parent
            .map(|session| (session.tenant_id, SessionVersion(session.version.0 + 1)))
            .unwrap_or_default();
        let session = Session {
            response_id,
            parent_response_id,
            tenant_id,
            version,
            committed_items: transcript,
            deterministic_fingerprint: fingerprint,
            final_response_json,
        };
        self.store
            .commit_completed(session.clone(), record.clone())
            .await?;
        if let Err(error) = self.node_sink.on_turn_committed(&session, &record).await {
            tracing::warn!(error = %error, "node sink failed after commit");
        }
        Ok(record)
    }

    async fn put_terminal_record(
        &self,
        response_id: ResponseId,
        turn_id: TurnId,
        model: &str,
        parent_response_id: Option<ResponseId>,
        outcome: TurnOutcome,
        request_items: Vec<TaggedItem>,
        output_items: Vec<TaggedItem>,
        usage: UsageTotals,
        error_code: Option<String>,
        error_message: Option<String>,
        deterministic_fingerprint: Option<String>,
    ) -> Result<TurnRecord> {
        let record = TurnRecord {
            turn_id,
            response_id: response_id.clone(),
            parent_response_id,
            outcome,
            request_items,
            output_items,
            usage,
            error_code,
            error_message,
            deterministic_fingerprint,
        };
        let response = self.non_streaming_response(model, &record);
        self.store
            .commit_terminal(response_id, response, record.clone())
            .await?;
        Ok(record)
    }

    pub fn non_streaming_response(&self, model: &str, record: &TurnRecord) -> Value {
        match record.outcome {
            TurnOutcome::Completed => response_to_value(completed_response_object(
                &record.response_id,
                model,
                &record.output_items,
                &record.usage,
            )),
            TurnOutcome::Incomplete => response_to_value(base_response(
                &record.response_id,
                model,
                "incomplete",
                record
                    .output_items
                    .iter()
                    .map(crate::responses::tagged_item_to_response_json)
                    .collect(),
                Some(&record.usage),
                record.error_code.as_deref(),
            )),
            TurnOutcome::Failed | TurnOutcome::Cancelled => {
                let mut response = base_response(
                    &record.response_id,
                    model,
                    "failed",
                    Vec::new(),
                    Some(&record.usage),
                    None,
                );
                if let Some(code) = record.error_code.as_deref() {
                    response.error = Some(json!({
                        "code": code,
                        "message": record.error_message.as_deref().unwrap_or(code),
                    }));
                }
                response_to_value(response)
            }
        }
    }
}

fn response_to_value(response: crate::wire::responses::ResponseObject) -> Value {
    serde_json::to_value(response).unwrap_or_else(|_| serde_json::json!({}))
}

async fn emit_sequenced(
    sink: &mut dyn ResponseEventSink,
    sequence_number: &mut u64,
    event: ResponseEvent,
) {
    let sequenced = with_sequence_number(event, *sequence_number);
    *sequence_number += 1;
    sink.emit(sequenced).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::responses::{VecEventSink, output_text_delta_event};

    #[tokio::test]
    async fn emit_sequenced_assigns_contiguous_sequence_numbers() {
        let response_id = ResponseId::from_string("resp_test");
        let mut sequence_number = 0;
        let mut sink = VecEventSink::default();

        emit_sequenced(
            &mut sink,
            &mut sequence_number,
            output_text_delta_event(&response_id, "item_test", 0, "first"),
        )
        .await;
        emit_sequenced(
            &mut sink,
            &mut sequence_number,
            output_text_delta_event(&response_id, "item_test", 0, "second"),
        )
        .await;

        assert_eq!(sequence_number, 2);
        assert_eq!(sink.events.len(), 2);
        assert_eq!(sink.events[0].data["sequence_number"], 0);
        assert_eq!(sink.events[1].data["sequence_number"], 1);
        assert_eq!(sink.events[0].data["delta"], "first");
        assert_eq!(sink.events[1].data["delta"], "second");
    }
}

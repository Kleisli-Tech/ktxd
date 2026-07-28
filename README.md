# Context-D (`ktxd`)

> A local context bridge for Codex CLI and Chat Completions backends.

**Context-D** is the project name. **`ktxd`** is its working handle — used across the crate, executable, config files, logs, and Codex provider key. Same project, just the name it travels under.

Context-D lets Codex CLI use an upstream Chat Completions model through the OpenAI Responses API shape that Codex expects. It translates `/v1/responses` requests into upstream Chat Completions requests, normalizes responses back into Responses objects or Server-Sent Events (SSE), and keeps completed responses in memory so `previous_response_id` can work across turns.

The first supported target is `DeepSeek-V4-Pro` on Azure AI Foundry.

## At a glance

| | |
| --- | --- |
| Project | Context-D |
| Command and package | `ktxd` |
| Primary client | Codex CLI |
| Client-facing API | OpenAI Responses API |
| Upstream compatibility | Chat Completions |
| First-class target | `DeepSeek-V4-Pro` on Azure AI Foundry |
| Runtime | Local Rust proxy |

## How it fits together

```text
Codex CLI
    │  Responses API: /v1/responses
    ▼
Context-D (`ktxd`)
    │  request translation, response normalization,
    │  SSE conversion, and in-memory continuation state
    ▼
Chat Completions backend
```

## Contents

- [Why this exists](#why-this-exists)
- [Current status](#current-status)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configure Codex CLI](#configure-codex-cli)
- [Function/tool-call smoke test](#functiontool-call-smoke-test)
- [Configuration reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Project layout](#project-layout)
- [Security notes](#security-notes)

## Why this exists

Codex CLI expects a Responses-compatible provider. Many useful hosted models, including Azure AI Foundry serverless models, expose Chat Completions-compatible endpoints instead. Context-D bridges that gap while preserving the Responses-facing contract Codex uses.

Use it when you want to:

- Run Codex CLI against `DeepSeek-V4-Pro` or another Chat Completions backend.
- Keep Codex on the `/v1/responses` wire API while adapting the upstream request format locally.
- Test Responses-style SSE output, function/tool calls, and `previous_response_id` behavior before wiring a provider directly into Codex.
- Provide Codex model metadata through `model_catalog_json` so Codex does not fall back to degraded unknown-model defaults.

## Current status

Implemented today:

- `GET /healthz`
- `GET /v1/models`
- `POST /v1/responses`
- `GET /v1/responses/{response_id}`
- Non-streaming Responses output
- Responses-style SSE output for streamed requests
- In-memory response/session storage
- `previous_response_id` continuation for completed responses
- Function tool definitions and `function_call_output` follow-up turns
- Azure-style auth headers and Azure v1 Chat Completions endpoints

Known limitations:

- Storage is in-memory only. Restarting the proxy forgets response IDs.
- Only function tools are supported. Tools such as `web_search` are rejected intentionally.
- Streamed upstream responses are fully buffered before their chunks are normalized and emitted as Responses SSE events; this is not low-latency pass-through streaming.
- The first-class target is currently `DeepSeek-V4-Pro`; other models may need config and metadata tuning.
- `upstream_family` and `upstream_deployment` are currently required configuration fields but do not select an adapter or construct an upstream URL. The current implementation always uses the Chat Completions client configured by `chat_completions_url`.

## Prerequisites

- Rust with edition 2024 support.
- A reachable Chat Completions endpoint. The examples below use Azure AI Foundry.
- An upstream credential, such as an API key or bearer token.
- `curl` and `jq` for the quick smoke tests.
- Codex CLI if you want to use the proxy from Codex.

## Quick start

### 1. Configure the proxy

Copy the example config and edit it for your upstream endpoint:

```bash
cd ktxd
cp config.example.toml config.toml
$EDITOR config.toml
```

For Azure AI Foundry `DeepSeek-V4-Pro`, the most important fields are:

```toml
[server]
bind = "127.0.0.1:3000"

[models.DeepSeek-V4-Pro]
public_model = "DeepSeek-V4-Pro"
upstream_family = "chat_completions"
upstream_deployment = "DeepSeek-V4-Pro"
upstream_model = "DeepSeek-V4-Pro"

# Azure AI Foundry v1 Chat Completions endpoint.
# Replace <resource> and <region> with your Foundry resource host.
chat_completions_url = "https://<resource>.<region>.services.ai.azure.com/openai/v1/chat/completions"

auth_header = "api-key"
auth_env_var = "AZURE_AI_FOUNDRY_API_KEY"

# Azure v1 endpoints commonly require the model/deployment in the JSON body.
send_model_in_body = true
include_stream_usage = true
retry_without_stream_options_on_4xx = true
instruction_role = "system"

context_window = 1000000
```

Set your upstream key in the environment. The variable name must match `auth_env_var`:

```bash
export AZURE_AI_FOUNDRY_API_KEY="<your-api-key>"
```

### 2. Run the proxy

```bash
export KTXD_CONFIG="$PWD/config.toml"
RUST_LOG=ktxd=info,tower_http=info cargo run
```

You should see a log line like:

```text
ktxd listening bind=127.0.0.1:3000
```

### 3. Smoke test the API

Check health:

```bash
curl -sS http://127.0.0.1:3000/healthz | jq .
```

List models:

```bash
curl -sS http://127.0.0.1:3000/v1/models | jq .
```

Run a non-streaming Responses request:

```bash
curl -sS http://127.0.0.1:3000/v1/responses \
  -H 'content-type: application/json' \
  -d '{
    "model": "DeepSeek-V4-Pro",
    "instructions": "Be concise.",
    "input": "Reply with exactly: pong",
    "stream": false
  }' | jq .
```

Expected shape:

```json
{
  "object": "response",
  "model": "DeepSeek-V4-Pro",
  "status": "completed",
  "output": [
    {
      "type": "message",
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "pong"
        }
      ]
    }
  ]
}
```

Run the same request as streaming SSE:

```bash
curl -N http://127.0.0.1:3000/v1/responses \
  -H 'content-type: application/json' \
  -d '{
    "model": "DeepSeek-V4-Pro",
    "instructions": "Be concise.",
    "input": "Reply with exactly: pong",
    "stream": true
  }'
```

You should see events such as `response.created`, `response.output_item.added`, `response.output_text.delta`, `response.output_item.done`, and `response.completed`. The proxy buffers the upstream stream first, so these events are emitted after the upstream response has been collected rather than incrementally as upstream chunks arrive.

### 4. Test response retrieval and continuation

Store the response ID from a completed response:

```bash
FIRST=$(curl -sS http://127.0.0.1:3000/v1/responses \
  -H 'content-type: application/json' \
  -d '{
    "model": "DeepSeek-V4-Pro",
    "instructions": "Be concise.",
    "input": "Remember this favorite flower: ORCHID. Reply exactly: remembered",
    "stream": false
  }')

RESP_ID=$(echo "$FIRST" | jq -r .id)
echo "$RESP_ID"
```

Retrieve it:

```bash
curl -sS "http://127.0.0.1:3000/v1/responses/$RESP_ID" | jq .
```

Continue from it:

```bash
curl -sS http://127.0.0.1:3000/v1/responses \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg previous_response_id "$RESP_ID" '{
    model: "DeepSeek-V4-Pro",
    previous_response_id: $previous_response_id,
    instructions: "Be concise.",
    input: "What is my favorite flower? Reply with only the flower name.",
    stream: false
  }')" | jq .
```

## Configure Codex CLI

Codex needs two things:

1. A provider entry that points Codex at `ktxd`.
2. Model metadata for `DeepSeek-V4-Pro`, otherwise Codex prints a warning like:

```text
⚠ Model metadata for `DeepSeek-V4-Pro` not found. Defaulting to fallback metadata; this can degrade performance and cause issues.
```

The metadata is supplied through `model_catalog_json`. The included catalog advertises DeepSeek-V4-Pro's 1M context window while intentionally capping any single tool/function output at 50K tokens through `truncation_policy`; that keeps runaway command output from crowding out the rest of the conversation.

### 1. Install the model catalog

Copy the example catalog into your Codex home:

```bash
mkdir -p ~/.codex/model-catalogs
cp examples/codex/model-catalogs/ktxd.json ~/.codex/model-catalogs/ktxd.json
```

Use an absolute path when referencing this file from Codex config. For example:

```text
/Users/alice/.codex/model-catalogs/ktxd.json
```

### 2. Add the provider to `~/.codex/config.toml`

Add this provider block to your user-level Codex config:

```toml
[model_providers.ktxd]
name = "Context-D (ktxd) proxy"
base_url = "http://127.0.0.1:3000/v1"
wire_api = "responses"
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
stream_idle_timeout_ms = 300000
```

Keep provider configuration in user-level `~/.codex/config.toml`. Project-scoped `.codex/config.toml` files are useful for project behavior, but provider definitions are machine-local settings.

### 3. Add a Codex profile

Codex profile loading changed across releases, so check `codex --version` and choose the layout that matches your installed version. If a profile does not load or the metadata warning remains, try the other layout and restart Codex.

#### Codex releases using standalone profile files

Create `~/.codex/ktxd.config.toml`:

```toml
model = "DeepSeek-V4-Pro"
model_provider = "ktxd"
model_catalog_json = "/Users/alice/.codex/model-catalogs/ktxd.json"
# DeepSeek-V4-Pro is exposed here without Codex reasoning metadata.
model_reasoning_effort = "none"
model_reasoning_summary = "none"
web_search = "disabled"
```

Then run:

```bash
codex --profile ktxd
```

For non-interactive smoke testing:

```bash
codex exec --profile ktxd \
  --skip-git-repo-check \
  --sandbox read-only \
  "what is your model identity?"
```

A healthy response should identify as Codex running on `DeepSeek-V4-Pro` via `ktxd`, and the metadata warning should be gone.

#### Codex releases using profile tables

Some Codex builds read profile tables from `~/.codex/config.toml`. If the standalone profile file does not load, put this in `~/.codex/config.toml` instead:

```toml
[profiles.ktxd]
model = "DeepSeek-V4-Pro"
model_provider = "ktxd"
model_catalog_json = "/Users/alice/.codex/model-catalogs/ktxd.json"
# DeepSeek-V4-Pro is exposed here without Codex reasoning metadata.
model_reasoning_effort = "none"
model_reasoning_summary = "none"
web_search = "disabled"
```

Then run the same command:

```bash
codex --profile ktxd
```

### Using a project-local Codex home for testing

If you want to test without touching your real `~/.codex`, create a local Codex home and launch Codex with `CODEX_HOME`.

For Codex releases that use standalone profile files, keep the provider in `.codex/config.toml` and put the profile in `.codex/ktxd.config.toml`:

```bash
mkdir -p .codex/model-catalogs
cp examples/codex/model-catalogs/ktxd.json .codex/model-catalogs/ktxd.json
CATALOG_PATH="$PWD/.codex/model-catalogs/ktxd.json"

cat > .codex/config.toml <<'TOML'
[model_providers.ktxd]
name = "Context-D (ktxd) proxy"
base_url = "http://127.0.0.1:3000/v1"
wire_api = "responses"
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
stream_idle_timeout_ms = 300000
TOML

cat > .codex/ktxd.config.toml <<TOML
model = "DeepSeek-V4-Pro"
model_provider = "ktxd"
model_catalog_json = "$CATALOG_PATH"
model_reasoning_effort = "none"
model_reasoning_summary = "none"
web_search = "disabled"
TOML

CODEX_HOME="$PWD/.codex" codex exec --profile ktxd \
  --skip-git-repo-check \
  --sandbox read-only \
  "what is your model identity?"
```

For Codex releases that use profile tables, put the profile table in `.codex/config.toml` instead:

```toml
[profiles.ktxd]
model = "DeepSeek-V4-Pro"
model_provider = "ktxd"
model_catalog_json = "/absolute/path/to/.codex/model-catalogs/ktxd.json"
model_reasoning_effort = "none"
model_reasoning_summary = "none"
web_search = "disabled"
```

## Function/tool-call smoke test

`ktxd` supports Responses function tools and converts them to Chat Completions tool calls.

```bash
curl -sS http://127.0.0.1:3000/v1/responses \
  -H 'content-type: application/json' \
  -d '{
    "model": "DeepSeek-V4-Pro",
    "input": "Use the tool to look up the status for ticket ABC-123.",
    "tools": [
      {
        "type": "function",
        "name": "lookup_ticket",
        "description": "Look up a ticket by ID.",
        "parameters": {
          "type": "object",
          "properties": {
            "ticket_id": { "type": "string" }
          },
          "required": ["ticket_id"],
          "additionalProperties": false
        }
      }
    ],
    "tool_choice": "auto",
    "stream": false
  }' | jq .
```

If the model calls the tool, send the result back using `previous_response_id` and an input item of type `function_call_output`:

```json
{
  "model": "DeepSeek-V4-Pro",
  "previous_response_id": "resp_...",
  "input": [
    {
      "type": "function_call_output",
      "call_id": "call_...",
      "output": "Ticket ABC-123 is open and assigned to Support."
    }
  ]
}
```

## Configuration reference

### Proxy config

`KTXD_CONFIG` points the binary at a TOML config file. If it is not set, the binary uses built-in defaults suitable for local startup and configuration validation, but not for real upstream calls.

When neither config environment variable is set, the binary checks the current directory for `config.toml` and then `config.local.toml`.

```bash
export KTXD_CONFIG="$PWD/config.toml"
```

Supported model config fields:

| Field | Purpose |
| --- | --- |
| `public_model` | Model name accepted from Codex and returned by `/v1/models`. |
| `display_name` | Human-readable model name returned by `/v1/models`. |
| `description` | Model description returned by `/v1/models`. |
| `upstream_family` | Required compatibility field. Only `chat_completions` is implemented; this field does not currently select an adapter. |
| `upstream_deployment` | Required compatibility field retained for deployment metadata; it is not currently used to construct the URL or request. |
| `upstream_model` | Model value sent upstream when `send_model_in_body = true`. |
| `chat_completions_url` | Full upstream Chat Completions URL. |
| `auth_header` | `api-key` or `authorization_bearer`. |
| `auth_env_var` | Environment variable that stores the upstream secret. |
| `send_model_in_body` | Include `model` in the upstream JSON body. Useful for Azure v1 endpoints. |
| `include_stream_usage` | Request upstream stream usage when supported. |
| `retry_without_stream_options_on_4xx` | Retry streaming without `stream_options` if the upstream rejects that field. |
| `instruction_role` | Lower Responses `instructions` as `system` or `developer`. |
| `context_window` | Metadata returned from `/v1/models`. |

### Environment variables

| Variable | Purpose |
| --- | --- |
| `KTXD_CONFIG` | Path to proxy TOML config. |
| `AZURE_AI_FOUNDRY_API_KEY` | Example upstream API key variable. Rename via `auth_env_var` if needed. |
| `RUST_LOG` | Enables Rust/tracing logs, for example `ktxd=debug,tower_http=info`. |
| `CODEX_HOME` | Optional Codex config directory for isolated testing. |

## Troubleshooting

### Codex still prints the model metadata warning

Check these items:

- `model_catalog_json` is set in the active profile or top-level Codex config.
- The path is absolute.
- The file exists and contains a model with `"slug": "DeepSeek-V4-Pro"`.
- Your installed Codex version is using the profile layout you edited: either standalone `~/.codex/ktxd.config.toml` or `[profiles.ktxd]` in `~/.codex/config.toml`.
- Restart Codex after changing config.

### Upstream returns `API version not supported`

Use the Azure AI Foundry v1 Chat Completions endpoint shape:

```toml
chat_completions_url = "https://<resource>.<region>.services.ai.azure.com/openai/v1/chat/completions"
send_model_in_body = true
```

Do not append an unsupported `api-version` query string to the v1 endpoint.

### `unknown previous_response_id`

The proxy stores sessions in memory. The response ID must come from a completed response created by the currently running proxy process. If you restart the proxy, old IDs are gone.

### `unsupported tool: web_search`

Only `function` tools are currently supported. Disable Codex web search in the `ktxd` profile:

```toml
web_search = "disabled"
```

### `missing secret environment variable`

Set the environment variable named by `auth_env_var` before starting the proxy:

```bash
export AZURE_AI_FOUNDRY_API_KEY="<your-api-key>"
```

### Port already in use

Change the bind address in `config.toml`:

```toml
[server]
bind = "127.0.0.1:3001"
```

Then update the Codex provider `base_url` accordingly:

```toml
base_url = "http://127.0.0.1:3001/v1"
```

## Development

Run the current test target (there are currently no repository tests):

```bash
cargo test
```

Run pre-PR checks before publishing changes:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test
```

Run with debug logs:

```bash
RUST_LOG=ktxd=debug,tower_http=debug cargo run
```

## Project layout

```text
src/
├── capability
│   └── mod.rs                            Supported upstream capability types
├── domain
│   ├── hash.rs                           Canonical serialization and content hashes
│   ├── items.rs                          Tagged messages, tool calls, and provenance
│   ├── mod.rs                            Canonical domain model exports
│   └── session.rs                        Sessions, transcripts, turns, and usage
├── driver
│   ├── mod.rs                            Turn driver exports
│   └── turn_driver.rs                    Turn orchestration and response persistence
├── policy
│   └── mod.rs                            Static route policy placeholder
├── responses
│   ├── events.rs                         Responses objects and SSE event construction
│   ├── handlers.rs                       HTTP routes and endpoint handling
│   └── mod.rs                            Responses API module exports
├── session
│   └── mod.rs                            In-memory session and response store
├── stream
│   └── mod.rs                            Stream translation exports
├── substrate
│   └── mod.rs                            Node sink and seed resolver interfaces
├── translator
│   ├── chat_compiler.rs                  Responses-to-Chat-Completions compiler
│   ├── chat_stream.rs                    Chat-Completions-to-Responses translation
│   ├── mod.rs                            Translation module exports
│   └── responses_normalizer.rs           Responses request normalization
├── upstream
│   └── mod.rs                            Reqwest Chat Completions client
├── wire
│   ├── chat.rs                           Chat Completions request/response schemas
│   ├── mod.rs                            Wire schema module exports
│   └── responses.rs                      Responses API request/response schemas
├── app_state.rs                          Shared application state
├── config.rs                             Proxy and model configuration
├── error.rs                              Error types and HTTP error responses
├── ids.rs                                Typed response, turn, item, and tenant IDs
├── lib.rs                                Library module exports
└── main.rs                               Binary entry point and server startup
```

## Security notes

- Bind to `127.0.0.1` for local Codex use. Exposing the proxy on a network interface should be done only behind appropriate authentication and network controls.
- The proxy forwards prompts and tool outputs to your configured upstream provider. Review your provider's data handling terms before sending sensitive code or secrets.

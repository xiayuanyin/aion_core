use std::sync::{Arc, Mutex};

use aion_config::config::Config;
use aion_providers::{LlmProvider, ProviderError, create_provider};
use aion_types::llm::{LlmEvent, LlmRequest, ThinkingConfig};
use aion_types::message::{ContentBlock, Role};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tracing::debug;

const PROMPT_TRACE_ENV: &str = "AIONUI_PROMPT_TRACE";

pub(super) fn create_provider_with_input_trace(config: &Config, conversation_id: String) -> Arc<dyn LlmProvider> {
    let provider = create_provider(config);
    if std::env::var(PROMPT_TRACE_ENV).as_deref() != Ok("1") {
        return provider;
    }

    Arc::new(ProviderInputTrace::new(provider, conversation_id))
}

struct ProviderInputTrace {
    inner: Arc<dyn LlmProvider>,
    conversation_id: String,
    previous_input: Mutex<String>,
}

impl ProviderInputTrace {
    fn new(inner: Arc<dyn LlmProvider>, conversation_id: String) -> Self {
        Self {
            inner,
            conversation_id,
            previous_input: Mutex::new(String::new()),
        }
    }

    fn compare_with_previous(&self, current_input: String) -> InputComparison {
        let mut previous_input = self
            .previous_input
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let repeated_prefix_bytes = common_prefix_bytes(&previous_input, &current_input);
        let comparison = InputComparison {
            previous_input: previous_input.clone(),
            current_input,
            repeated_prefix_bytes,
        };
        previous_input.clone_from(&comparison.current_input);
        comparison
    }
}

#[async_trait::async_trait]
impl LlmProvider for ProviderInputTrace {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let comparison = self.compare_with_previous(provider_input_string(request));
        debug!(
            conversation_id = %self.conversation_id,
            current_input = %comparison.current_input,
            previous_input = %comparison.previous_input,
            current_input_bytes = comparison.current_input.len(),
            previous_input_bytes = comparison.previous_input.len(),
            repeated_prefix_bytes = comparison.repeated_prefix_bytes,
            "DEV Aionrs provider input comparison"
        );

        self.inner.stream(request).await
    }
}

struct InputComparison {
    previous_input: String,
    current_input: String,
    repeated_prefix_bytes: usize,
}

fn provider_input_string(request: &LlmRequest) -> String {
    let system = serde_json::to_string(&request.system).expect("serializing a string cannot fail");
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "deferred": tool.deferred,
            })
        })
        .collect::<Vec<_>>();
    let messages = request
        .messages
        .iter()
        .map(|message| {
            json!({
                "role": role_name(message.role),
                "content": message.content.iter().map(content_block_value).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let options = json!({
        "model": request.model,
        "max_tokens": request.max_tokens,
        "thinking": thinking_value(request.thinking.as_ref()),
        "reasoning_effort": request.reasoning_effort,
    });

    format!(
        "{{\"system\":{system},\"tools\":{},\"messages\":{},\"options\":{options}}}",
        Value::Array(tools),
        Value::Array(messages),
    )
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => "system",
        Role::Tool => "tool",
    }
}

fn content_block_value(block: &ContentBlock) -> Value {
    serde_json::to_value(block).expect("ContentBlock serialization cannot fail")
}

fn thinking_value(thinking: Option<&ThinkingConfig>) -> Value {
    match thinking {
        Some(ThinkingConfig::Enabled { budget_tokens }) => json!({
            "enabled": true,
            "budget_tokens": budget_tokens,
        }),
        Some(ThinkingConfig::Disabled) => json!({ "enabled": false }),
        None => Value::Null,
    }
}

fn common_prefix_bytes(left: &str, right: &str) -> usize {
    left.bytes().zip(right.bytes()).take_while(|(a, b)| a == b).count()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use aion_types::message::Message;

    fn request_with_message(timestamped: bool) -> LlmRequest {
        let mut message = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "test input".to_owned(),
            }],
        );
        if timestamped {
            message.timestamp = Some(Utc::now());
        }

        LlmRequest {
            model: "gpt-test".to_owned(),
            system: "system prompt".to_owned(),
            messages: vec![message],
            tools: Vec::new(),
            max_tokens: None,
            thinking: None,
            reasoning_effort: None,
        }
    }

    #[test]
    fn provider_input_string_excludes_local_message_timestamp() {
        assert_eq!(
            provider_input_string(&request_with_message(false)),
            provider_input_string(&request_with_message(true))
        );
    }

    #[test]
    fn common_prefix_is_counted_in_utf8_bytes() {
        assert_eq!(common_prefix_bytes("same-中文-a", "same-中文-b"), 12);
        assert_eq!(common_prefix_bytes("abc", "xyz"), 0);
    }
}

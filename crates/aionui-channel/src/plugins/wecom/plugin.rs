use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aes::Aes256;
use aes::cipher::{BlockDecrypt, KeyInit, generic_array::GenericArray};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, warn};

use crate::error::ChannelError;
use crate::plugin::{ChannelPlugin, PluginCallbacks};
use crate::types::{
    ActionButton, BotInfo, MessageContentType, OutgoingMessageType, PluginConfig, PluginStatus, PluginType,
    UnifiedAttachment, UnifiedIncomingMessage, UnifiedMessageContent, UnifiedOutgoingMessage, UnifiedUser,
};

const WS_URL: &str = "wss://openws.work.weixin.qq.com";
const HEARTBEAT: Duration = Duration::from_secs(30);
const RESPONSE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const MAX_MESSAGE_CHARS: usize = 4096;
const MAX_DEDUP_ENTRIES: usize = 2048;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

struct Outbound {
    chat_id: String,
    req_id: String,
    force_active: bool,
    /// Existing stream id when this is a refresh through `edit_message`.
    stream_id: Option<String>,
    message: UnifiedOutgoingMessage,
    result: oneshot::Sender<Result<String, ChannelError>>,
}

struct PendingOutbound {
    sent_at: Instant,
    result: oneshot::Sender<Result<String, ChannelError>>,
    stream_id: String,
}

#[derive(Clone)]
struct RequestContext {
    req_id: String,
}

struct ConnectionContext {
    bot_id: String,
    secret: String,
    ws_url: String,
    callbacks: PluginCallbacks,
    status: Arc<AtomicU8>,
    last_error: Arc<Mutex<Option<String>>>,
    welcome_message: Option<String>,
}

/// Enterprise WeCom AI Bot long-connection plugin.
pub struct WecomPlugin {
    status: Arc<AtomicU8>,
    bot_info: Option<BotInfo>,
    last_error: Arc<Mutex<Option<String>>>,
    bot_id: Option<String>,
    secret: Option<String>,
    ws_url: Option<String>,
    callbacks: Option<PluginCallbacks>,
    out_tx: Option<mpsc::Sender<Outbound>>,
    ws_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    welcome_message: Option<String>,
}

impl Default for WecomPlugin {
    fn default() -> Self {
        Self {
            status: Arc::new(AtomicU8::new(status_code(PluginStatus::Created))),
            bot_info: None,
            last_error: Arc::new(Mutex::new(None)),
            bot_id: None,
            secret: None,
            ws_url: None,
            callbacks: None,
            out_tx: None,
            ws_handle: None,
            shutdown_tx: None,
            welcome_message: None,
        }
    }
}

impl WecomPlugin {
    pub fn new() -> Self {
        Self::default()
    }

    fn set_status(&self, status: PluginStatus, callbacks: &mpsc::UnboundedSender<PluginStatus>) {
        self.status.store(status_code(status), Ordering::Release);
        let _ = callbacks.send(status);
    }

    /// Sends an `aibot_send_msg` push without requiring a preceding callback.
    /// The platform still requires that the user has previously contacted the bot.
    pub async fn send_active_message(
        &self,
        chat_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<String, ChannelError> {
        self.enqueue_outbound(chat_id, message, true).await
    }

    async fn enqueue_outbound(
        &self,
        chat_id: &str,
        message: UnifiedOutgoingMessage,
        force_active: bool,
    ) -> Result<String, ChannelError> {
        let request = latest_request(chat_id);
        let req_id = if force_active {
            next_id("send")
        } else {
            request
                .as_ref()
                .map(|request| request.req_id.clone())
                .unwrap_or_else(|| next_id("send"))
        };
        let out_tx = self
            .out_tx
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom plugin is not connected".into()))?;
        let (result_tx, result_rx) = oneshot::channel();
        out_tx
            .send(Outbound {
                chat_id: chat_id.to_owned(),
                req_id,
                force_active,
                stream_id: None,
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom connection is stopping".into()))?;
        result_rx
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom send task stopped".into()))?
    }
}

#[async_trait::async_trait]
impl ChannelPlugin for WecomPlugin {
    async fn initialize(&mut self, config: PluginConfig, callbacks: PluginCallbacks) -> Result<(), ChannelError> {
        self.set_status(PluginStatus::Initializing, &callbacks.status_tx);
        let bot_id = config
            .credentials
            .extra
            .get("bot_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| self.fail(&callbacks.status_tx, "Missing WeCom bot_id"))?;
        let secret = config
            .credentials
            .extra
            .get("secret")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .or(config.credentials.token.as_deref())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| self.fail(&callbacks.status_tx, "Missing WeCom secret"))?;

        self.bot_id = Some(bot_id.to_owned());
        self.bot_info = Some(BotInfo {
            id: bot_id.to_owned(),
            username: None,
            display_name: "WeCom AI Bot".into(),
        });
        // Keep the secret only in the spawned task. It is never logged or exposed
        // through BotInfo/status responses.
        self.secret = Some(secret.to_owned());
        self.ws_url = config
            .config
            .as_ref()
            .and_then(|options| {
                options
                    .extra
                    .get("websocket_url")
                    .or_else(|| options.extra.get("ws_url"))
            })
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.welcome_message = config
            .config
            .as_ref()
            .and_then(|options| options.extra.get("welcome_message"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let status_tx = callbacks.status_tx.clone();
        self.callbacks = Some(callbacks);
        self.set_status(PluginStatus::Ready, &status_tx);
        Ok(())
    }

    async fn start(&mut self) -> Result<(), ChannelError> {
        if self.ws_handle.is_some() {
            return Ok(());
        }
        let bot_id = self
            .bot_id
            .clone()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom plugin not initialized".into()))?;
        let secret = self
            .secret
            .take()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom credentials not initialized".into()))?;
        let ws_url = self.ws_url.clone().unwrap_or_else(|| WS_URL.into());
        let callbacks = self
            .callbacks
            .take()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom callbacks not initialized".into()))?;
        let (out_tx, out_rx) = mpsc::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.out_tx = Some(out_tx);
        self.shutdown_tx = Some(shutdown_tx);
        self.status
            .store(status_code(PluginStatus::Starting), Ordering::Release);
        let _ = callbacks.status_tx.send(PluginStatus::Starting);
        let context = ConnectionContext {
            bot_id,
            secret,
            ws_url,
            callbacks,
            status: Arc::clone(&self.status),
            last_error: Arc::clone(&self.last_error),
            welcome_message: self.welcome_message.clone(),
        };
        self.ws_handle = Some(tokio::spawn(connection_loop(context, out_rx, shutdown_rx)));
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ChannelError> {
        self.status
            .store(status_code(PluginStatus::Stopping), Ordering::Release);
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(mut handle) = self.ws_handle.take()
            && tokio::time::timeout(Duration::from_secs(5), &mut handle).await.is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        self.out_tx = None;
        self.callbacks = None;
        self.status.store(status_code(PluginStatus::Stopped), Ordering::Release);
        Ok(())
    }

    async fn send_message(&self, chat_id: &str, message: UnifiedOutgoingMessage) -> Result<String, ChannelError> {
        self.enqueue_outbound(chat_id, message, false).await
    }

    async fn send_active_message(
        &self,
        chat_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<String, ChannelError> {
        self.enqueue_outbound(chat_id, message, true).await
    }

    async fn edit_message(
        &self,
        chat_id: &str,
        message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        let request = latest_request(chat_id);
        let req_id = request
            .as_ref()
            .map(|request| request.req_id.clone())
            .unwrap_or_else(|| next_id("send"));
        let out_tx = self
            .out_tx
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom plugin is not connected".into()))?;
        let (result_tx, result_rx) = oneshot::channel();
        out_tx
            .send(Outbound {
                chat_id: chat_id.to_owned(),
                req_id,
                force_active: false,
                stream_id: Some(message_id.to_owned()),
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom connection is stopping".into()))?;
        let _ = result_rx
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom send task stopped".into()))??;
        Ok(())
    }

    fn active_user_count(&self) -> usize {
        0
    }
    fn bot_info(&self) -> Option<&BotInfo> {
        self.bot_info.as_ref()
    }
    fn plugin_type(&self) -> PluginType {
        PluginType::Wecom
    }
    fn status(&self) -> PluginStatus {
        status_from_code(self.status.load(Ordering::Acquire))
    }
    fn last_error(&self) -> Option<&str> {
        None
    }
}

impl WecomPlugin {
    fn fail(&self, callbacks: &mpsc::UnboundedSender<PluginStatus>, message: &str) -> ChannelError {
        self.status.store(status_code(PluginStatus::Error), Ordering::Release);
        if let Ok(mut error) = self.last_error.lock() {
            *error = Some(message.into());
        }
        let _ = callbacks.send(PluginStatus::Error);
        ChannelError::InvalidConfig(message.into())
    }
}

async fn connection_loop(
    context: ConnectionContext,
    mut out_rx: mpsc::Receiver<Outbound>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut failures = 0u32;
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        match connect_once(&context, &mut out_rx, &mut shutdown_rx).await {
            Ok(()) => failures = 0,
            Err(error) => {
                failures = failures.saturating_add(1);
                if let Ok(mut last) = context.last_error.lock() {
                    *last = Some(error.to_string());
                }
                context
                    .status
                    .store(status_code(PluginStatus::Starting), Ordering::Release);
                let _ = context.callbacks.status_tx.send(PluginStatus::Starting);
                if failures >= MAX_RECONNECT_ATTEMPTS {
                    break;
                }
                let delay = Duration::from_secs(2u64.saturating_pow(failures.min(5)));
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = shutdown_rx.changed() => break,
                }
            }
        }
    }
    if !*shutdown_rx.borrow() {
        context
            .status
            .store(status_code(PluginStatus::Error), Ordering::Release);
        let _ = context.callbacks.status_tx.send(PluginStatus::Error);
    }
}

async fn connect_once(
    context: &ConnectionContext,
    out_rx: &mut mpsc::Receiver<Outbound>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), ChannelError> {
    let connector = tls_connector()?;
    let (stream, _) = tokio_tungstenite::connect_async_tls_with_config(&context.ws_url, None, false, Some(connector))
        .await
        .map_err(|error| ChannelError::ConnectionFailed(format!("WeCom WebSocket connect failed: {error}")))?;
    let (mut write, mut read) = stream.split();
    let auth_req_id = next_id("subscribe");
    write
        .send(WsMessage::Text(
            serde_json::to_string(&serde_json::json!({
                "cmd": "aibot_subscribe", "headers": {"req_id": auth_req_id},
                "body": {"bot_id": context.bot_id, "secret": context.secret}
            }))
            .expect("valid subscribe frame")
            .into(),
        ))
        .await
        .map_err(|error| ChannelError::ConnectionFailed(format!("WeCom auth send failed: {error}")))?;

    let mut authenticated = false;
    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let pending_deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(pending_deadline);
    let mut seen: HashMap<String, Instant> = HashMap::new();
    let mut pending: HashMap<String, Vec<PendingOutbound>> = HashMap::new();
    let mut ack_timer = tokio::time::interval(Duration::from_secs(1));
    ack_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        expire_pending(&mut pending);
        tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),
            _ = &mut pending_deadline, if !authenticated => {
                return Err(ChannelError::ConnectionFailed("WeCom authentication timed out".into()));
            }
            _ = ack_timer.tick(), if !pending.is_empty() => expire_pending(&mut pending),
            _ = heartbeat.tick(), if authenticated => {
                let frame = serde_json::json!({"cmd":"ping", "headers":{"req_id":next_id("ping")}});
                if write.send(WsMessage::Text(frame.to_string().into())).await.is_err() { return Err(ChannelError::ConnectionFailed("WeCom heartbeat failed".into())); }
            }
            outbound = out_rx.recv() => {
                if let Some(outbound) = outbound {
                    let request = (!outbound.force_active).then(|| latest_request(&outbound.chat_id)).flatten();
                    let result = send_outbound(&mut write, &outbound, request).await;
                    match result {
                        Ok(stream_id) => {
                            pending.entry(outbound.req_id).or_default().push(PendingOutbound {
                                sent_at: Instant::now(),
                                result: outbound.result,
                                stream_id,
                            });
                        }
                        Err(error) => { let _ = outbound.result.send(Err(error)); }
                    }
                } else { return Ok(()); }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(WsMessage::Text(text))) => {
                        let frame: serde_json::Value = match serde_json::from_str(&text) { Ok(value) => value, Err(_) => continue };
                        let req_id = frame.pointer("/headers/req_id").and_then(serde_json::Value::as_str).unwrap_or("");
                        if req_id != auth_req_id
                            && let Some(errcode) = frame.get("errcode").and_then(serde_json::Value::as_i64)
                        {
                            if let Some(requests) = pending.remove(req_id) {
                                for request in requests {
                                    let result = if errcode == 0 {
                                        Ok(request.stream_id)
                                    } else {
                                        Err(ChannelError::MessageSendFailed(format!(
                                            "WeCom response rejected (errcode={errcode})"
                                        )))
                                    };
                                    let _ = request.result.send(result);
                                }
                            }
                            if errcode != 0 {
                                warn!(errcode, "WeCom server rejected a WebSocket request");
                            }
                            if frame.get("cmd").is_none() {
                                continue;
                            }
                        }
                        if req_id != auth_req_id && frame.get("errcode").is_some() && frame.get("cmd").is_none() {
                            debug!("WeCom WebSocket request acknowledged");
                            continue;
                        }
                        if req_id == auth_req_id {
                            let errcode = frame.get("errcode").and_then(serde_json::Value::as_i64).unwrap_or(-1);
                            if errcode != 0 { return Err(ChannelError::ConnectionFailed(format!("WeCom authentication rejected (errcode={errcode})"))); }
                            authenticated = true;
                            context.status.store(status_code(PluginStatus::Running), Ordering::Release);
                            let _ = context.callbacks.status_tx.send(PluginStatus::Running);
                            continue;
                        }
                        if frame.get("cmd").and_then(serde_json::Value::as_str) == Some("ping") {
                            let pong_req_id = if req_id.is_empty() { next_id("pong") } else { req_id.to_owned() };
                            let pong = serde_json::json!({
                                "cmd": "pong",
                                "headers": {"req_id": pong_req_id},
                            });
                            if write.send(WsMessage::Text(pong.to_string().into())).await.is_err() {
                                return Err(ChannelError::ConnectionFailed("WeCom pong failed".into()));
                            }
                            continue;
                        }
                        let cmd = frame.get("cmd").and_then(serde_json::Value::as_str).unwrap_or("");
                        if (cmd == "aibot_msg_callback" || cmd == "aibot_event_callback") && authenticated {
                            if let Some(mut message) = parse_incoming(&frame, &mut seen) {
                                prepare_media_attachments(&frame, &mut message).await;
                                remember_request(&message.chat_id, req_id);
                                let _ = context.callbacks.message_tx.send(message).await;
                            } else if cmd == "aibot_event_callback" && mark_event_seen(&frame, &mut seen) {
                                handle_event_callback(&mut write, context, &frame, req_id).await?;
                            }
                        }
                    }
                    Some(Ok(WsMessage::Ping(data))) => { let _ = write.send(WsMessage::Pong(data)).await; }
                    Some(Ok(WsMessage::Close(_))) | None => return Err(ChannelError::ConnectionFailed("WeCom WebSocket closed".into())),
                    Some(Err(error)) => return Err(ChannelError::ConnectionFailed(format!("WeCom WebSocket read failed: {error}"))),
                    _ => {}
                }
            }
        }
    }
}

fn expire_pending(pending: &mut HashMap<String, Vec<PendingOutbound>>) {
    let now = Instant::now();
    let expired: Vec<String> = pending
        .iter()
        .filter(|(_, requests)| {
            requests
                .iter()
                .any(|request| now.duration_since(request.sent_at) >= RESPONSE_ACK_TIMEOUT)
        })
        .map(|(req_id, _)| req_id.clone())
        .collect();
    for req_id in expired {
        if let Some(requests) = pending.remove(&req_id) {
            for request in requests {
                let _ = request.result.send(Err(ChannelError::MessageSendFailed(
                    "WeCom response confirmation timed out".into(),
                )));
            }
        }
    }
}

async fn send_outbound<S>(
    write: &mut S,
    outbound: &Outbound,
    request: Option<RequestContext>,
) -> Result<String, ChannelError>
where
    S: futures_util::Sink<WsMessage> + Unpin,
    <S as futures_util::Sink<WsMessage>>::Error: std::fmt::Display,
{
    let text = outbound.message.text.as_deref().unwrap_or("");
    let stream_id = outbound.stream_id.clone().unwrap_or_else(|| next_id("stream"));
    if request.is_none() {
        let body = active_push_body(&message_body(&outbound.message)?, &outbound.message, &outbound.chat_id);
        let frame = serde_json::json!({
            "cmd": "aibot_send_msg",
            "headers": {"req_id": outbound.req_id},
            "body": body,
        });
        write
            .send(WsMessage::Text(frame.to_string().into()))
            .await
            .map_err(|error| ChannelError::MessageSendFailed(format!("WeCom response send failed: {error}")))?;
    } else if outbound.stream_id.is_some()
        || matches!(
            outbound.message.message_type,
            crate::types::OutgoingMessageType::Text | crate::types::OutgoingMessageType::Buttons
        )
    {
        let chunks = split_text(text, MAX_MESSAGE_CHARS);
        for (index, chunk) in chunks.iter().enumerate() {
            let finish = index + 1 == chunks.len();
            let body = if outbound.message.parse_mode.is_some() {
                serde_json::json!({"msgtype":"markdown", "markdown":{"content":chunk}})
            } else {
                serde_json::json!({"msgtype":"stream", "stream":{"id":stream_id,"finish":finish,"content":chunk}})
            };
            let frame = serde_json::json!({
                "cmd": "aibot_respond_msg",
                "headers": {"req_id": outbound.req_id},
                "body": body,
            });
            write
                .send(WsMessage::Text(frame.to_string().into()))
                .await
                .map_err(|error| ChannelError::MessageSendFailed(format!("WeCom response send failed: {error}")))?;
        }
    } else {
        let body = message_body(&outbound.message)?;
        let frame = serde_json::json!({
            "cmd": "aibot_respond_msg", "headers": {"req_id": outbound.req_id}, "body": body
        });
        write
            .send(WsMessage::Text(frame.to_string().into()))
            .await
            .map_err(|error| ChannelError::MessageSendFailed(format!("WeCom response send failed: {error}")))?;
    }
    Ok(if outbound.force_active {
        outbound.req_id.clone()
    } else {
        stream_id
    })
}

fn parse_incoming(frame: &serde_json::Value, seen: &mut HashMap<String, Instant>) -> Option<UnifiedIncomingMessage> {
    let body = frame.get("body")?;
    let msgid = body.get("msgid")?.as_str()?.to_owned();
    let msgtype = body.get("msgtype").and_then(serde_json::Value::as_str).unwrap_or("");
    if msgtype != "event" && !mark_event_seen(frame, seen) {
        return None;
    }
    let (text, attachments, content_type) = match msgtype {
        "text" => (
            body.pointer("/text/content")
                .and_then(serde_json::Value::as_str)?
                .to_owned(),
            None,
            MessageContentType::Text,
        ),
        "voice" => (
            body.pointer("/voice/content")
                .and_then(serde_json::Value::as_str)?
                .to_owned(),
            None,
            MessageContentType::Voice,
        ),
        "image" => (
            "[图片]".into(),
            media_attachment(body, "image", "image/*"),
            MessageContentType::Photo,
        ),
        "file" => (
            "[文件]".into(),
            media_attachment(body, "file", "application/octet-stream"),
            MessageContentType::Document,
        ),
        "video" => (
            "[视频]".into(),
            media_attachment(body, "video", "video/*"),
            MessageContentType::Video,
        ),
        "mixed" => parse_mixed(body)?,
        _ => return None,
    };
    let userid = body
        .pointer("/from/userid")
        .and_then(serde_json::Value::as_str)?
        .to_owned();
    let chat_id = body
        .get("chatid")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .unwrap_or(&userid)
        .to_owned();
    let display_name = body
        .pointer("/from/name")
        .and_then(serde_json::Value::as_str)
        .or_else(|| body.pointer("/from/display_name").and_then(serde_json::Value::as_str))
        .unwrap_or(&userid)
        .to_owned();
    let timestamp = body
        .get("create_time")
        .and_then(serde_json::Value::as_i64)
        .map(|value| if value < 1_000_000_000_000 { value * 1000 } else { value })
        .unwrap_or_else(now_ms);
    Some(UnifiedIncomingMessage {
        id: msgid,
        platform: PluginType::Wecom,
        chat_id,
        user: UnifiedUser {
            id: userid,
            username: None,
            display_name,
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type,
            text,
            attachments,
        },
        timestamp,
        reply_to_message_id: None,
        action: None,
        raw: Some(frame.clone()),
    })
}

fn mark_event_seen(frame: &serde_json::Value, seen: &mut HashMap<String, Instant>) -> bool {
    let Some(msgid) = frame.pointer("/body/msgid").and_then(serde_json::Value::as_str) else {
        return true;
    };
    let now = Instant::now();
    seen.retain(|_, value| now.duration_since(*value) < Duration::from_secs(3600));
    if seen.insert(msgid.to_owned(), now).is_some() {
        return false;
    }
    if seen.len() > MAX_DEDUP_ENTRIES
        && let Some(key) = seen.keys().next().cloned()
    {
        seen.remove(&key);
    }
    true
}

fn media_attachment(body: &serde_json::Value, kind: &str, mime_type: &str) -> Option<Vec<UnifiedAttachment>> {
    let media = body.get(kind)?;
    let url = media.get("url").and_then(serde_json::Value::as_str)?.to_owned();
    Some(vec![UnifiedAttachment {
        file_id: media
            .get("media_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        file_name: media
            .get("filename")
            .or_else(|| media.get("file_name"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        mime_type: Some(mime_type.into()),
        file_size: media.get("filesize").and_then(serde_json::Value::as_u64),
        url: Some(url),
    }])
}

const MAX_MEDIA_BYTES: usize = 50 * 1024 * 1024;

/// Download and decrypt WeCom media before handing the message to the channel
/// orchestrator. The remote URL is short-lived, so it must never be deferred
/// to the UI.
async fn prepare_media_attachments(frame: &serde_json::Value, message: &mut UnifiedIncomingMessage) {
    let Some(attachments) = message.content.attachments.as_mut() else {
        return;
    };
    let message_id = message.id.clone();
    let Some(body) = frame.get("body") else { return };
    let mut media_items = Vec::new();
    match body.get("msgtype").and_then(serde_json::Value::as_str) {
        Some("image") => body.get("image").into_iter().for_each(|value| media_items.push(value)),
        Some("file") => body.get("file").into_iter().for_each(|value| media_items.push(value)),
        Some("video") => body.get("video").into_iter().for_each(|value| media_items.push(value)),
        Some("mixed") => {
            if let Some(items) = body.pointer("/mixed/msg_item").and_then(serde_json::Value::as_array) {
                for item in items {
                    if matches!(
                        item.get("msgtype").and_then(serde_json::Value::as_str),
                        Some("image" | "file" | "video")
                    ) {
                        let key = item.get("msgtype").and_then(serde_json::Value::as_str).unwrap();
                        if let Some(value) = item.get(key) {
                            media_items.push(value);
                        }
                    }
                }
            }
        }
        _ => {}
    }

    let mut prepared = Vec::with_capacity(attachments.len());
    for attachment in attachments.drain(..) {
        let media = media_items
            .iter()
            .find(|item| item.get("url").and_then(serde_json::Value::as_str) == attachment.url.as_deref());
        let Some(media) = media else { continue };
        let Some(url) = attachment.url.as_deref() else { continue };
        let Some(aeskey) = media.get("aeskey").and_then(serde_json::Value::as_str) else {
            warn!("WeCom media did not include aeskey");
            continue;
        };
        match download_and_decrypt_media(url, aeskey, attachment.file_name.as_deref(), &message_id).await {
            Ok((path, size, detected_mime)) => {
                let mut attachment = attachment;
                attachment.url = Some(path);
                attachment.file_size = Some(size);
                if attachment.mime_type.as_deref().is_some_and(|mime| mime.ends_with("/*")) {
                    attachment.mime_type = detected_mime.or(attachment.mime_type);
                }
                prepared.push(attachment);
            }
            Err(error) => warn!(error = %error, "failed to download WeCom media"),
        }
    }
    *attachments = prepared;
}

async fn download_and_decrypt_media(
    url: &str,
    aeskey: &str,
    file_name: Option<&str>,
    message_id: &str,
) -> Result<(String, u64, Option<String>), String> {
    let response = reqwest::get(url)
        .await
        .map_err(|error| format!("download failed: {error}"))?;
    if let Some(length) = response.content_length()
        && length > MAX_MEDIA_BYTES as u64
    {
        return Err("media exceeds 50 MiB limit".into());
    }
    let encrypted = response
        .bytes()
        .await
        .map_err(|error| format!("read download failed: {error}"))?;
    if encrypted.len() > MAX_MEDIA_BYTES {
        return Err("media exceeds 50 MiB limit".into());
    }
    let decrypted = decrypt_media(&encrypted, aeskey)?;
    let mime = detect_media_mime(&decrypted);
    let name = safe_media_name(file_name, mime.as_deref(), message_id);
    let dir = std::env::temp_dir()
        .join("aionui")
        .join("wecom")
        .join(safe_component(message_id));
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("create attachment directory failed: {error}"))?;
    let (stem, extension) = Path::new(&name)
        .file_stem()
        .and_then(|value| value.to_str())
        .map(|stem| {
            (
                stem.to_owned(),
                Path::new(&name)
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(str::to_owned),
            )
        })
        .unwrap_or_else(|| (name.clone(), None));
    for suffix in 0..1000u16 {
        let candidate = match (&extension, suffix) {
            (Some(extension), 0) => format!("{stem}.{extension}"),
            (Some(extension), suffix) => format!("{stem}-{suffix}.{extension}"),
            (None, 0) => stem.clone(),
            (None, suffix) => format!("{stem}-{suffix}"),
        };
        let path = dir.join(candidate);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&decrypted)
                    .await
                    .map_err(|error| format!("save attachment failed: {error}"))?;
                return Ok((path.to_string_lossy().into_owned(), decrypted.len() as u64, mime));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("save attachment failed: {error}")),
        }
    }
    Err("could not allocate a unique attachment path".into())
}

fn decrypt_media(encrypted: &[u8], aeskey: &str) -> Result<Vec<u8>, String> {
    let key = decode_aes_key(aeskey)?;
    if encrypted.is_empty() || !encrypted.len().is_multiple_of(16) {
        return Err("encrypted media has invalid block length".into());
    }
    let cipher = Aes256::new_from_slice(&key).map_err(|_| "invalid AES key".to_owned())?;
    let mut previous = [0u8; 16];
    previous.copy_from_slice(&key[..16]);
    let mut plaintext = Vec::with_capacity(encrypted.len());
    for chunk in encrypted.chunks_exact(16) {
        let mut block = GenericArray::clone_from_slice(chunk);
        cipher.decrypt_block(&mut block);
        for (index, value) in block.iter_mut().enumerate() {
            *value ^= previous[index];
        }
        plaintext.extend_from_slice(&block);
        previous.copy_from_slice(chunk);
    }
    let padding = *plaintext.last().ok_or_else(|| "decrypted media is empty".to_owned())? as usize;
    if !(1..=32).contains(&padding)
        || padding > plaintext.len()
        || !plaintext[plaintext.len() - padding..]
            .iter()
            .all(|value| *value as usize == padding)
    {
        return Err("invalid PKCS#7 padding".into());
    }
    plaintext.truncate(plaintext.len() - padding);
    Ok(plaintext)
}

fn decode_aes_key(value: &str) -> Result<Vec<u8>, String> {
    let value = value.trim();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(value))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(value))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value))
        .ok()
        .filter(|bytes| bytes.len() == 32)
        .or_else(|| hex::decode(value).ok().filter(|bytes| bytes.len() == 32))
        .ok_or_else(|| "aeskey is not a 32-byte base64/hex key".to_owned())?;
    Ok(decoded)
}

fn safe_media_name(file_name: Option<&str>, mime: Option<&str>, message_id: &str) -> String {
    let original = file_name.unwrap_or_default();
    let base = Path::new(original)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty());
    if let Some(base) = base {
        let sanitized: String = base
            .chars()
            .map(|value| {
                if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                    value
                } else {
                    '_'
                }
            })
            .collect();
        if Path::new(&sanitized).extension().is_none()
            && let Some(extension) = mime.and_then(media_extension)
        {
            return format!("{sanitized}.{extension}");
        }
        return sanitized;
    }
    let extension = mime.and_then(media_extension).unwrap_or("bin");
    format!("wecom-{}.{extension}", safe_component(message_id))
}

fn media_extension(mime: &str) -> Option<&str> {
    Some(match mime {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        _ => mime.split('/').nth(1).filter(|value| !value.is_empty())?,
    })
}

fn safe_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "message".into()
    } else {
        sanitized
    }
}

fn detect_media_mime(bytes: &[u8]) -> Option<String> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png".into())
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg".into())
    } else if bytes.starts_with(b"GIF8") {
        Some("image/gif".into())
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp".into())
    } else {
        None
    }
}

fn parse_mixed(body: &serde_json::Value) -> Option<(String, Option<Vec<UnifiedAttachment>>, MessageContentType)> {
    let items = body.pointer("/mixed/msg_item")?.as_array()?;
    let mut text = String::new();
    let mut attachments = Vec::new();
    for item in items {
        match item.get("msgtype").and_then(serde_json::Value::as_str) {
            Some("text") => {
                if let Some(value) = item.pointer("/text/content").and_then(serde_json::Value::as_str) {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(value);
                }
            }
            Some("image") => {
                if let Some(mut values) = media_attachment(item, "image", "image/*") {
                    attachments.append(&mut values);
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str("[图片]");
                }
            }
            _ => {}
        }
    }
    Some((
        text,
        (!attachments.is_empty()).then_some(attachments),
        MessageContentType::Text,
    ))
}

fn message_body(message: &UnifiedOutgoingMessage) -> Result<serde_json::Value, ChannelError> {
    match message.message_type {
        OutgoingMessageType::Image => {
            let media_id = message
                .image_url
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ChannelError::MessageSendFailed("WeCom image message requires image_url as media_id".into())
                })?;
            Ok(serde_json::json!({"msgtype":"image", "image":{"media_id":media_id}}))
        }
        OutgoingMessageType::File => {
            let media_id = message
                .file_url
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ChannelError::MessageSendFailed("WeCom file message requires file_url as media_id".into())
                })?;
            Ok(serde_json::json!({"msgtype":"file", "file":{"media_id":media_id}}))
        }
        OutgoingMessageType::Buttons => {
            let buttons = message
                .buttons
                .as_ref()
                .or(message.keyboard.as_ref())
                .ok_or_else(|| ChannelError::MessageSendFailed("WeCom template card requires buttons".into()))?;
            Ok(template_card_body(message.text.as_deref().unwrap_or(""), buttons))
        }
        OutgoingMessageType::Text => {
            let content = message.text.as_deref().unwrap_or(" ");
            Ok(serde_json::json!({"msgtype":"markdown", "markdown":{"content":content}}))
        }
    }
}

fn template_card_body(title: &str, rows: &[Vec<ActionButton>]) -> serde_json::Value {
    let button_list: Vec<serde_json::Value> = rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|button| {
            serde_json::json!({
                "text": button.label,
                "style": if button.action.starts_with("pairing.") { 1 } else { 2 },
                "key": button.action,
            })
        })
        .collect();
    serde_json::json!({
        "msgtype":"template_card",
        "template_card": {
            "card_type":"button_interaction",
            "main_title":{"title":title},
            "button_list":button_list,
            "task_id": next_id("card"),
        }
    })
}

fn active_push_body(body: &serde_json::Value, _message: &UnifiedOutgoingMessage, chat_id: &str) -> serde_json::Value {
    let mut body = body.clone();
    if let Some(object) = body.as_object_mut() {
        object.insert("chatid".into(), serde_json::Value::String(chat_id.into()));
        object.insert("chat_type".into(), serde_json::json!(0));
    }
    body
}

async fn handle_event_callback<S>(
    write: &mut S,
    context: &ConnectionContext,
    frame: &serde_json::Value,
    req_id: &str,
) -> Result<(), ChannelError>
where
    S: futures_util::Sink<WsMessage> + Unpin,
    <S as futures_util::Sink<WsMessage>>::Error: std::fmt::Display,
{
    let event_type = frame
        .pointer("/body/event/eventtype")
        .and_then(serde_json::Value::as_str);
    if event_type == Some("enter_chat")
        && let Some(content) = context.welcome_message.as_deref()
    {
        let response = serde_json::json!({
            "cmd":"aibot_respond_welcome_msg",
            "headers":{"req_id":req_id},
            "body":{"msgtype":"text","text":{"content":content}},
        });
        write
            .send(WsMessage::Text(response.to_string().into()))
            .await
            .map_err(|error| ChannelError::MessageSendFailed(format!("WeCom welcome reply failed: {error}")))?;
    }
    Ok(())
}

fn split_text(text: &str, max: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![" ".into()];
    }
    let chars: Vec<char> = text.chars().collect();
    chars.chunks(max).map(|chunk| chunk.iter().collect()).collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn next_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        now_ms(),
        REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}
fn status_code(status: PluginStatus) -> u8 {
    status as u8
}
fn status_from_code(code: u8) -> PluginStatus {
    match code {
        1 => PluginStatus::Initializing,
        2 => PluginStatus::Ready,
        3 => PluginStatus::Starting,
        4 => PluginStatus::Running,
        5 => PluginStatus::Stopping,
        6 => PluginStatus::Stopped,
        7 => PluginStatus::Error,
        _ => PluginStatus::Created,
    }
}

fn remember_request(chat_id: &str, req_id: &str) {
    if !req_id.is_empty() {
        request_map()
            .lock()
            .unwrap()
            .insert(chat_id.into(), RequestContext { req_id: req_id.into() });
    }
}
fn latest_request(chat_id: &str) -> Option<RequestContext> {
    request_map().lock().unwrap().get(chat_id).cloned()
}
fn request_map() -> &'static Mutex<HashMap<String, RequestContext>> {
    static MAP: std::sync::OnceLock<Mutex<HashMap<String, RequestContext>>> = std::sync::OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn tls_connector() -> Result<tokio_tungstenite::Connector, ChannelError> {
    let certs = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(certs.certs);
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| ChannelError::ConnectionFailed(format!("TLS config failed: {error}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_tungstenite::Connector::Rustls(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::PluginCallbacks;
    use crate::types::{PluginConfigOptions, PluginCredentials};
    use futures_util::{SinkExt, StreamExt};
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    fn callbacks() -> PluginCallbacks {
        let (message_tx, _) = mpsc::channel(1);
        let (confirm_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::unbounded_channel();
        PluginCallbacks {
            message_tx,
            confirm_tx,
            status_tx,
        }
    }

    fn config(bot_id: Option<&str>, secret: Option<&str>) -> PluginConfig {
        let mut extra = HashMap::new();
        if let Some(value) = bot_id {
            extra.insert("bot_id".into(), serde_json::json!(value));
        }
        if let Some(value) = secret {
            extra.insert("secret".into(), serde_json::json!(value));
        }
        PluginConfig {
            credentials: PluginCredentials {
                token: None,
                app_id: None,
                app_secret: None,
                encrypt_key: None,
                verification_token: None,
                client_id: None,
                client_secret: None,
                account_id: None,
                bot_token: None,
                extra,
            },
            config: None,
        }
    }

    async fn mock_subscription_server(errcode: i64) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let Some(Ok(WsMessage::Text(request))) = socket.next().await else {
                return;
            };
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            assert_eq!(request["cmd"], "aibot_subscribe");
            assert_eq!(request["body"]["bot_id"], "bot");
            assert_eq!(request["body"]["secret"], "secret");
            let req_id = request["headers"]["req_id"].as_str().unwrap();
            socket
                .send(WsMessage::Text(
                    serde_json::json!({
                        "cmd": "aibot_subscribe",
                        "headers": {"req_id": req_id},
                        "errcode": errcode,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            if errcode == 0 {
                let _ = socket.next().await;
            }
        });
        (format!("ws://{address}"), task)
    }

    fn connection_context(ws_url: String, status_tx: mpsc::UnboundedSender<PluginStatus>) -> ConnectionContext {
        let (message_tx, _) = mpsc::channel(1);
        let (confirm_tx, _) = mpsc::channel(1);
        ConnectionContext {
            bot_id: "bot".into(),
            secret: "secret".into(),
            ws_url,
            callbacks: PluginCallbacks {
                message_tx,
                confirm_tx,
                status_tx,
            },
            status: Arc::new(AtomicU8::new(status_code(PluginStatus::Starting))),
            last_error: Arc::new(Mutex::new(None)),
            welcome_message: None,
        }
    }

    #[tokio::test]
    async fn validates_required_credentials_without_starting_network_task() {
        let mut plugin = WecomPlugin::new();
        assert!(
            plugin
                .initialize(config(None, Some("secret")), callbacks())
                .await
                .is_err()
        );
        let mut plugin = WecomPlugin::new();
        assert!(plugin.initialize(config(Some("bot"), None), callbacks()).await.is_err());
        let mut plugin = WecomPlugin::new();
        plugin
            .initialize(config(Some("bot"), Some("secret")), callbacks())
            .await
            .unwrap();
        assert_eq!(plugin.status(), PluginStatus::Ready);
        assert!(plugin.ws_handle.is_none());
    }

    #[tokio::test]
    async fn authenticates_only_after_success_response() {
        let (ws_url, server) = mock_subscription_server(0).await;
        let (status_tx, mut status_rx) = mpsc::unbounded_channel();
        let context = connection_context(ws_url, status_tx);
        let (_out_tx, out_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            let mut out_rx = out_rx;
            connect_once(&context, &mut out_rx, &mut shutdown_rx).await
        });

        let mut saw_running = false;
        for _ in 0..3 {
            if tokio::time::timeout(Duration::from_secs(2), status_rx.recv())
                .await
                .ok()
                .flatten()
                == Some(PluginStatus::Running)
            {
                saw_running = true;
                break;
            }
        }
        assert!(saw_running);
        shutdown_tx.send(true).unwrap();
        assert!(task.await.unwrap().is_ok());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn replies_to_callback_with_same_req_id_and_stream_id() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let Some(Ok(WsMessage::Text(subscribe))) = socket.next().await else {
                return;
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let subscribe_req_id = subscribe["headers"]["req_id"].as_str().unwrap();
            socket
                .send(WsMessage::Text(
                    serde_json::json!({
                        "cmd": "aibot_subscribe",
                        "headers": {"req_id": subscribe_req_id},
                        "errcode": 0,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket
                .send(WsMessage::Text(
                    serde_json::json!({
                        "cmd": "aibot_msg_callback",
                        "headers": {"req_id": "callback-req-1"},
                        "body": {
                            "msgid": "callback-msg-1",
                            "chatid": "chat-1",
                            "chattype": "single",
                            "from": {"userid": "user-1"},
                            "msgtype": "text",
                            "text": {"content": "hello"}
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();

            let Some(Ok(WsMessage::Text(first_reply))) = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
            else {
                return;
            };
            let first_reply: serde_json::Value = serde_json::from_str(&first_reply).unwrap();
            assert_eq!(first_reply["cmd"], "aibot_respond_msg");
            assert_eq!(first_reply["headers"]["req_id"], "callback-req-1");
            assert_eq!(first_reply["body"]["msgtype"], "stream");
            assert_eq!(first_reply["body"]["stream"]["finish"], false);
            let stream_id = first_reply["body"]["stream"]["id"].as_str().unwrap();
            assert!(!stream_id.is_empty());
            let Some(Ok(WsMessage::Text(last_reply))) = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
            else {
                return;
            };
            let last_reply: serde_json::Value = serde_json::from_str(&last_reply).unwrap();
            assert_eq!(last_reply["cmd"], "aibot_respond_msg");
            assert_eq!(last_reply["headers"]["req_id"], "callback-req-1");
            assert_eq!(last_reply["body"]["stream"]["id"], stream_id);
            assert_eq!(last_reply["body"]["stream"]["finish"], true);
            socket
                .send(WsMessage::Text(
                    serde_json::json!({
                        "headers": {"req_id": "callback-req-1"},
                        "errcode": 0,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await;
        });

        let (message_tx, mut message_rx) = mpsc::channel(1);
        let (confirm_tx, _) = mpsc::channel(1);
        let (status_tx, _) = mpsc::unbounded_channel();
        let callbacks = PluginCallbacks {
            message_tx,
            confirm_tx,
            status_tx,
        };
        let mut plugin_config = config(Some("bot"), Some("secret"));
        plugin_config.config = Some(PluginConfigOptions {
            mode: None,
            webhook_url: None,
            rate_limit: None,
            require_mention: None,
            extra: HashMap::from([(
                String::from("websocket_url"),
                serde_json::json!(format!("ws://{address}")),
            )]),
        });
        let mut plugin = WecomPlugin::new();
        plugin.initialize(plugin_config, callbacks).await.unwrap();
        plugin.start().await.unwrap();

        let incoming = tokio::time::timeout(Duration::from_secs(2), message_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(incoming.chat_id, "chat-1");
        assert_eq!(incoming.content.text, "hello");
        let message = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Buttons,
            text: Some(format!("{}tail", "x".repeat(MAX_MESSAGE_CHARS))),
            parse_mode: None,
            buttons: Some(vec![vec![ActionButton {
                label: "Regenerate".into(),
                action: "chat.regenerate".into(),
                params: None,
            }]]),
            keyboard: None,
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };
        assert!(plugin.send_message("chat-1", message).await.is_ok());
        plugin.stop().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn authentication_failure_is_reported_without_running_status() {
        let (ws_url, server) = mock_subscription_server(40001).await;
        let (status_tx, mut status_rx) = mpsc::unbounded_channel();
        let context = connection_context(ws_url, status_tx);
        let (_out_tx, out_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut shutdown_rx = shutdown_rx;
        let mut out_rx = out_rx;
        let result = connect_once(&context, &mut out_rx, &mut shutdown_rx).await;
        assert!(result.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), status_rx.recv())
                .await
                .ok()
                .flatten()
                .is_none()
        );
        server.await.unwrap();
    }

    #[test]
    fn parses_text_and_preserves_protocol_fields() {
        let frame = serde_json::json!({"cmd":"aibot_msg_callback","headers":{"req_id":"r1"},"body":{
            "msgid":"m1","aibotid":"b1","chatid":"c1","chattype":"group","from":{"userid":"u1","name":"Alice"},"create_time":1700000000,"msgtype":"text","text":{"content":"hello"}
        }});
        let mut seen = HashMap::new();
        let message = parse_incoming(&frame, &mut seen).unwrap();
        assert_eq!(message.platform, PluginType::Wecom);
        assert_eq!(message.id, "m1");
        assert_eq!(message.chat_id, "c1");
        assert_eq!(message.user.display_name, "Alice");
        assert_eq!(message.timestamp, 1_700_000_000_000);
        assert!(parse_incoming(&frame, &mut seen).is_none());
    }

    #[test]
    fn parses_media_and_mixed_messages_into_attachments() {
        let image = serde_json::json!({
            "cmd":"aibot_msg_callback",
            "body": {
                "msgid":"image-1", "chattype":"single", "from":{"userid":"u1"},
                "msgtype":"image", "image":{"url":"https://example.test/image","aeskey":"key"}
            }
        });
        let mut seen = HashMap::new();
        let message = parse_incoming(&image, &mut seen).unwrap();
        assert_eq!(message.content.content_type, MessageContentType::Photo);
        assert_eq!(
            message.content.attachments.as_ref().unwrap()[0].url.as_deref(),
            Some("https://example.test/image")
        );
        assert_eq!(message.content.text, "[图片]");

        let mixed = serde_json::json!({
            "cmd":"aibot_msg_callback",
            "body": {
                "msgid":"mixed-1", "chatid":"group-1", "from":{"userid":"u1"},
                "msgtype":"mixed", "mixed":{"msg_item":[
                    {"msgtype":"text", "text":{"content":"hello"}},
                    {"msgtype":"image", "image":{"url":"https://example.test/image"}}
                ]}
            }
        });
        let message = parse_incoming(&mixed, &mut seen).unwrap();
        assert_eq!(message.chat_id, "group-1");
        assert_eq!(message.content.text, "hello\n[图片]");
        assert_eq!(message.content.attachments.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn decrypts_wecom_aes256_cbc_with_key_prefix_iv_and_32_byte_padding() {
        use aes::cipher::BlockEncrypt;

        let key: Vec<u8> = (0..32).collect();
        let mut padded = b"wecom attachment".to_vec();
        padded.extend(std::iter::repeat_n(
            (32 - padded.len() % 32) as u8,
            32 - padded.len() % 32,
        ));
        let cipher = Aes256::new_from_slice(&key).unwrap();
        let mut previous = [0u8; 16];
        previous.copy_from_slice(&key[..16]);
        let mut encrypted = Vec::new();
        for chunk in padded.chunks_exact(16) {
            let mut block = GenericArray::clone_from_slice(chunk);
            for (index, value) in block.iter_mut().enumerate() {
                *value ^= previous[index];
            }
            cipher.encrypt_block(&mut block);
            previous.copy_from_slice(&block);
            encrypted.extend_from_slice(&block);
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(&key);
        assert_eq!(decrypt_media(&encrypted, &encoded).unwrap(), b"wecom attachment");

        let unpadded_standard = base64::engine::general_purpose::STANDARD
            .encode([0xfb_u8; 32])
            .trim_end_matches('=')
            .to_owned();
        assert_eq!(decode_aes_key(&unpadded_standard).unwrap(), vec![0xfb_u8; 32]);
    }

    #[test]
    fn builds_wecom_media_and_template_card_bodies() {
        let image = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Image,
            text: None,
            parse_mode: None,
            buttons: None,
            keyboard: None,
            image_url: Some("MEDIA_IMAGE".into()),
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };
        assert_eq!(message_body(&image).unwrap()["image"]["media_id"], "MEDIA_IMAGE");

        let card = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Buttons,
            text: Some("Choose".into()),
            parse_mode: None,
            buttons: Some(vec![vec![ActionButton {
                label: "Yes".into(),
                action: "confirm".into(),
                params: None,
            }]]),
            keyboard: None,
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };
        let body = message_body(&card).unwrap();
        assert_eq!(body["msgtype"], "template_card");
        assert_eq!(body["template_card"]["button_list"][0]["key"], "confirm");
        assert!(!body["template_card"]["task_id"].as_str().unwrap().is_empty());
        assert_eq!(active_push_body(&body, &card, "chat-1")["chatid"], "chat-1");
    }

    #[test]
    fn splits_long_text_without_breaking_unicode() {
        assert_eq!(split_text("你好世界", 2), vec!["你好", "世界"]);
    }
}

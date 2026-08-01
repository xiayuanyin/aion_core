use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, warn};

use crate::error::ChannelError;
use crate::plugin::{ChannelPlugin, PluginCallbacks};
use crate::types::{
    BotInfo, MessageContentType, PluginConfig, PluginStatus, PluginType, UnifiedIncomingMessage, UnifiedMessageContent,
    UnifiedOutgoingMessage, UnifiedUser,
};

const WS_URL: &str = "wss://openws.work.weixin.qq.com";
const HEARTBEAT: Duration = Duration::from_secs(30);
const RESPONSE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const MAX_MESSAGE_CHARS: usize = 4096;
const MAX_DEDUP_ENTRIES: usize = 2048;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

struct Outbound {
    req_id: String,
    message: UnifiedOutgoingMessage,
    result: oneshot::Sender<Result<String, ChannelError>>,
}

struct PendingOutbound {
    sent_at: Instant,
    result: oneshot::Sender<Result<String, ChannelError>>,
    stream_id: String,
}

struct ConnectionContext {
    bot_id: String,
    secret: String,
    ws_url: String,
    callbacks: PluginCallbacks,
    status: Arc<AtomicU8>,
    last_error: Arc<Mutex<Option<String>>>,
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
        let req_id = latest_req_id(chat_id)
            .ok_or_else(|| ChannelError::MessageSendFailed("WeCom reply has no inbound request context".into()))?;
        let out_tx = self
            .out_tx
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("WeCom plugin is not connected".into()))?;
        let (result_tx, result_rx) = oneshot::channel();
        out_tx
            .send(Outbound {
                req_id,
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom connection is stopping".into()))?;
        result_rx
            .await
            .map_err(|_| ChannelError::MessageSendFailed("WeCom send task stopped".into()))?
    }

    async fn edit_message(
        &self,
        chat_id: &str,
        _message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        let _ = self.send_message(chat_id, message).await?;
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
                    let result = send_outbound(&mut write, &outbound).await;
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
                        if (cmd == "aibot_msg_callback" || cmd == "aibot_event_callback") && authenticated
                            && let Some(message) = parse_incoming(&frame, &mut seen) {
                            remember_req_id(&message.chat_id, req_id);
                            let _ = context.callbacks.message_tx.send(message).await;
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

async fn send_outbound<S>(write: &mut S, outbound: &Outbound) -> Result<String, ChannelError>
where
    S: futures_util::Sink<WsMessage> + Unpin,
    <S as futures_util::Sink<WsMessage>>::Error: std::fmt::Display,
{
    let text = outbound.message.text.as_deref().unwrap_or("");
    let chunks = split_text(text, MAX_MESSAGE_CHARS);
    let stream_id = next_id("stream");
    for (index, chunk) in chunks.iter().enumerate() {
        let finish = index + 1 == chunks.len();
        let body = if outbound.message.parse_mode.is_some() {
            serde_json::json!({"msgtype":"markdown", "markdown":{"content":chunk}})
        } else {
            serde_json::json!({"msgtype":"stream", "stream":{"id":stream_id,"finish":finish,"content":chunk}})
        };
        let frame = serde_json::json!({
            "cmd": "aibot_respond_msg", "headers": {"req_id": outbound.req_id},
            "body": body
        });
        write
            .send(WsMessage::Text(frame.to_string().into()))
            .await
            .map_err(|error| ChannelError::MessageSendFailed(format!("WeCom response send failed: {error}")))?;
    }
    Ok(stream_id)
}

fn parse_incoming(frame: &serde_json::Value, seen: &mut HashMap<String, Instant>) -> Option<UnifiedIncomingMessage> {
    let body = frame.get("body")?;
    let msgid = body.get("msgid")?.as_str()?.to_owned();
    let now = Instant::now();
    seen.retain(|_, value| now.duration_since(*value) < Duration::from_secs(3600));
    if seen.insert(msgid.clone(), now).is_some() {
        return None;
    }
    if seen.len() > MAX_DEDUP_ENTRIES
        && let Some(key) = seen.keys().next().cloned()
    {
        seen.remove(&key);
    }
    let msgtype = body.get("msgtype").and_then(serde_json::Value::as_str).unwrap_or("");
    let text = match msgtype {
        "text" => body
            .pointer("/text/content")
            .and_then(serde_json::Value::as_str)?
            .to_owned(),
        "voice" => body
            .pointer("/voice/content")
            .and_then(serde_json::Value::as_str)?
            .to_owned(),
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
            content_type: MessageContentType::Text,
            text,
            attachments: None,
        },
        timestamp,
        reply_to_message_id: None,
        action: None,
        raw: Some(frame.clone()),
    })
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

fn remember_req_id(chat_id: &str, req_id: &str) {
    if !req_id.is_empty() {
        request_map().lock().unwrap().insert(chat_id.into(), req_id.into());
    }
}
fn latest_req_id(chat_id: &str) -> Option<String> {
    request_map().lock().unwrap().get(chat_id).cloned()
}
fn request_map() -> &'static Mutex<HashMap<String, String>> {
    static MAP: std::sync::OnceLock<Mutex<HashMap<String, String>>> = std::sync::OnceLock::new();
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
    use crate::types::PluginCredentials;
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
    fn splits_long_text_without_breaking_unicode() {
        assert_eq!(split_text("你好世界", 2), vec!["你好", "世界"]);
    }
}

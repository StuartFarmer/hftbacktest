use std::{
    collections::HashMap,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{self, timeout},
};
use tokio_tungstenite::tungstenite::{Bytes, Message};

use crate::pacifica::{
    config::Config,
    order_payload::{CancelOrderPayload, CreateOrderPayload, EditOrderPayload},
    signing::{PacificaSigner, SigningError},
    websocket::{self, WebSocketError},
    ws_trade::{
        WsTradeAction, WsTradeProtocolError, command_envelope, parse_trade_response,
        request_id as ws_request_id,
    },
};

const ORDER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum TradeError {
    #[error("signing: {0}")]
    Signing(#[from] SigningError),
    #[error("private key: {0}")]
    PrivateKey(String),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("websocket: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error("tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("websocket protocol: {0}")]
    WsProtocol(#[from] WsTradeProtocolError),
    #[error("order request timed out")]
    Timeout,
    #[error("trade command client has not been started")]
    NotStarted,
    #[error("trade command client stopped")]
    Stopped,
    #[error("unsupported trade operation: {0}")]
    UnsupportedOperation(&'static str),
    #[error("order request failed: {0}")]
    Order(String),
}

#[derive(Debug, Clone)]
pub struct TradeClient {
    trade_url: String,
    account: String,
    private_key_file: String,
    expiry_window: i64,
    order_timeout: Duration,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<TradeCommand>>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignedTradeRequest {
    pub request_id: String,
    pub operation_type: &'static str,
    pub body: serde_json::Value,
}

struct TradeCommand {
    request: SignedTradeRequest,
    response_tx: oneshot::Sender<Result<(), TradeError>>,
}

impl TradeClient {
    pub fn new(config: &Config) -> Self {
        Self {
            trade_url: config.trade_url.clone(),
            account: config.account.clone(),
            private_key_file: config.private_key_file.clone(),
            expiry_window: 5000,
            order_timeout: ORDER_TIMEOUT,
            command_tx: Default::default(),
        }
    }

    pub fn start(&self) {
        let mut guard = self.command_tx.lock().unwrap();
        if guard.is_some() {
            return;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        *guard = Some(tx);
        let url = self.trade_url.clone();
        tokio::spawn(async move {
            run_command_worker(url, rx).await;
        });
    }

    #[cfg(test)]
    pub(crate) fn install_success_probe(&self) -> mpsc::UnboundedReceiver<SignedTradeRequest> {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<TradeCommand>();
        let (probe_tx, probe_rx) = mpsc::unbounded_channel();
        *self.command_tx.lock().unwrap() = Some(command_tx);
        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                let _ = probe_tx.send(command.request.clone());
                let _ = command.response_tx.send(Ok(()));
            }
        });
        probe_rx
    }

    pub async fn submit_limit(&self, payload: CreateOrderPayload) -> Result<(), TradeError> {
        self.send_signed(self.create_limit_request(&payload)?).await
    }

    pub async fn cancel_order(&self, payload: CancelOrderPayload) -> Result<(), TradeError> {
        self.send_signed(self.cancel_request(&payload)?).await
    }

    pub async fn edit_order(&self, payload: EditOrderPayload) -> Result<(), TradeError> {
        self.send_signed(self.edit_request(&payload)?).await
    }

    pub fn create_limit_request(
        &self,
        payload: &CreateOrderPayload,
    ) -> Result<SignedTradeRequest, TradeError> {
        self.signed_request("create_order", payload)
    }

    pub fn cancel_request(
        &self,
        payload: &CancelOrderPayload,
    ) -> Result<SignedTradeRequest, TradeError> {
        self.signed_request("cancel_order", payload)
    }

    pub fn edit_request(
        &self,
        payload: &EditOrderPayload,
    ) -> Result<SignedTradeRequest, TradeError> {
        self.signed_request("edit_order", payload)
    }

    fn signed_request<T: serde::Serialize>(
        &self,
        operation_type: &'static str,
        payload: &T,
    ) -> Result<SignedTradeRequest, TradeError> {
        let signer = self.signer()?;
        let timestamp = Utc::now().timestamp_millis();
        let signed = signer.sign_payload(operation_type, payload, timestamp)?;
        Ok(SignedTradeRequest {
            request_id: request_id(),
            operation_type,
            body: signed.into_value(),
        })
    }

    fn signer(&self) -> Result<PacificaSigner, TradeError> {
        let key = fs::read_to_string(&self.private_key_file)
            .map_err(|error| TradeError::PrivateKey(error.to_string()))?;
        let key = key.trim();
        if key.starts_with('[') {
            Ok(PacificaSigner::from_json_uint8_keypair(
                key,
                self.account.clone(),
                self.expiry_window,
            )?)
        } else {
            Ok(PacificaSigner::from_base58_keypair(
                key,
                self.account.clone(),
                self.expiry_window,
            )?)
        }
    }

    async fn send_signed(&self, request: SignedTradeRequest) -> Result<(), TradeError> {
        let response_rx = {
            let guard = self.command_tx.lock().unwrap();
            let command_tx = guard.as_ref().ok_or(TradeError::NotStarted)?;
            let (response_tx, response_rx) = oneshot::channel();
            command_tx
                .send(TradeCommand {
                    request,
                    response_tx,
                })
                .map_err(|_| TradeError::Stopped)?;
            response_rx
        };
        let result = timeout(self.order_timeout, response_rx)
            .await
            .map_err(|_| TradeError::Timeout)?
            .map_err(|_| TradeError::Stopped)?;
        result
    }
}

impl TradeError {
    pub fn to_value(&self) -> hftbacktest::prelude::Value {
        hftbacktest::prelude::Value::String(self.to_string())
    }
}

async fn run_command_worker(url: String, mut rx: mpsc::UnboundedReceiver<TradeCommand>) {
    let mut stream = None;
    let mut pending: HashMap<String, oneshot::Sender<Result<(), TradeError>>> = HashMap::new();
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        if stream.is_none() {
            match rx.recv().await {
                Some(command) => match websocket::connect(&url).await {
                    Ok(ws) => {
                        stream = Some(ws);
                        if let Some(ws) = stream.as_mut()
                            && let Err(error) = send_command(ws, command, &mut pending).await
                        {
                            fail_pending(&mut pending, error.to_string());
                            stream = None;
                        }
                    }
                    Err(error) => {
                        let _ = command.response_tx.send(Err(error.into()));
                    }
                },
                None => break,
            }
            continue;
        }

        let ws = stream.as_mut().unwrap();
        select! {
            Some(command) = rx.recv() => {
                if let Err(error) = send_command(ws, command, &mut pending).await {
                    fail_pending(&mut pending, error.to_string());
                    stream = None;
                }
            }
            message = ws.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        handle_response_text(&text, &mut pending);
                    }
                    Some(Ok(Message::Ping(_))) => {
                        if let Err(error) = ws.send(Message::Pong(Bytes::default())).await {
                            fail_pending(&mut pending, error.to_string());
                            stream = None;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        fail_pending(&mut pending, "connection interrupted".to_string());
                        stream = None;
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        fail_pending(&mut pending, error.to_string());
                        stream = None;
                    }
                }
            }
            _ = interval.tick() => {
                if let Err(error) = ws.send(Message::Text(websocket::ping_payload().to_string().into())).await {
                    fail_pending(&mut pending, error.to_string());
                    stream = None;
                }
            }
            else => break,
        }
    }
    fail_pending(&mut pending, "trade command client stopped".to_string());
}

async fn send_command(
    ws: &mut websocket::WsStream,
    command: TradeCommand,
    pending: &mut HashMap<String, oneshot::Sender<Result<(), TradeError>>>,
) -> Result<(), TradeError> {
    let action = WsTradeAction::from_operation_type(command.request.operation_type).ok_or(
        TradeError::UnsupportedOperation(command.request.operation_type),
    )?;
    let payload = command_envelope(
        command.request.request_id.clone(),
        action,
        command.request.body,
    );
    let request_id = command.request.request_id;
    pending.insert(request_id.clone(), command.response_tx);
    if let Err(error) = ws.send(Message::Text(payload.to_string().into())).await {
        let error = TradeError::Tungstenite(error);
        if let Some(tx) = pending.remove(&request_id) {
            let _ = tx.send(Err(TradeError::Order(error.to_string())));
        }
        return Err(error);
    }
    Ok(())
}

fn handle_response_text(
    text: &str,
    pending: &mut HashMap<String, oneshot::Sender<Result<(), TradeError>>>,
) {
    if is_pong(text) {
        return;
    }
    match parse_trade_response(text, None) {
        Ok(response) => {
            if let Some(tx) = pending.remove(&response.request_id) {
                let _ = tx.send(Ok(()));
            }
        }
        Err(WsTradeProtocolError::Command { .. }) => {
            if let Some(id) = response_id(text)
                && let Some(tx) = pending.remove(&id)
            {
                let _ = tx.send(Err(TradeError::Order(redacted_error_text(text))));
            }
        }
        Err(error) => {
            if let Some(id) = response_id(text)
                && let Some(tx) = pending.remove(&id)
            {
                let _ = tx.send(Err(error.into()));
            }
        }
    }
}

fn is_pong(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("channel")
                .and_then(|channel| channel.as_str())
                .map(|channel| channel == "pong")
        })
        .unwrap_or(false)
}

fn response_id(text: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| value.get("id").and_then(|id| id.as_str()).map(String::from))
}

fn redacted_error_text(text: &str) -> String {
    let mut value = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) => value,
        Err(_) => return text.to_string(),
    };
    redact_signatures(&mut value);
    value.to_string()
}

fn redact_signatures(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if key == "signature" {
                    *value = serde_json::Value::String("<REDACTED>".to_string());
                } else {
                    redact_signatures(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_signatures(value);
            }
        }
        _ => {}
    }
}

fn fail_pending(
    pending: &mut HashMap<String, oneshot::Sender<Result<(), TradeError>>>,
    message: String,
) {
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(TradeError::Order(message.clone())));
    }
}

fn request_id() -> String {
    ws_request_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacifica::ordermanager::OrderManager;
    use hftbacktest::prelude::{OrdType, Order, Side, TimeInForce};
    use serde_json::Value;

    #[test]
    fn submit_limit_gtx_builds_alo_create() {
        let mut manager = OrderManager::new("pfhbt-");
        let payload = manager
            .new_order(
                "BTC",
                Order::new(
                    42,
                    63_441,
                    1.0,
                    0.0002,
                    Side::Buy,
                    OrdType::Limit,
                    TimeInForce::GTX,
                ),
            )
            .unwrap();

        assert_eq!(payload.symbol, "BTC");
        assert_eq!(payload.side, "bid");
        assert_eq!(payload.price, "63441");
        assert_eq!(payload.amount, "0.0002");
        assert_eq!(payload.tif, "ALO");
        assert_uuid_v4(&payload.client_order_id);
    }

    #[test]
    fn cancel_builds_client_order_id_cancel() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order(
                "BTC",
                Order::new(
                    42,
                    63_441,
                    1.0,
                    0.0002,
                    Side::Buy,
                    OrdType::Limit,
                    TimeInForce::GTX,
                ),
            )
            .unwrap()
            .client_order_id;

        let payload = manager.cancel_order("BTC", 42).unwrap();

        assert_eq!(payload.symbol, "BTC");
        assert_eq!(payload.client_order_id, created);
    }

    #[test]
    fn request_ids_are_unique_and_hide_signature() {
        let first = request_id();
        let second = request_id();

        assert_uuid_v4(&first);
        assert_uuid_v4(&second);
        assert_ne!(first, second);
        assert!(!first.contains("signature"));
        assert!(!second.contains("signature"));
    }

    #[test]
    fn trade_error_converts_to_value() {
        let hftbacktest::prelude::Value::String(message) = TradeError::Timeout.to_value() else {
            panic!("expected string value");
        };
        assert_eq!(message, "order request timed out");
    }

    #[test]
    fn create_failure_clears_pending_order() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order(
                "BTC",
                Order::new(
                    42,
                    63_441,
                    1.0,
                    0.0002,
                    Side::Buy,
                    OrdType::Limit,
                    TimeInForce::GTX,
                ),
            )
            .unwrap()
            .client_order_id;

        let rejected = manager.update_submit_fail(&created).unwrap();

        assert_eq!(
            rejected.order.status,
            hftbacktest::prelude::Status::Rejected
        );
        assert!(manager.cancel_order("BTC", 42).is_err());
    }

    #[test]
    fn cancel_timeout_keeps_order_active_and_publishes_error() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order(
                "BTC",
                Order::new(
                    42,
                    63_441,
                    1.0,
                    0.0002,
                    Side::Buy,
                    OrdType::Limit,
                    TimeInForce::GTX,
                ),
            )
            .unwrap()
            .client_order_id;

        let active = manager.update_cancel_fail(&created).unwrap();

        assert_eq!(active.order.status, hftbacktest::prelude::Status::New);
        let hftbacktest::prelude::Value::String(message) = TradeError::Timeout.to_value() else {
            panic!("expected string value");
        };
        assert!(message.contains("timed out"));
    }

    #[tokio::test]
    async fn ws_command_client_sends_to_worker_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let client = test_client("ws://example.invalid/ws", Duration::from_secs(1));
        *client.command_tx.lock().unwrap() = Some(tx);
        tokio::spawn(async move {
            let command = rx.recv().await.unwrap();
            assert_eq!(command.request.operation_type, "create_order");
            assert_eq!(command.request.body["symbol"], "BTC");
            assert_eq!(command.request.body["tif"], "ALO");
            assert!(command.request.body["signature"].as_str().is_some());
            command.response_tx.send(Ok(())).unwrap();
        });

        client
            .submit_limit(CreateOrderPayload::new(
                "BTC",
                "63441",
                "0.0002",
                "bid",
                "79f948fd-7556-4066-a128-083f3ea49322",
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn edit_order_builds_signed_request() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let client = test_client("ws://example.invalid/ws", Duration::from_secs(1));
        *client.command_tx.lock().unwrap() = Some(tx);
        tokio::spawn(async move {
            let command = rx.recv().await.unwrap();
            assert_eq!(command.request.operation_type, "edit_order");
            assert_eq!(command.request.body["symbol"], "BTC");
            assert_eq!(command.request.body["price"], "63442");
            assert_eq!(command.request.body["amount"], "0.0002");
            assert_eq!(
                command.request.body["client_order_id"],
                "79f948fd-7556-4066-a128-083f3ea49322"
            );
            assert!(command.request.body["signature"].as_str().is_some());
            command.response_tx.send(Ok(())).unwrap();
        });

        client
            .edit_order(EditOrderPayload::new(
                "BTC",
                "63442",
                "0.0002",
                "79f948fd-7556-4066-a128-083f3ea49322",
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ws_command_client_times_out_pending_request() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let client = test_client("ws://example.invalid/ws", Duration::from_millis(50));
        *client.command_tx.lock().unwrap() = Some(tx);

        let error = client
            .submit_limit(CreateOrderPayload::new(
                "BTC",
                "63441",
                "0.0002",
                "bid",
                "79f948fd-7556-4066-a128-083f3ea49322",
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, TradeError::Timeout));
    }

    #[test]
    fn ws_command_client_fails_pending_on_disconnect() {
        let (tx, rx) = oneshot::channel();
        let mut pending = HashMap::from([("request".to_string(), tx)]);

        fail_pending(&mut pending, "connection interrupted".to_string());
        let error = rx.blocking_recv().unwrap().unwrap_err();

        assert!(error.to_string().contains("connection interrupted"));
    }

    #[test]
    fn ws_response_text_resolves_matching_pending_request() {
        let (tx, rx) = oneshot::channel();
        let mut pending = HashMap::from([("request".to_string(), tx)]);

        handle_response_text(
            r#"{"code":200,"data":{"I":"client","i":645953,"s":"BTC"},"id":"request","t":1749223025962,"type":"create_order"}"#,
            &mut pending,
        );

        assert!(rx.blocking_recv().unwrap().is_ok());
        assert!(pending.is_empty());
    }

    #[test]
    fn ws_error_message_redacts_signature() {
        let error = redacted_error_text(
            r#"{"code":400,"error":{"signature":"secret","nested":{"signature":"hidden"}},"id":"id","type":"create_order"}"#,
        );

        assert!(!error.contains("secret"));
        assert!(!error.contains("hidden"));
        assert!(error.contains("<REDACTED>"));
    }

    fn assert_uuid_v4(value: &str) {
        let bytes = value.as_bytes();
        assert_eq!(bytes.len(), 36);
        assert_eq!(bytes[8], b'-');
        assert_eq!(bytes[13], b'-');
        assert_eq!(bytes[18], b'-');
        assert_eq!(bytes[23], b'-');
        assert_eq!(bytes[14], b'4');
        assert!(matches!(bytes[19], b'8' | b'9' | b'a' | b'b'));
    }

    fn test_client(url: &str, order_timeout: Duration) -> TradeClient {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../fixtures/pacifica/signing_create_order.json"
        ))
        .unwrap();
        let key_file =
            std::env::temp_dir().join(format!("pacifica-test-key-{}.json", request_id()));
        std::fs::write(
            &key_file,
            serde_json::to_string(&fixture["private_key_uint8"]).unwrap(),
        )
        .unwrap();
        TradeClient {
            trade_url: url.to_string(),
            account: fixture["public_key"].as_str().unwrap().to_string(),
            private_key_file: key_file.to_string_lossy().to_string(),
            expiry_window: fixture["expiry_window"].as_i64().unwrap(),
            order_timeout,
            command_tx: Default::default(),
        }
    }
}

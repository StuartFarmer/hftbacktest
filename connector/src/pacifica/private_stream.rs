use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::LiveEvent;
use thiserror::Error;
use tokio::{select, sync::mpsc::UnboundedSender, time};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tracing::{info, warn};

use crate::{
    connector::PublishEvent,
    pacifica::{
        account_state::{
            AccountState, AccountStateError, max_li_from_text, tracked_symbols_from_positions_text,
        },
        ordermanager::{
            OrderExt, OrderManager, OrderManagerError, PacificaOrderStatus, PacificaOrderUpdate,
        },
        websocket::{self, WebSocketError},
    },
};

#[derive(Debug, Error)]
pub enum PrivateStreamError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid numeric field {field}: {value}")]
    InvalidNumber { field: &'static str, value: String },
    #[error("unsupported order status: {0}")]
    UnsupportedOrderStatus(String),
    #[error("order manager: {0}")]
    OrderManager(#[from] OrderManagerError),
    #[error("account state: {0}")]
    AccountState(#[from] AccountStateError),
    #[error("position update missing symbol")]
    MissingPositionSymbol,
    #[error("websocket: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error("tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
}

#[derive(Debug, serde::Deserialize)]
struct OrderUpdatesFrame {
    data: Vec<AccountOrderUpdate>,
}

#[derive(Debug, serde::Deserialize)]
struct AccountOrderUpdate {
    #[serde(rename = "I")]
    client_order_id: Option<String>,
    #[serde(rename = "i")]
    exchange_order_id: Option<i64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "d")]
    side: Option<String>,
    #[serde(rename = "ip")]
    initial_price: Option<String>,
    #[serde(rename = "a")]
    amount: Option<String>,
    #[serde(rename = "f")]
    filled_amount: Option<String>,
    #[serde(rename = "lp")]
    last_price: Option<String>,
    #[serde(rename = "p")]
    average_price: Option<String>,
    #[serde(rename = "os")]
    status: String,
    #[serde(rename = "ut")]
    updated_at_ms: i64,
}

#[derive(Debug, serde::Deserialize)]
struct AccountTradesFrame {
    data: Vec<AccountTrade>,
}

#[derive(Debug, serde::Deserialize)]
struct AccountTrade {
    #[serde(rename = "I")]
    client_order_id: Option<String>,
    #[serde(rename = "i")]
    exchange_order_id: Option<i64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "a")]
    amount: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "ts")]
    _trade_side: String,
    #[serde(rename = "t")]
    timestamp_ms: i64,
}

pub struct PrivateStream {
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: Arc<Mutex<OrderManager>>,
    account_state: Arc<Mutex<AccountState>>,
    symbols: Arc<Mutex<HashSet<String>>>,
    account: String,
}

impl PrivateStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: Arc<Mutex<OrderManager>>,
        account_state: Arc<Mutex<AccountState>>,
        symbols: Arc<Mutex<HashSet<String>>>,
        account: String,
    ) -> Self {
        Self {
            ev_tx,
            order_manager,
            account_state,
            symbols,
            account,
        }
    }

    pub async fn connect(self, url: &str) -> Result<(), PrivateStreamError> {
        let stream = websocket::connect(url).await?;
        info!(url, "Pacifica private websocket connected");
        let (mut write, mut read) = stream.split();
        for payload in websocket::private_subscriptions(&self.account) {
            write
                .send(Message::Text(payload.to_string().into()))
                .await?;
            info!(payload = %payload, "Pacifica private subscription sent");
        }
        let mut interval = time::interval(Duration::from_secs(30));

        loop {
            select! {
                _ = interval.tick() => {
                    write.send(Message::Text(websocket::ping_payload().to_string().into())).await?;
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => self.handle_text(&text)?,
                        Some(Ok(Message::Ping(_))) => write.send(Message::Pong(Bytes::default())).await?,
                        Some(Ok(Message::Close(_))) | None => return Err(WebSocketError::ConnectionInterrupted.into()),
                        Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(error)) => return Err(WebSocketError::from(error).into()),
                    }
                }
            }
        }
    }

    fn handle_text(&self, text: &str) -> Result<(), PrivateStreamError> {
        let channel = channel_name(text)?;
        let events = match channel.as_deref() {
            Some("account_order_updates") => apply_order_updates_if_fresh(
                &mut self.order_manager.lock().unwrap(),
                &mut self.account_state.lock().unwrap(),
                text,
            )?,
            Some("account_trades") => apply_account_trades_if_fresh(
                &mut self.order_manager.lock().unwrap(),
                &mut self.account_state.lock().unwrap(),
                text,
            )
            .unwrap_or_default(),
            Some("account_positions") => {
                let symbols = self.symbols.lock().unwrap().clone();
                self.account_state
                    .lock()
                    .unwrap()
                    .apply_positions_text(text, &symbols)?
            }
            Some("subscribe") | Some("pong") | None => Vec::new(),
            _ => Vec::new(),
        };
        for event in events {
            let _ = self.ev_tx.send(PublishEvent::LiveEvent(event));
        }
        Ok(())
    }
}

fn channel_name(text: &str) -> Result<Option<String>, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_str(text)?;
    Ok(value
        .get("channel")
        .and_then(|channel| channel.as_str())
        .map(ToString::to_string))
}

pub fn parse_order_updates(text: &str) -> Result<Vec<PacificaOrderUpdate>, PrivateStreamError> {
    let frame: OrderUpdatesFrame = serde_json::from_str(text)?;
    frame
        .data
        .into_iter()
        .map(|update| {
            let exec_qty = optional_f64("filled_amount", update.filled_amount.as_deref())?;
            let leaves_qty = match (optional_f64("amount", update.amount.as_deref())?, exec_qty) {
                (Some(amount), Some(filled)) => Some((amount - filled).max(0.0)),
                (Some(amount), None) => Some(amount),
                (None, _) => None,
            };
            let exec_price = first_positive_price([
                update.last_price.as_deref(),
                update.average_price.as_deref(),
            ])?;

            Ok(PacificaOrderUpdate {
                symbol: update.symbol,
                client_order_id: update.client_order_id,
                exchange_order_id: update.exchange_order_id,
                side: update.side,
                price: update.initial_price,
                status: parse_status(&update.status)?,
                leaves_qty,
                exec_qty,
                exec_price,
                exch_timestamp: update.updated_at_ms * 1_000_000,
            })
        })
        .collect()
}

pub fn apply_order_updates(
    order_manager: &mut OrderManager,
    text: &str,
) -> Result<Vec<LiveEvent>, PrivateStreamError> {
    let mut events = Vec::new();
    for update in parse_order_updates(text)? {
        if let Some(event) = apply_known_order_update(order_manager, update)? {
            events.push(event);
        }
    }
    Ok(events)
}

pub fn apply_order_updates_if_fresh(
    order_manager: &mut OrderManager,
    account_state: &mut AccountState,
    text: &str,
) -> Result<Vec<LiveEvent>, PrivateStreamError> {
    if !account_state.accept_frame_li(max_li_from_text(text)?) {
        return Ok(Vec::new());
    }
    apply_order_updates(order_manager, text)
}

pub fn parse_account_trades(text: &str) -> Result<Vec<PacificaOrderUpdate>, PrivateStreamError> {
    let frame: AccountTradesFrame = serde_json::from_str(text)?;
    frame
        .data
        .into_iter()
        .map(|trade| {
            Ok(PacificaOrderUpdate {
                symbol: trade.symbol,
                client_order_id: trade.client_order_id,
                exchange_order_id: trade.exchange_order_id,
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: None,
                exec_qty: Some(parse_f64("trade_amount", &trade.amount)?),
                exec_price: Some(parse_f64("trade_price", &trade.price)?),
                exch_timestamp: trade.timestamp_ms * 1_000_000,
            })
        })
        .collect()
}

pub fn apply_account_trades(
    order_manager: &mut OrderManager,
    text: &str,
) -> Result<Vec<LiveEvent>, PrivateStreamError> {
    let mut events = Vec::new();
    for update in parse_account_trades(text)? {
        if let Some(event) = apply_known_order_update(order_manager, update)? {
            events.push(event);
        }
    }
    Ok(events)
}

pub fn apply_account_trades_if_fresh(
    order_manager: &mut OrderManager,
    account_state: &mut AccountState,
    text: &str,
) -> Result<Vec<LiveEvent>, PrivateStreamError> {
    if !account_state.accept_frame_li(max_li_from_text(text)?) {
        return Ok(Vec::new());
    }
    apply_account_trades(order_manager, text)
}

pub fn position_events(text: &str) -> Result<Vec<LiveEvent>, PrivateStreamError> {
    let tracked_symbols = tracked_symbols_from_positions_text(text)?;
    AccountState::new()
        .apply_positions_text(text, &tracked_symbols)
        .map_err(PrivateStreamError::from)
}

fn apply_known_order_update(
    order_manager: &mut OrderManager,
    update: PacificaOrderUpdate,
) -> Result<Option<LiveEvent>, PrivateStreamError> {
    match order_manager.apply_update(update.clone()) {
        Ok(OrderExt { symbol, order, .. }) => Ok(Some(LiveEvent::Order { symbol, order })),
        Err(OrderManagerError::UnresolvedUpdate | OrderManagerError::OrderNotFound) => {
            warn!(
                symbol = %update.symbol,
                client_order_id = ?update.client_order_id,
                exchange_order_id = ?update.exchange_order_id,
                "Ignoring unresolved Pacifica private order event"
            );
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn parse_status(status: &str) -> Result<PacificaOrderStatus, PrivateStreamError> {
    match status {
        "open" => Ok(PacificaOrderStatus::Open),
        "partially_filled" | "partial_filled" | "partial" => {
            Ok(PacificaOrderStatus::PartiallyFilled)
        }
        "filled" => Ok(PacificaOrderStatus::Filled),
        "cancelled" | "canceled" => Ok(PacificaOrderStatus::Cancelled),
        "rejected" => Ok(PacificaOrderStatus::Rejected),
        other => Err(PrivateStreamError::UnsupportedOrderStatus(
            other.to_string(),
        )),
    }
}

fn optional_f64(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<f64>, PrivateStreamError> {
    match value {
        Some(value) if !value.is_empty() => {
            value
                .parse()
                .map(Some)
                .map_err(|_| PrivateStreamError::InvalidNumber {
                    field,
                    value: value.to_string(),
                })
        }
        _ => Ok(None),
    }
}

fn parse_f64(field: &'static str, value: &str) -> Result<f64, PrivateStreamError> {
    value
        .parse()
        .map_err(|_| PrivateStreamError::InvalidNumber {
            field,
            value: value.to_string(),
        })
}

fn first_positive_price<const N: usize>(
    values: [Option<&str>; N],
) -> Result<Option<f64>, PrivateStreamError> {
    for value in values {
        if let Some(price) = optional_f64("exec_price", value)? {
            if price > 0.0 {
                return Ok(Some(price));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::GetOrders;
    use hftbacktest::prelude::{OrdType, Order, Side, Status, TimeInForce};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CapturedMessages {
        messages: Vec<serde_json::Value>,
    }

    fn fixture_message(path: &str, index: usize) -> String {
        let captured: CapturedMessages = serde_json::from_str(path).unwrap();
        serde_json::to_string(&captured.messages[index]).unwrap()
    }

    fn order_update_fixture_message(index: usize) -> String {
        let captured: CapturedMessages = serde_json::from_str(include_str!(
            "../../fixtures/pacifica/captured/private_account_order_updates_sample.json"
        ))
        .unwrap();
        serde_json::to_string(&captured.messages[index]).unwrap()
    }

    #[test]
    fn captured_order_update_open_then_filled_removes_active_order() {
        let raw_text = order_update_fixture_message(0);
        let (mut manager, client_order_id) =
            seeded_manager_with_client_order_id(price_tick_from_order_update(&raw_text));
        let text = replace_client_order_ids(&raw_text, &client_order_id);
        let expected_ts = timestamp_from_order_update(&text);
        let events = apply_order_updates(&mut manager, &text).unwrap();

        assert_eq!(events.len(), 2);
        let LiveEvent::Order { symbol, order } = &events[0] else {
            panic!("expected order event");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.status, Status::New);
        assert_eq!(order.leaves_qty, 0.0002);
        assert_eq!(order.exch_timestamp, expected_ts);

        let LiveEvent::Order { order, .. } = &events[1] else {
            panic!("expected order event");
        };
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.leaves_qty, 0.0);
        assert!(manager.orders(Some("BTC".to_string())).is_empty());
    }

    #[test]
    fn order_update_cancelled_removes_active_order() {
        let (mut manager, client_order_id) = seeded_manager_with_client_order_id(63_441);
        let open_text = format!(
            r#"{{"channel":"account_order_updates","data":[{{"I":"{client_order_id}","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672102}}]}}"#
        );
        let cancel_text = format!(
            r#"{{"channel":"account_order_updates","data":[{{"I":"{client_order_id}","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","lp":"0","os":"cancelled","p":"0","s":"BTC","ut":1780919680798}}]}}"#
        );
        apply_order_updates(&mut manager, &open_text).unwrap();
        let expected_ts = timestamp_from_order_update(&cancel_text);
        let events = apply_order_updates(&mut manager, &cancel_text).unwrap();

        assert_eq!(events.len(), 1);
        let LiveEvent::Order { order, .. } = &events[0] else {
            panic!("expected order event");
        };
        assert_eq!(order.status, Status::Canceled);
        assert_eq!(order.leaves_qty, 0.0002);
        assert_eq!(order.exch_timestamp, expected_ts);
        assert!(manager.orders(Some("BTC".to_string())).is_empty());
    }

    #[test]
    fn unknown_order_update_is_ignored_without_interrupting_stream() {
        let mut manager = OrderManager::new("pfhbt-");
        let text = r#"{"channel":"account_order_updates","data":[{"I":"external-order","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672102}]}"#;

        let events = apply_order_updates(&mut manager, text).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn mixed_known_and_unknown_order_updates_keep_known_event() {
        let (mut manager, client_order_id) = seeded_manager_with_client_order_id(63_441);
        let text = format!(
            r#"{{"channel":"account_order_updates","data":[{{"I":"external-order","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672102}},{{"I":"{client_order_id}","a":"0.0002","d":"bid","f":"0","i":336542010,"ip":"63441","lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672103}}]}}"#
        );

        let events = apply_order_updates(&mut manager, &text).unwrap();

        assert_eq!(events.len(), 1);
        let LiveEvent::Order { order, .. } = &events[0] else {
            panic!("expected order event");
        };
        assert_eq!(order.status, Status::New);
        assert_eq!(order.exch_timestamp, 1_780_919_672_103_000_000);
    }

    #[test]
    fn account_trade_updates_execution_fields() {
        let (mut manager, client_order_id) = seeded_manager_with_client_order_id(63_441);
        let text = format!(
            r#"{{"channel":"account_trades","data":[{{"I":"{client_order_id}","a":"0.0001","i":336542009,"p":"63441","s":"BTC","t":1780919672102,"ts":"open_long"}}]}}"#
        );

        let events = apply_account_trades(&mut manager, &text).unwrap();

        assert_eq!(events.len(), 1);
        let LiveEvent::Order { symbol, order } = &events[0] else {
            panic!("expected order event");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.status, Status::PartiallyFilled);
        assert_eq!(order.leaves_qty, 0.0001);
        assert_eq!(order.exec_qty, 0.0001);
        assert_eq!(order.exec_price_tick, 63_441);
        assert_eq!(order.exch_timestamp, 1_780_919_672_102_000_000);
        assert_eq!(manager.orders(Some("BTC".to_string())).len(), 1);
    }

    #[test]
    fn account_trade_full_fill_removes_active_order() {
        let (mut manager, client_order_id) = seeded_manager_with_client_order_id(63_441);
        let text = format!(
            r#"{{"channel":"account_trades","data":[{{"I":"{client_order_id}","a":"0.0002","i":336542009,"p":"63441","s":"BTC","t":1780919672102,"ts":"open_long"}}]}}"#
        );

        let events = apply_account_trades(&mut manager, &text).unwrap();

        assert_eq!(events.len(), 1);
        let LiveEvent::Order { order, .. } = &events[0] else {
            panic!("expected order event");
        };
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.leaves_qty, 0.0);
        assert_eq!(order.exec_qty, 0.0002);
        assert!(manager.orders(Some("BTC".to_string())).is_empty());
    }

    #[test]
    fn unknown_account_trade_is_ignored_without_interrupting_stream() {
        let mut manager = OrderManager::new("pfhbt-");
        let text = r#"{"channel":"account_trades","data":[{"I":"external-order","a":"0.0002","i":336542009,"p":"63441","s":"BTC","t":1780919672102,"ts":"open_long"}]}"#;

        let events = apply_account_trades(&mut manager, text).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn captured_account_trade_parses_fill_fields() {
        let text = fixture_message(
            include_str!("../../fixtures/pacifica/captured/private_account_trades_sample.json"),
            0,
        );

        let updates = parse_account_trades(&text).unwrap();

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].symbol, "BTC");
        assert_eq!(
            updates[0].client_order_id.as_deref(),
            Some("26350e10-72a4-4de1-b1f8-5bc671dd561b")
        );
        assert_eq!(updates[0].exchange_order_id, Some(336542009));
        assert_eq!(updates[0].status, PacificaOrderStatus::PartiallyFilled);
        assert_eq!(updates[0].exec_qty, Some(0.0002));
        assert_eq!(updates[0].exec_price, Some(63441.0));
        assert_eq!(updates[0].exch_timestamp, 1_780_919_672_102_000_000);
    }

    #[test]
    fn stale_order_update_li_is_ignored() {
        let mut state = AccountState::new();
        assert!(state.accept_frame_li(Some(10)));
        let mut manager = seeded_manager(63_441);
        let stale_text = r#"{"channel":"account_order_updates","data":[{"I":"pfhbt-btc-42","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","li":9,"lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672102}]}"#;

        let events = apply_order_updates_if_fresh(&mut manager, &mut state, stale_text).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn equal_trade_li_is_accepted() {
        let mut state = AccountState::new();
        assert!(state.accept_frame_li(Some(10)));
        let (mut manager, client_order_id) = seeded_manager_with_client_order_id(63_441);
        let text = format!(
            r#"{{"channel":"account_trades","data":[{{"I":"{client_order_id}","a":"0.0002","i":336542009,"li":10,"p":"63441","s":"BTC","t":1780919672102,"ts":"open_long"}}]}}"#
        );

        let events = apply_account_trades_if_fresh(&mut manager, &mut state, &text).unwrap();

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn position_update_maps_signed_quantity() {
        let text = fixture_message(
            include_str!("../../fixtures/pacifica/captured/private_account_positions_sample.json"),
            0,
        );

        let events = position_events(&text).unwrap();

        assert_eq!(events.len(), 1);
        let LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } = &events[0]
        else {
            panic!("expected position event");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(*qty, 0.0002);
        assert_eq!(*exch_ts, 1_780_919_672_151_000_000);
    }

    #[test]
    fn flat_position_update_maps_to_no_events() {
        let text = fixture_message(
            include_str!("../../fixtures/pacifica/captured/private_account_positions_sample.json"),
            1,
        );

        assert!(position_events(&text).unwrap().is_empty());
    }

    fn seeded_manager(price_tick: i64) -> OrderManager {
        seeded_manager_with_client_order_id(price_tick).0
    }

    fn seeded_manager_with_client_order_id(price_tick: i64) -> (OrderManager, String) {
        let mut manager = OrderManager::new("pfhbt-");
        let client_order_id = manager
            .new_order(
                "BTC",
                Order::new(
                    42,
                    price_tick,
                    1.0,
                    0.0002,
                    Side::Buy,
                    OrdType::Limit,
                    TimeInForce::GTX,
                ),
            )
            .unwrap()
            .client_order_id;
        (manager, client_order_id)
    }

    fn price_tick_from_order_update(text: &str) -> i64 {
        let frame: serde_json::Value = serde_json::from_str(text).unwrap();
        frame["data"][0]["ip"].as_str().unwrap().parse().unwrap()
    }

    fn timestamp_from_order_update(text: &str) -> i64 {
        let frame: serde_json::Value = serde_json::from_str(text).unwrap();
        frame["data"][0]["ut"].as_i64().unwrap() * 1_000_000
    }

    fn replace_client_order_ids(text: &str, client_order_id: &str) -> String {
        let mut frame: serde_json::Value = serde_json::from_str(text).unwrap();
        let data = frame["data"].as_array_mut().unwrap();
        for update in data {
            update["I"] = serde_json::Value::String(client_order_id.to_string());
        }
        serde_json::to_string(&frame).unwrap()
    }
}

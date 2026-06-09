use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Value};
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, Sender},
    mpsc::UnboundedSender,
};
use tracing::{info, warn};

use crate::connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent};

pub mod account_state;
pub mod config;
pub mod msg;
pub mod order_payload;
pub mod ordermanager;
pub mod private_stream;
pub mod public_stream;
pub mod rest;
pub mod sanity;
pub mod signing;
pub mod startup;
pub mod trade_stream;
pub mod websocket;
pub mod ws_trade;

use account_state::AccountState;
use config::{Config, ConfigError};
use ordermanager::{OrderExt, OrderManager};
use private_stream::PrivateStream;
use public_stream::PublicStream;
use trade_stream::{TradeClient, TradeError};
use websocket::WebSocketError;

#[derive(Debug, Error)]
pub enum PacificaError {
    #[error("Config: {0}")]
    Config(#[from] toml::de::Error),
    #[error("InvalidConfig: {0}")]
    InvalidConfig(#[from] ConfigError),
    #[error("Trade: {0}")]
    Trade(#[from] TradeError),
    #[error("WebSocket: {0}")]
    WebSocket(#[from] WebSocketError),
}

impl PacificaError {
    pub fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

#[derive(Debug)]
pub struct Pacifica {
    config: Config,
    order_manager: Arc<Mutex<OrderManager>>,
    account_state: Arc<Mutex<AccountState>>,
    trade_client: TradeClient,
    symbols: Arc<Mutex<HashSet<String>>>,
    symbol_tx: Sender<String>,
}

impl ConnectorBuilder for Pacifica {
    type Error = PacificaError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        let config = config.resolve()?;
        config.validate()?;
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let account_state = Arc::new(Mutex::new(AccountState::new()));
        let trade_client = TradeClient::new(&config);
        let (symbol_tx, _) = broadcast::channel(128);
        Ok(Self {
            config,
            order_manager,
            account_state,
            trade_client,
            symbols: Default::default(),
            symbol_tx,
        })
    }
}

impl Connector for Pacifica {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if symbols.insert(symbol.clone()) {
            info!(%symbol, "Pacifica connector registered symbol");
            let _ = self.symbol_tx.send(symbol);
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, tx: UnboundedSender<PublishEvent>) {
        self.trade_client.start();
        self.connect_public_stream(tx.clone());
        self.connect_private_stream(tx);
    }

    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let create = match self.order_manager.lock().unwrap().new_order(&symbol, order) {
            Ok(create) => create,
            Err(error) => {
                publish_order_error(tx, error.to_string());
                return;
            }
        };
        let trade_client = self.trade_client.clone();
        let order_manager = self.order_manager.clone();
        tokio::spawn(async move {
            if let Err(error) = trade_client.submit_limit(create.clone()).await {
                let rejected = order_manager
                    .lock()
                    .unwrap()
                    .update_submit_fail(&create.client_order_id);
                if let Ok(OrderExt { symbol, order, .. }) = rejected {
                    let _ = tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }));
                }
                publish_order_error(tx, error.to_string());
            }
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let cancel = match self
            .order_manager
            .lock()
            .unwrap()
            .cancel_order(&symbol, order.order_id)
        {
            Ok(cancel) => cancel,
            Err(error) => {
                publish_order_error(tx, error.to_string());
                return;
            }
        };
        let trade_client = self.trade_client.clone();
        let order_manager = self.order_manager.clone();
        tokio::spawn(async move {
            if let Err(error) = trade_client.cancel_order(cancel.clone()).await {
                let active = order_manager
                    .lock()
                    .unwrap()
                    .update_cancel_fail(&cancel.client_order_id);
                if let Ok(OrderExt { symbol, order, .. }) = active {
                    let _ = tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }));
                }
                publish_order_error(tx, error.to_string());
            }
        });
    }

    fn modify(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let edit = match self
            .order_manager
            .lock()
            .unwrap()
            .edit_order(&symbol, order)
        {
            Ok(edit) => edit,
            Err(error) => {
                publish_order_error(tx, error.to_string());
                return;
            }
        };
        let trade_client = self.trade_client.clone();
        let order_manager = self.order_manager.clone();
        tokio::spawn(async move {
            match trade_client.edit_order(edit.clone()).await {
                Ok(()) => {
                    let active = order_manager
                        .lock()
                        .unwrap()
                        .update_edit_ack(&edit.client_order_id);
                    if let Ok(OrderExt { symbol, order, .. }) = active {
                        let _ =
                            tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }));
                    }
                }
                Err(error) => {
                    let active = order_manager
                        .lock()
                        .unwrap()
                        .update_edit_fail(&edit.client_order_id);
                    if let Ok(OrderExt { symbol, order, .. }) = active {
                        let _ =
                            tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }));
                    }
                    publish_order_error(tx, error.to_string());
                }
            }
        });
    }
}

impl Pacifica {
    fn connect_public_stream(&self, tx: UnboundedSender<PublishEvent>) {
        let url = self.config.public_url.clone();
        let symbol_rx = self.symbol_tx.subscribe();
        let symbols = self.symbols.clone();
        tokio::spawn(async move {
            loop {
                let stream =
                    PublicStream::new(tx.clone(), symbol_rx.resubscribe(), symbols.clone(), 1);
                if let Err(error) = stream.connect(&url).await {
                    warn!(?error, "Pacifica public stream interrupted");
                    publish_connection_error(tx.clone(), error.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }

    fn connect_private_stream(&self, tx: UnboundedSender<PublishEvent>) {
        let url = self.config.private_url.clone();
        let account = self.config.account.clone();
        let order_manager = self.order_manager.clone();
        let account_state = self.account_state.clone();
        let symbols = self.symbols.clone();
        tokio::spawn(async move {
            loop {
                let stream = PrivateStream::new(
                    tx.clone(),
                    order_manager.clone(),
                    account_state.clone(),
                    symbols.clone(),
                    account.clone(),
                );
                if let Err(error) = stream.connect(&url).await {
                    warn!(?error, "Pacifica private stream interrupted");
                    publish_connection_error(tx.clone(), error.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }
}

fn publish_order_error(tx: UnboundedSender<PublishEvent>, message: String) {
    let mut map = HashMap::new();
    map.insert("msg".to_string(), Value::String(message));
    let _ = tx.send(PublishEvent::LiveEvent(LiveEvent::Error(
        hftbacktest::types::LiveError::with(
            hftbacktest::types::ErrorKind::OrderError,
            Value::Map(map),
        ),
    )));
}

fn publish_connection_error(tx: UnboundedSender<PublishEvent>, message: String) {
    let _ = tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
        ErrorKind::ConnectionInterrupted,
        Value::String(message),
    ))));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connector::{Connector, ConnectorBuilder},
        pacifica::{private_stream::apply_order_updates, public_stream::map_public_message},
    };
    use hftbacktest::prelude::{OrdType, Order, Side, Status, TimeInForce};
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn build_from_valid_config() {
        let connector = Pacifica::build_from(
            r#"
account = "acct"
private_key_file = "key.json"
"#,
        )
        .unwrap();

        assert_eq!(connector.config.account, "acct");
    }

    #[test]
    fn build_from_rejects_missing_account_and_key() {
        let error = Pacifica::build_from(
            r#"
private_key_file = ""
"#,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            PacificaError::InvalidConfig(ConfigError::Missing("account"))
        ));
    }

    #[test]
    fn build_from_derives_missing_account_from_key_file() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/pacifica/signing_create_order.json"
        ))
        .unwrap();
        let key_file = temp_key_file(&fixture);
        let config = format!(
            r#"
private_key_file = "{}"
"#,
            key_file.to_string_lossy()
        );

        let connector = Pacifica::build_from(&config).unwrap();
        let _ = fs::remove_file(&key_file);

        assert_eq!(
            connector.config.account,
            fixture["public_key"].as_str().unwrap()
        );
    }

    #[test]
    fn register_tracks_symbol_and_signals_subscription() {
        let mut connector = Pacifica::build_from(
            r#"
account = "acct"
private_key_file = "key.json"
"#,
        )
        .unwrap();
        let mut rx = connector.symbol_tx.subscribe();

        connector.register("BTC".to_string());
        connector.register("BTC".to_string());

        assert_eq!(rx.try_recv().unwrap(), "BTC");
        assert!(rx.try_recv().is_err());
        assert!(connector.symbols.lock().unwrap().contains("BTC"));
    }

    #[test]
    fn connector_order_manager_exposes_active_orders() {
        let connector = Pacifica::build_from(
            r#"
account = "acct"
private_key_file = "key.json"
"#,
        )
        .unwrap();
        let created = connector
            .order_manager
            .lock()
            .unwrap()
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
        connector
            .order_manager
            .lock()
            .unwrap()
            .apply_update(ordermanager::PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created.client_order_id),
                exchange_order_id: Some(336542009),
                side: Some("bid".to_string()),
                price: Some("63441".to_string()),
                status: ordermanager::PacificaOrderStatus::Open,
                leaves_qty: Some(0.0002),
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 1,
            })
            .unwrap();

        let orders = connector
            .order_manager()
            .lock()
            .unwrap()
            .orders(Some("BTC".to_string()));

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, 42);
    }

    #[tokio::test]
    async fn connector_modify_sends_edit_order() {
        let connector = test_connector();
        let created = open_connector_order(&connector);
        let mut probe_rx = connector.trade_client.install_success_probe();
        let (tx, mut rx) = unbounded_channel();

        connector.modify("BTC".to_string(), replacement_order(42, 63_442, 0.0002), tx);

        let request = tokio::time::timeout(Duration::from_secs(1), probe_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(request.operation_type, "edit_order");
        assert_eq!(request.body["symbol"], "BTC");
        assert_eq!(request.body["price"], "63442");
        assert_eq!(request.body["amount"], "0.0002");
        assert_eq!(request.body["client_order_id"], created);
        let PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }) = event else {
            panic!("expected order response");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.order_id, 42);
        assert_eq!(order.price_tick, 63_442);
        assert_eq!(order.req, Status::None);
        assert_eq!(order.status, Status::New);
    }

    #[tokio::test]
    async fn connector_modify_failure_does_not_drop_order() {
        let connector = test_connector();
        let created = open_connector_order(&connector);
        let (tx, mut rx) = unbounded_channel();

        connector.modify("BTC".to_string(), replacement_order(42, 63_442, 0.0002), tx);

        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = event else {
            panic!("expected restored order response");
        };
        let active = connector
            .order_manager
            .lock()
            .unwrap()
            .orders(Some("BTC".to_string()));

        assert_eq!(order.order_id, 42);
        assert_eq!(order.price_tick, 63_441);
        assert_eq!(order.req, Status::None);
        assert_eq!(order.status, Status::New);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].price_tick, 63_441);
        assert_eq!(
            connector
                .order_manager
                .lock()
                .unwrap()
                .cancel_order("BTC", 42)
                .unwrap()
                .client_order_id,
            created
        );
    }

    #[test]
    fn fake_stream_bot_receives_feed_and_order_events() {
        let (tx, mut rx) = unbounded_channel();
        let feed_text = {
            let captured: serde_json::Value = serde_json::from_str(include_str!(
                "../../fixtures/pacifica/captured/public_bbo_sample.json"
            ))
            .unwrap();
            serde_json::to_string(&captured["messages"][0]).unwrap()
        };
        for feed in map_public_message(&feed_text, 100).unwrap() {
            tx.send(PublishEvent::LiveEvent(LiveEvent::Feed {
                symbol: feed.symbol,
                event: feed.event,
            }))
            .unwrap();
        }

        let mut order_manager = OrderManager::new("pfhbt-");
        let created = order_manager
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
        let order_text = format!(
            r#"{{"channel":"account_order_updates","data":[{{"I":"{}","a":"0.0002","d":"bid","f":"0","i":336542009,"ip":"63441","lp":"0","os":"open","p":"0","s":"BTC","ut":1780919672102}}]}}"#,
            created.client_order_id
        );
        for event in apply_order_updates(&mut order_manager, &order_text).unwrap() {
            tx.send(PublishEvent::LiveEvent(event)).unwrap();
        }

        let first = rx.try_recv().unwrap();
        let second = rx.try_recv().unwrap();
        let third = rx.try_recv().unwrap();

        assert!(matches!(
            first,
            PublishEvent::LiveEvent(LiveEvent::Feed { .. })
        ));
        assert!(matches!(
            second,
            PublishEvent::LiveEvent(LiveEvent::Feed { .. })
        ));
        let PublishEvent::LiveEvent(LiveEvent::Order { order, .. }) = third else {
            panic!("expected order event");
        };
        assert_eq!(order.status, Status::New);
    }

    fn temp_key_file(fixture: &serde_json::Value) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pacifica-build-key-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &path,
            serde_json::to_string(&fixture["private_key_uint8"]).unwrap(),
        )
        .unwrap();
        path
    }

    fn test_connector() -> Pacifica {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/pacifica/signing_create_order.json"
        ))
        .unwrap();
        let key_file = temp_key_file(&fixture);
        let config = format!(
            r#"
private_key_file = "{}"
"#,
            key_file.to_string_lossy()
        );
        let connector = Pacifica::build_from(&config).unwrap();
        connector
    }

    fn open_connector_order(connector: &Pacifica) -> String {
        let created = connector
            .order_manager
            .lock()
            .unwrap()
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
        connector
            .order_manager
            .lock()
            .unwrap()
            .apply_update(ordermanager::PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created.client_order_id.clone()),
                exchange_order_id: Some(336542009),
                side: Some("bid".to_string()),
                price: Some("63441".to_string()),
                status: ordermanager::PacificaOrderStatus::Open,
                leaves_qty: Some(0.0002),
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 1,
            })
            .unwrap();
        created.client_order_id
    }

    fn replacement_order(order_id: u64, price_tick: i64, qty: f64) -> Order {
        let mut order = Order::new(
            order_id,
            price_tick,
            1.0,
            qty,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTX,
        );
        order.status = Status::New;
        order.req = Status::Replaced;
        order
    }
}

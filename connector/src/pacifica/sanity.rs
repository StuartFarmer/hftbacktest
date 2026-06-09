use std::{fs, time::Duration};

use clap::Parser;
use hftbacktest::prelude::{OrdType, Order, Side, Status, TimeInForce};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio::time::sleep;

use crate::pacifica::{
    config::{Config, ConfigError},
    ordermanager::OrderManager,
    rest::{BookSnapshot, MarketInfo, RestPayload},
    startup::MarketSpec,
    trade_stream::{TradeClient, TradeError},
};

#[derive(Parser, Debug, Clone)]
#[command(about = "Run guarded Pacifica testnet create/cancel sanity checks.")]
pub struct SanityArgs {
    /// Pacifica connector TOML config path.
    pub config: String,

    /// Symbol to probe.
    #[arg(long, default_value = "BTC")]
    pub symbol: String,

    /// Side to quote first: bid or ask.
    #[arg(long, default_value = "bid")]
    pub side: String,

    /// Base order amount. The runner raises this if min notional requires it.
    #[arg(long, default_value_t = 0.0002)]
    pub amount: f64,

    /// Distance from current best bid/ask, in ticks.
    #[arg(long, default_value_t = 500)]
    pub quote_offset_ticks: i64,

    /// Required minimum notional for sanity orders.
    #[arg(long, default_value_t = 11.0)]
    pub min_order_notional: f64,

    /// Submit real orders. Without this flag the command exits before trading.
    #[arg(long)]
    pub execute: bool,

    /// Permit non-testnet URLs. This is intentionally explicit.
    #[arg(long)]
    pub allow_non_testnet: bool,

    /// Delay after submit and cancel calls before checking open orders.
    #[arg(long, default_value_t = 750)]
    pub settle_millis: u64,
}

#[derive(Debug, Error)]
pub enum SanityError {
    #[error("config: {0}")]
    Config(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    InvalidConfig(#[from] ConfigError),
    #[error("read config: {0}")]
    ReadConfig(String),
    #[error("sanity refuses to submit without --execute")]
    MissingExecute,
    #[error("sanity refuses non-testnet config without --allow-non-testnet")]
    NonTestnet,
    #[error("invalid side: {0}")]
    InvalidSide(String),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("market: {0}")]
    Market(String),
    #[error("trade: {0}")]
    Trade(#[from] TradeError),
    #[error("residual sanity client order ids remain open: {0:?}")]
    ResidualOrders(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SanityPlan {
    pub symbol: String,
    pub first_price: f64,
    pub second_price: f64,
    pub tick_size: f64,
    pub amount: f64,
    pub side: Side,
    pub offset_ticks: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SanitySummary {
    pub creates_ok: usize,
    pub edits_ok: usize,
    pub cancels_ok: usize,
    pub residual_client_order_ids: Vec<String>,
    pub edited_client_order_id: Option<String>,
    pub edited_exchange_order_id: Option<i64>,
}

pub async fn run(args: SanityArgs) -> Result<SanitySummary, SanityError> {
    if !args.execute {
        return Err(SanityError::MissingExecute);
    }

    let config_text = fs::read_to_string(&args.config)
        .map_err(|error| SanityError::ReadConfig(error.to_string()))?;
    let config: Config = toml::from_str(&config_text)?;
    let config = config.resolve()?;
    config.validate()?;
    validate_testnet_guard(&config, args.allow_non_testnet)?;

    let client = reqwest::Client::new();
    let info_text = get_text(&client, &config, "/api/v1/info", &[]).await?;
    let market = parse_market_info_for_symbol(&info_text, &args.symbol)?;
    let book_text = get_text(
        &client,
        &config,
        "/api/v1/book",
        &[("symbol", args.symbol.as_str())],
    )
    .await?;
    let book = parse_book(&book_text)?;
    let plan = plan_from_book(&args, &market, &book)?;

    let mut manager = OrderManager::new(&config.order_prefix);
    let trade_client = TradeClient::new(&config);
    trade_client.start();
    let mut created_ids = Vec::new();
    let mut creates_ok = 0;
    let mut edits_ok = 0;
    let mut cancels_ok = 0;
    let order = sanity_order(1, plan.first_price, &plan);
    let payload = manager
        .new_order(&plan.symbol, order)
        .map_err(|error| SanityError::Market(error.to_string()))?;
    let client_order_id = payload.client_order_id.clone();
    trade_client.submit_limit(payload).await?;
    creates_ok += 1;
    created_ids.push(client_order_id.clone());
    sleep(Duration::from_millis(args.settle_millis)).await;

    let orders_after_create = get_text(
        &client,
        &config,
        "/api/v1/orders",
        &[("account", config.account.as_str())],
    )
    .await?;
    let create_exchange_order_id = open_order_exchange_id(&orders_after_create, &client_order_id)?;
    mark_order_open(
        &mut manager,
        &plan,
        &client_order_id,
        create_exchange_order_id,
        1,
    )?;

    let edit = manager
        .edit_order(
            &plan.symbol,
            sanity_replacement_order(1, plan.second_price, &plan),
        )
        .map_err(|error| SanityError::Market(error.to_string()))?;
    trade_client.edit_order(edit.clone()).await?;
    manager
        .update_edit_ack(&edit.client_order_id)
        .map_err(|error| SanityError::Market(error.to_string()))?;
    edits_ok += 1;
    sleep(Duration::from_millis(args.settle_millis)).await;

    let orders_after_edit = get_text(
        &client,
        &config,
        "/api/v1/orders",
        &[("account", config.account.as_str())],
    )
    .await?;
    let edited_exchange_order_id = open_order_exchange_id(&orders_after_edit, &client_order_id)?;

    let cancel = manager
        .cancel_order(&plan.symbol, 1)
        .map_err(|error| SanityError::Market(error.to_string()))?;
    trade_client.cancel_order(cancel).await?;
    cancels_ok += 1;
    sleep(Duration::from_millis(args.settle_millis)).await;

    let orders_text = get_text(
        &client,
        &config,
        "/api/v1/orders",
        &[("account", config.account.as_str())],
    )
    .await?;
    let residual_client_order_ids = residual_client_order_ids(&orders_text, &created_ids)?;
    if !residual_client_order_ids.is_empty() {
        return Err(SanityError::ResidualOrders(residual_client_order_ids));
    }

    Ok(SanitySummary {
        creates_ok,
        edits_ok,
        cancels_ok,
        residual_client_order_ids,
        edited_client_order_id: Some(client_order_id),
        edited_exchange_order_id,
    })
}

pub fn validate_testnet_guard(config: &Config, allow_non_testnet: bool) -> Result<(), SanityError> {
    if allow_non_testnet {
        return Ok(());
    }
    let urls = [
        &config.rest_url,
        &config.public_url,
        &config.private_url,
        &config.trade_url,
    ];
    if config.testnet && urls.iter().all(|url| url.contains("test-")) {
        Ok(())
    } else {
        Err(SanityError::NonTestnet)
    }
}

pub fn plan_from_book(
    args: &SanityArgs,
    market: &MarketSpec,
    book: &BookSnapshot,
) -> Result<SanityPlan, SanityError> {
    let side = parse_side(&args.side)?;
    let levels = book
        .levels
        .get(match side {
            Side::Buy => 0,
            Side::Sell => 1,
            _ => unreachable!(),
        })
        .and_then(|levels| levels.first())
        .ok_or_else(|| SanityError::Market("missing book side".to_string()))?;
    let best_price: f64 = levels
        .price
        .parse()
        .map_err(|_| SanityError::Market(format!("invalid book price: {}", levels.price)))?;
    let offset = args.quote_offset_ticks as f64 * market.tick_size;
    let first_price = match side {
        Side::Buy => floor_to_increment(best_price - offset, market.tick_size),
        Side::Sell => ceil_to_increment(best_price + offset, market.tick_size),
        _ => unreachable!(),
    };
    let second_price = match side {
        Side::Buy => floor_to_increment(first_price - market.tick_size, market.tick_size),
        Side::Sell => ceil_to_increment(first_price + market.tick_size, market.tick_size),
        _ => unreachable!(),
    };
    let required_notional = market.min_order_size.max(args.min_order_notional);
    let amount = order_amount(args.amount, first_price, market.lot_size, required_notional);
    Ok(SanityPlan {
        symbol: args.symbol.clone(),
        first_price,
        second_price,
        tick_size: market.tick_size,
        amount,
        side,
        offset_ticks: args.quote_offset_ticks,
    })
}

pub fn residual_client_order_ids(
    orders_text: &str,
    expected_client_order_ids: &[String],
) -> Result<Vec<String>, SanityError> {
    let payload: RestPayload<Vec<Value>> = parse_rest_payload(orders_text)?;
    let residual = payload
        .data
        .into_iter()
        .filter_map(|order| {
            let client_order_id = order
                .get("client_order_id")
                .or_else(|| order.get("clientOrderId"))
                .or_else(|| order.get("coid"))
                .and_then(Value::as_str)?;
            expected_client_order_ids
                .iter()
                .any(|expected| expected == client_order_id)
                .then(|| client_order_id.to_string())
        })
        .collect();
    Ok(residual)
}

pub fn open_order_exchange_id(
    orders_text: &str,
    expected_client_order_id: &str,
) -> Result<Option<i64>, SanityError> {
    let payload: RestPayload<Vec<Value>> = parse_rest_payload(orders_text)?;
    Ok(payload
        .data
        .into_iter()
        .find(|order| order_client_order_id(order).as_deref() == Some(expected_client_order_id))
        .and_then(|order| order_exchange_order_id(&order)))
}

fn sanity_order(order_id: u64, price: f64, plan: &SanityPlan) -> Order {
    let mut order = Order::new(
        order_id,
        (price / plan.tick_size).round() as i64,
        plan.tick_size,
        plan.amount,
        plan.side,
        OrdType::Limit,
        TimeInForce::GTX,
    );
    order.req = Status::New;
    order
}

fn sanity_replacement_order(order_id: u64, price: f64, plan: &SanityPlan) -> Order {
    let mut order = sanity_order(order_id, price, plan);
    order.req = Status::Replaced;
    order.status = Status::New;
    order
}

fn mark_order_open(
    manager: &mut OrderManager,
    plan: &SanityPlan,
    client_order_id: &str,
    exchange_order_id: Option<i64>,
    exch_timestamp: i64,
) -> Result<(), SanityError> {
    manager
        .apply_update(crate::pacifica::ordermanager::PacificaOrderUpdate {
            symbol: plan.symbol.clone(),
            client_order_id: Some(client_order_id.to_string()),
            exchange_order_id,
            side: Some(match plan.side {
                Side::Buy => "bid".to_string(),
                Side::Sell => "ask".to_string(),
                _ => unreachable!(),
            }),
            price: Some(plan.first_price.to_string()),
            status: crate::pacifica::ordermanager::PacificaOrderStatus::Open,
            leaves_qty: Some(plan.amount),
            exec_qty: None,
            exec_price: None,
            exch_timestamp,
        })
        .map_err(|error| SanityError::Market(error.to_string()))?;
    Ok(())
}

fn order_client_order_id(order: &Value) -> Option<String> {
    order
        .get("client_order_id")
        .or_else(|| order.get("clientOrderId"))
        .or_else(|| order.get("coid"))
        .or_else(|| order.get("I"))
        .and_then(Value::as_str)
        .map(String::from)
}

fn order_exchange_order_id(order: &Value) -> Option<i64> {
    for key in ["order_id", "orderId", "exchange_order_id", "id", "i"] {
        if let Some(value) = order.get(key) {
            if let Some(id) = value.as_i64() {
                return Some(id);
            }
            if let Some(id) = value.as_u64().and_then(|id| i64::try_from(id).ok()) {
                return Some(id);
            }
            if let Some(id) = value.as_str().and_then(|id| id.parse().ok()) {
                return Some(id);
            }
        }
    }
    None
}

async fn get_text(
    client: &reqwest::Client,
    config: &Config,
    path: &str,
    query: &[(&str, &str)],
) -> Result<String, SanityError> {
    let request = client
        .get(format!("{}{}", config.rest_url.trim_end_matches('/'), path))
        .query(query);
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(SanityError::Market(format!("{path} failed: {text}")));
    }
    Ok(text)
}

fn parse_book(text: &str) -> Result<BookSnapshot, SanityError> {
    let payload: RestPayload<BookSnapshot> = parse_rest_payload(text)?;
    Ok(payload.data)
}

fn parse_market_info_for_symbol(text: &str, symbol: &str) -> Result<MarketSpec, SanityError> {
    let payload: RestPayload<Vec<MarketInfo>> = parse_rest_payload(text)?;
    let info = payload
        .data
        .into_iter()
        .find(|info| info.symbol == symbol)
        .ok_or_else(|| SanityError::Market(format!("missing market info for symbol: {symbol}")))?;

    Ok(MarketSpec {
        symbol: info.symbol,
        tick_size: parse_f64("tick_size", &info.tick_size)?,
        lot_size: parse_f64("lot_size", &info.lot_size)?,
        min_order_size: parse_f64("min_order_size", &info.min_order_size)?,
    })
}

fn parse_rest_payload<T>(text: &str) -> Result<RestPayload<T>, SanityError>
where
    T: for<'de> Deserialize<'de>,
{
    let value: Value = serde_json::from_str(text)?;
    let payload = value.get("payload").cloned().unwrap_or(value);
    Ok(serde_json::from_value(payload)?)
}

fn parse_f64(field: &'static str, value: &str) -> Result<f64, SanityError> {
    value
        .parse()
        .map_err(|_| SanityError::Market(format!("invalid numeric field {field}: {value}")))
}

fn parse_side(side: &str) -> Result<Side, SanityError> {
    match side {
        "bid" | "buy" => Ok(Side::Buy),
        "ask" | "sell" => Ok(Side::Sell),
        value => Err(SanityError::InvalidSide(value.to_string())),
    }
}

fn order_amount(base_amount: f64, price: f64, lot_size: f64, required_notional: f64) -> f64 {
    let amount = floor_to_increment(base_amount, lot_size);
    if price * amount >= required_notional {
        amount
    } else {
        ceil_to_increment(required_notional / price, lot_size)
    }
}

fn floor_to_increment(value: f64, increment: f64) -> f64 {
    (value / increment).floor() * increment
}

fn ceil_to_increment(value: f64, increment: f64) -> f64 {
    (value / increment).ceil() * increment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> SanityArgs {
        SanityArgs {
            config: "config.toml".to_string(),
            symbol: "BTC".to_string(),
            side: "bid".to_string(),
            amount: 0.0002,
            quote_offset_ticks: 500,
            min_order_notional: 11.0,
            execute: false,
            allow_non_testnet: false,
            settle_millis: 0,
        }
    }

    fn market() -> MarketSpec {
        MarketSpec {
            symbol: "BTC".to_string(),
            tick_size: 1.0,
            lot_size: 0.00001,
            min_order_size: 10.0,
        }
    }

    fn book() -> BookSnapshot {
        serde_json::from_str(
            r#"{"s":"BTC","l":[[{"p":"100000","a":"1"}],[{"p":"100001","a":"1"}]],"t":1}"#,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sanity_requires_execute_flag() {
        assert!(matches!(
            run(args()).await,
            Err(SanityError::MissingExecute)
        ));
    }

    #[test]
    fn sanity_rejects_non_testnet_without_override() {
        let config = Config {
            rest_url: "https://api.pacifica.fi".to_string(),
            public_url: "wss://ws.pacifica.fi/ws".to_string(),
            private_url: "wss://ws.pacifica.fi/ws".to_string(),
            trade_url: "wss://ws.pacifica.fi/ws".to_string(),
            account: "acct".to_string(),
            private_key_file: "key.json".to_string(),
            testnet: false,
            ..Default::default()
        };

        assert!(matches!(
            validate_testnet_guard(&config, false),
            Err(SanityError::NonTestnet)
        ));
        assert!(validate_testnet_guard(&config, true).is_ok());
    }

    #[test]
    fn sanity_price_levels_are_far_from_mid_for_bid() {
        let plan = plan_from_book(&args(), &market(), &book()).unwrap();

        assert_eq!(plan.first_price, 99500.0);
        assert_eq!(plan.second_price, 99499.0);
        assert!(plan.first_price < 100000.0);
        assert!(plan.amount * plan.first_price >= 11.0);
    }

    #[test]
    fn sanity_price_levels_are_far_from_mid_for_ask() {
        let plan = plan_from_book(
            &SanityArgs {
                side: "ask".to_string(),
                ..args()
            },
            &market(),
            &book(),
        )
        .unwrap();

        assert_eq!(plan.first_price, 100501.0);
        assert_eq!(plan.second_price, 100502.0);
    }

    #[test]
    fn sanity_success_requires_no_residual_orders() {
        let expected = vec!["pfhbt-btc-1".to_string(), "pfhbt-btc-2".to_string()];
        let residual = residual_client_order_ids(
            r#"{"payload":{"success":true,"data":[{"client_order_id":"pfhbt-btc-2"}]}}"#,
            &expected,
        )
        .unwrap();

        assert_eq!(residual, vec!["pfhbt-btc-2"]);
    }

    #[test]
    fn empty_orders_are_clean_success() {
        let expected = vec!["pfhbt-btc-1".to_string()];
        let residual =
            residual_client_order_ids(r#"{"payload":{"success":true,"data":[]}}"#, &expected)
                .unwrap();

        assert!(residual.is_empty());
    }

    #[test]
    fn open_order_exchange_id_accepts_rest_and_short_fields() {
        let rest = open_order_exchange_id(
            r#"{"payload":{"success":true,"data":[{"client_order_id":"client","order_id":"645954"}]}}"#,
            "client",
        )
        .unwrap();
        let short = open_order_exchange_id(
            r#"{"payload":{"success":true,"data":[{"I":"client","i":645955}]}}"#,
            "client",
        )
        .unwrap();

        assert_eq!(rest, Some(645954));
        assert_eq!(short, Some(645955));
    }
}

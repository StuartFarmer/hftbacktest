use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RestEnvelope<T> {
    pub payload: RestPayload<T>,
}

#[derive(Debug, Deserialize)]
pub struct RestPayload<T> {
    pub success: Option<bool>,
    pub data: T,
}

#[derive(Debug, Deserialize)]
pub struct MarketInfo {
    pub symbol: String,
    pub tick_size: String,
    pub lot_size: String,
    pub min_order_size: String,
}

#[derive(Debug, Deserialize)]
pub struct BookSnapshot {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "l")]
    pub levels: Vec<Vec<BookLevel>>,
    #[serde(rename = "t")]
    pub timestamp_ms: i64,
}

#[derive(Debug, Deserialize)]
pub struct BookLevel {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "a")]
    pub amount: String,
}

#[derive(Debug, Deserialize)]
pub struct AccountInfo {
    pub account_equity: String,
    pub available_to_spend: String,
    pub orders_count: i64,
    pub positions_count: i64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct PositionSnapshot {
    pub symbol: Option<String>,
    #[serde(rename = "s")]
    pub short_symbol: Option<String>,
    pub amount: Option<String>,
    #[serde(rename = "a")]
    pub short_amount: Option<String>,
    pub side: Option<String>,
    #[serde(rename = "d")]
    pub short_side: Option<String>,
    pub updated_at: Option<i64>,
    #[serde(rename = "ut")]
    pub short_updated_at: Option<i64>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StartupPosition {
    pub symbol: String,
    pub qty: f64,
    pub exch_ts: i64,
}

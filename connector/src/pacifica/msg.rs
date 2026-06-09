use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ChannelFrame {
    pub channel: String,
}

#[derive(Debug, Deserialize)]
pub struct BboFrame {
    pub data: Bbo,
}

#[derive(Debug, Deserialize)]
pub struct Bbo {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_px: String,
    #[serde(rename = "B")]
    pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask_px: String,
    #[serde(rename = "A")]
    pub ask_qty: String,
    #[serde(rename = "t")]
    pub timestamp_ms: i64,
    #[serde(rename = "li")]
    pub last_increment_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BookFrame {
    pub data: Book,
}

#[derive(Debug, Deserialize)]
pub struct Book {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "l")]
    pub levels: Vec<Vec<BookLevel>>,
    #[serde(rename = "t")]
    pub timestamp_ms: i64,
    #[serde(rename = "li")]
    pub last_increment_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BookLevel {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "a")]
    pub amount: String,
}

#[derive(Debug, Deserialize)]
pub struct TradesFrame {
    pub data: Vec<Trade>,
}

#[derive(Debug, Deserialize)]
pub struct Trade {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "a")]
    pub amount: String,
    #[serde(rename = "d")]
    pub direction: String,
    #[serde(rename = "t")]
    pub timestamp_ms: i64,
    #[serde(rename = "li")]
    pub last_increment_id: Option<i64>,
}

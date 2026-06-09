use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CreateOrderPayload {
    pub amount: String,
    pub client_order_id: String,
    pub price: String,
    pub reduce_only: bool,
    pub side: String,
    pub symbol: String,
    pub tif: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CancelOrderPayload {
    pub client_order_id: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EditOrderPayload {
    pub amount: String,
    pub client_order_id: String,
    pub price: String,
    pub symbol: String,
}

impl CreateOrderPayload {
    pub fn new(
        symbol: impl Into<String>,
        price: impl Into<String>,
        amount: impl Into<String>,
        side: impl Into<String>,
        client_order_id: impl Into<String>,
    ) -> Self {
        Self {
            amount: amount.into(),
            client_order_id: client_order_id.into(),
            price: price.into(),
            reduce_only: false,
            side: side.into(),
            symbol: symbol.into(),
            tif: "ALO".to_string(),
        }
    }
}

impl CancelOrderPayload {
    pub fn new(symbol: impl Into<String>, client_order_id: impl Into<String>) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            symbol: symbol.into(),
        }
    }
}

impl EditOrderPayload {
    pub fn new(
        symbol: impl Into<String>,
        price: impl Into<String>,
        amount: impl Into<String>,
        client_order_id: impl Into<String>,
    ) -> Self {
        Self {
            amount: amount.into(),
            client_order_id: client_order_id.into(),
            price: price.into(),
            symbol: symbol.into(),
        }
    }
}

pub fn to_sorted_payload<T: Serialize>(
    payload: &T,
) -> Result<BTreeMap<String, Value>, serde_json::Error> {
    match serde_json::to_value(payload)? {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Ok(BTreeMap::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_payload_uses_alo() {
        let payload = CreateOrderPayload::new("BTC", "100000.00", "0.001", "bid", "client");

        assert_eq!(payload.tif, "ALO");
        assert!(!payload.reduce_only);
    }

    #[test]
    fn cancel_payload_contains_client_order_id() {
        let payload = CancelOrderPayload::new("BTC", "client");

        assert_eq!(payload.symbol, "BTC");
        assert_eq!(payload.client_order_id, "client");
    }

    #[test]
    fn edit_payload_contains_price_amount_and_client_order_id() {
        let payload = EditOrderPayload::new("BTC", "99500", "0.002", "client");

        assert_eq!(payload.symbol, "BTC");
        assert_eq!(payload.price, "99500");
        assert_eq!(payload.amount, "0.002");
        assert_eq!(payload.client_order_id, "client");
    }
}

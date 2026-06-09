use serde_json::{Value, json};
use thiserror::Error;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Error as TungsteniteError,
};

#[derive(Debug, Error)]
pub enum WebSocketError {
    #[error("websocket: {0}")]
    Tungstenite(#[from] TungsteniteError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("connection interrupted")]
    ConnectionInterrupted,
}

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub fn public_subscriptions(symbol: &str, agg_level: i64) -> Vec<Value> {
    vec![
        subscription(json!({"source":"book","symbol":symbol,"agg_level":agg_level})),
        subscription(json!({"source":"trades","symbol":symbol})),
        subscription(json!({"source":"bbo","symbol":symbol})),
    ]
}

pub fn private_subscriptions(account: &str) -> Vec<Value> {
    vec![
        subscription(json!({"source":"account_positions","account":account})),
        subscription(json!({"source":"account_order_updates","account":account})),
        subscription(json!({"source":"account_trades","account":account})),
    ]
}

pub fn ping_payload() -> Value {
    json!({"method":"ping"})
}

pub async fn connect(url: &str) -> Result<WsStream, WebSocketError> {
    let (stream, _) = connect_async(url).await?;
    Ok(stream)
}

fn subscription(params: Value) -> Value {
    json!({"method":"subscribe","params":params})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_stream_subscribes_registered_symbol() {
        let payloads = public_subscriptions("BTC", 1);

        assert_eq!(payloads[0]["params"]["source"], "book");
        assert_eq!(payloads[0]["params"]["symbol"], "BTC");
        assert_eq!(payloads[0]["params"]["agg_level"], 1);
        assert_eq!(payloads[1]["params"]["source"], "trades");
        assert_eq!(payloads[2]["params"]["source"], "bbo");
    }

    #[test]
    fn private_stream_subscribes_account_channels() {
        let payloads = private_subscriptions("acct");

        assert_eq!(payloads[0]["params"]["source"], "account_positions");
        assert_eq!(payloads[1]["params"]["source"], "account_order_updates");
        assert_eq!(payloads[2]["params"]["source"], "account_trades");
        assert_eq!(payloads[2]["params"]["account"], "acct");
    }

    #[test]
    fn ping_payload_matches_pacifica_heartbeat() {
        assert_eq!(ping_payload(), json!({"method":"ping"}));
    }
}

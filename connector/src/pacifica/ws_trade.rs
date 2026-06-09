use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsTradeAction {
    CreateOrder,
    CancelOrder,
    EditOrder,
    CreateMarketOrder,
    CancelAllOrders,
    BatchOrders,
}

impl WsTradeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateOrder => "create_order",
            Self::CancelOrder => "cancel_order",
            Self::EditOrder => "edit_order",
            Self::CreateMarketOrder => "create_market_order",
            Self::CancelAllOrders => "cancel_all_orders",
            Self::BatchOrders => "batch_orders",
        }
    }

    pub fn from_operation_type(operation_type: &str) -> Option<Self> {
        match operation_type {
            "create_order" => Some(Self::CreateOrder),
            "cancel_order" => Some(Self::CancelOrder),
            "edit_order" => Some(Self::EditOrder),
            "create_market_order" => Some(Self::CreateMarketOrder),
            "cancel_all_orders" => Some(Self::CancelAllOrders),
            "batch_orders" => Some(Self::BatchOrders),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CreateMarketOrderPayload {
    pub amount: String,
    pub client_order_id: String,
    pub reduce_only: bool,
    pub side: String,
    pub slippage_percent: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CancelAllOrdersPayload {
    pub all_symbols: bool,
    pub exclude_reduce_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WsTradeResponse {
    pub request_id: String,
    pub command_type: String,
    pub code: i64,
    pub data: Value,
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Error)]
pub enum WsTradeProtocolError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing websocket trade response id")]
    MissingRequestId,
    #[error("missing websocket trade response type")]
    MissingCommandType,
    #[error("websocket trade response id mismatch: expected {expected}, got {actual}")]
    RequestIdMismatch { expected: String, actual: String },
    #[error("websocket trade command failed: code={code:?} error={error:?}")]
    Command {
        code: Option<i64>,
        error: Option<Value>,
    },
}

impl CreateMarketOrderPayload {
    pub fn new(
        symbol: impl Into<String>,
        amount: impl Into<String>,
        side: impl Into<String>,
        slippage_percent: impl Into<String>,
        client_order_id: impl Into<String>,
        reduce_only: bool,
    ) -> Self {
        Self {
            amount: amount.into(),
            client_order_id: client_order_id.into(),
            reduce_only,
            side: side.into(),
            slippage_percent: slippage_percent.into(),
            symbol: symbol.into(),
        }
    }
}

impl CancelAllOrdersPayload {
    pub fn for_symbol(symbol: impl Into<String>, exclude_reduce_only: bool) -> Self {
        Self {
            all_symbols: false,
            exclude_reduce_only,
            symbol: Some(symbol.into()),
        }
    }

    pub fn all_symbols(exclude_reduce_only: bool) -> Self {
        Self {
            all_symbols: true,
            exclude_reduce_only,
            symbol: None,
        }
    }
}

pub fn command_envelope(
    request_id: impl Into<String>,
    action: WsTradeAction,
    signed_body: Value,
) -> Value {
    json!({
        "id": request_id.into(),
        "params": {
            action.as_str(): signed_body
        }
    })
}

pub fn request_id() -> String {
    uuid_v4()
}

pub fn parse_trade_response(
    text: &str,
    expected_request_id: Option<&str>,
) -> Result<WsTradeResponse, WsTradeProtocolError> {
    let frame: RawTradeResponse = serde_json::from_str(text)?;
    if frame.code != Some(200) || frame.error.is_some() {
        return Err(WsTradeProtocolError::Command {
            code: frame.code,
            error: frame.error,
        });
    }
    let request_id = frame.id.ok_or(WsTradeProtocolError::MissingRequestId)?;
    if let Some(expected) = expected_request_id {
        if request_id != expected {
            return Err(WsTradeProtocolError::RequestIdMismatch {
                expected: expected.to_string(),
                actual: request_id,
            });
        }
    }
    let command_type = frame
        .command_type
        .ok_or(WsTradeProtocolError::MissingCommandType)?;
    Ok(WsTradeResponse {
        request_id,
        command_type,
        code: 200,
        data: frame.data.unwrap_or(Value::Null),
        timestamp_ms: frame.timestamp_ms,
    })
}

#[derive(Debug, Deserialize)]
struct RawTradeResponse {
    code: Option<i64>,
    data: Option<Value>,
    error: Option<Value>,
    id: Option<String>,
    #[serde(rename = "t")]
    timestamp_ms: Option<i64>,
    #[serde(rename = "type")]
    command_type: Option<String>,
}

fn uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    let mut rng = rand::rng();
    for byte in &mut bytes {
        *byte = rng.random();
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacifica::{
        order_payload::{CancelOrderPayload, CreateOrderPayload, EditOrderPayload},
        signing::PacificaSigner,
    };
    use serde_json::Value;

    const CREATE_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_create_order.json");
    const CANCEL_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_cancel_order.json");
    const EDIT_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_edit_order.json");

    #[test]
    fn create_order_ws_envelope_matches_docs() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();
        let signed = signed_create_from_fixture(&fixture);
        let request = command_envelope(
            "660065de-8f32-46ad-ba1e-83c93d3e3966",
            WsTradeAction::CreateOrder,
            signed.clone(),
        );

        assert_eq!(request["id"], "660065de-8f32-46ad-ba1e-83c93d3e3966");
        assert_eq!(request["params"]["create_order"], signed);
        assert!(request["params"]["cancel_order"].is_null());
        assert!(
            serde_json::to_string(&request)
                .unwrap()
                .contains("signature")
        );
    }

    #[test]
    fn cancel_order_ws_envelope_matches_docs() {
        let fixture: Value = serde_json::from_str(CANCEL_FIXTURE).unwrap();
        let signed = signed_cancel_from_fixture(&fixture);
        let request = command_envelope(
            "1bb2b72f-f545-4938-8a38-c5cda8823675",
            WsTradeAction::CancelOrder,
            signed.clone(),
        );

        assert_eq!(request["id"], "1bb2b72f-f545-4938-8a38-c5cda8823675");
        assert_eq!(request["params"]["cancel_order"], signed);
        assert!(request["params"]["create_order"].is_null());
    }

    #[test]
    fn edit_order_ws_envelope_matches_docs() {
        let fixture: Value = serde_json::from_str(EDIT_FIXTURE).unwrap();
        let signed = signed_edit_from_fixture(&fixture);
        let request = command_envelope(
            "660065de-8f32-46ad-ba1e-83c93d3e3966",
            WsTradeAction::EditOrder,
            signed.clone(),
        );

        assert_eq!(request["id"], "660065de-8f32-46ad-ba1e-83c93d3e3966");
        assert_eq!(request["params"]["edit_order"], signed);
        assert!(request["params"]["create_order"].is_null());
        assert!(request["params"]["cancel_order"].is_null());
    }

    #[test]
    fn market_and_cancel_all_envelopes_match_docs() {
        let market = CreateMarketOrderPayload::new(
            "BTC",
            "0.001",
            "bid",
            "0.5",
            "79f948fd-7556-4066-a128-083f3ea49322",
            false,
        );
        let market_value = serde_json::to_value(market).unwrap();
        let market_request = command_envelope(
            "660065de-8f32-46ad-ba1e-83c93d3e3966",
            WsTradeAction::CreateMarketOrder,
            market_value,
        );

        assert_eq!(
            market_request["params"]["create_market_order"]["slippage_percent"],
            "0.5"
        );
        assert_eq!(
            market_request["params"]["create_market_order"]["reduce_only"],
            false
        );

        let cancel_all = serde_json::to_value(CancelAllOrdersPayload::all_symbols(false)).unwrap();
        let cancel_all_request = command_envelope(
            "4e9b4edb-b123-4759-9250-d19db61fabcb",
            WsTradeAction::CancelAllOrders,
            cancel_all,
        );

        assert_eq!(
            cancel_all_request["params"]["cancel_all_orders"]["all_symbols"],
            true
        );
        assert_eq!(
            cancel_all_request["params"]["cancel_all_orders"]["exclude_reduce_only"],
            false
        );
        assert!(cancel_all_request["params"]["cancel_all_orders"]["symbol"].is_null());
    }

    #[test]
    fn generated_request_id_is_uuid_v4_and_hides_signature() {
        let id = request_id();

        assert_uuid_v4(&id);
        assert!(!id.contains("signature"));
    }

    #[test]
    fn parse_create_order_ws_success() {
        let text = r#"{"code":200,"data":{"I":"79f948fd-7556-4066-a128-083f3ea49322","i":645953,"s":"BTC"},"id":"660065de-8f32-46ad-ba1e-83c93d3e3966","t":1749223025962,"type":"create_order"}"#;

        let response =
            parse_trade_response(text, Some("660065de-8f32-46ad-ba1e-83c93d3e3966")).unwrap();

        assert_eq!(response.request_id, "660065de-8f32-46ad-ba1e-83c93d3e3966");
        assert_eq!(response.command_type, "create_order");
        assert_eq!(response.code, 200);
        assert_eq!(response.data["i"], 645953);
        assert_eq!(response.timestamp_ms, Some(1749223025962));
    }

    #[test]
    fn parse_cancel_order_ws_success() {
        let text = r#"{"code":200,"data":{"I":"79f948fd-7556-4066-a128-083f3ea49322","i":null,"s":"BTC"},"id":"1bb2b72f-f545-4938-8a38-c5cda8823675","t":1749223343610,"type":"cancel_order"}"#;

        let response =
            parse_trade_response(text, Some("1bb2b72f-f545-4938-8a38-c5cda8823675")).unwrap();

        assert_eq!(response.command_type, "cancel_order");
        assert_eq!(response.data["I"], "79f948fd-7556-4066-a128-083f3ea49322");
        assert!(response.data["i"].is_null());
    }

    #[test]
    fn parse_edit_order_ws_success() {
        let text = r#"{"code":200,"data":{"I":"79f948fd-7556-4066-a128-083f3ea49322","i":645954,"s":"BTC"},"id":"660065de-8f32-46ad-ba1e-83c93d3e3966","t":1749223026150,"type":"edit_order"}"#;

        let response =
            parse_trade_response(text, Some("660065de-8f32-46ad-ba1e-83c93d3e3966")).unwrap();

        assert_eq!(response.command_type, "edit_order");
        assert_eq!(response.data["I"], "79f948fd-7556-4066-a128-083f3ea49322");
        assert_eq!(response.data["i"], 645954);
    }

    #[test]
    fn parse_ws_command_error() {
        let text = r#"{"code":400,"error":"Invalid batch operation parameters","id":"660065de-8f32-46ad-ba1e-83c93d3e3966","type":"batch_orders"}"#;

        let error =
            parse_trade_response(text, Some("660065de-8f32-46ad-ba1e-83c93d3e3966")).unwrap_err();

        assert!(matches!(
            error,
            WsTradeProtocolError::Command {
                code: Some(400),
                ..
            }
        ));
    }

    #[test]
    fn parse_ws_command_rejects_mismatched_id() {
        let text = r#"{"code":200,"data":{"I":"client","s":"BTC"},"id":"actual","t":1749223343610,"type":"cancel_order"}"#;

        let error = parse_trade_response(text, Some("expected")).unwrap_err();

        assert!(matches!(
            error,
            WsTradeProtocolError::RequestIdMismatch { .. }
        ));
    }

    #[test]
    fn ws_trade_fixtures_match_docs() {
        let fixtures: Value =
            serde_json::from_str(include_str!("../../fixtures/pacifica/ws_trade_docs.json"))
                .unwrap();

        for (key, action) in [
            ("create_order_request", WsTradeAction::CreateOrder),
            ("cancel_order_request", WsTradeAction::CancelOrder),
            ("edit_order_request", WsTradeAction::EditOrder),
            (
                "create_market_order_request",
                WsTradeAction::CreateMarketOrder,
            ),
            ("cancel_all_orders_request", WsTradeAction::CancelAllOrders),
        ] {
            let expected = &fixtures[key];
            let action_key = action.as_str();
            let actual = command_envelope(
                expected["id"].as_str().unwrap(),
                action,
                expected["params"][action_key].clone(),
            );
            assert_eq!(&actual, expected);
        }
    }

    fn signed_create_from_fixture(fixture: &Value) -> Value {
        let signer = signer_from_fixture(fixture);
        let payload = CreateOrderPayload::new(
            "BTC",
            "100000.00",
            "0.001",
            "bid",
            "79f948fd-7556-4066-a128-083f3ea49322",
        );
        signer
            .sign_payload(
                "create_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap()
            .into_value()
    }

    fn signed_cancel_from_fixture(fixture: &Value) -> Value {
        let signer = signer_from_fixture(fixture);
        let payload = CancelOrderPayload::new("BTC", "79f948fd-7556-4066-a128-083f3ea49322");
        signer
            .sign_payload(
                "cancel_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap()
            .into_value()
    }

    fn signed_edit_from_fixture(fixture: &Value) -> Value {
        let signer = signer_from_fixture(fixture);
        let payload = EditOrderPayload::new(
            "BTC",
            "99500",
            "0.002",
            "79f948fd-7556-4066-a128-083f3ea49322",
        );
        signer
            .sign_payload(
                "edit_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap()
            .into_value()
    }

    fn signer_from_fixture(fixture: &Value) -> PacificaSigner {
        let key_bytes: Vec<u8> = fixture["private_key_uint8"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as u8)
            .collect();
        PacificaSigner::from_key_bytes(
            &key_bytes,
            fixture["public_key"].as_str().unwrap(),
            fixture["expiry_window"].as_i64().unwrap(),
        )
        .unwrap()
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
}

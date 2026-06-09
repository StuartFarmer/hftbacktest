use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT, LiveEvent,
};
use thiserror::Error;

use crate::pacifica::rest::{
    AccountInfo, BookSnapshot, MarketInfo, PositionSnapshot, RestEnvelope, StartupPosition,
};

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("REST response did not report success")]
    RestFailure,
    #[error("missing market info for symbol: {0}")]
    MissingMarketInfo(String),
    #[error("invalid numeric field {field}: {value}")]
    InvalidNumber { field: &'static str, value: String },
    #[error("book snapshot must include bid and ask level arrays")]
    MissingBookSide,
    #[error("startup found {0} open order(s); fail_on_unknown_orders policy refuses to continue")]
    UnknownOpenOrders(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketSpec {
    pub symbol: String,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_order_size: f64,
}

pub fn market_info_for_symbol(text: &str, symbol: &str) -> Result<MarketSpec, StartupError> {
    let envelope: RestEnvelope<Vec<MarketInfo>> = serde_json::from_str(text)?;
    ensure_success(envelope.payload.success)?;
    let info = envelope
        .payload
        .data
        .into_iter()
        .find(|info| info.symbol == symbol)
        .ok_or_else(|| StartupError::MissingMarketInfo(symbol.to_string()))?;

    Ok(MarketSpec {
        symbol: info.symbol,
        tick_size: parse_f64("tick_size", &info.tick_size)?,
        lot_size: parse_f64("lot_size", &info.lot_size)?,
        min_order_size: parse_f64("min_order_size", &info.min_order_size)?,
    })
}

pub fn book_snapshot_events(text: &str, local_ts_ns: i64) -> Result<Vec<LiveEvent>, StartupError> {
    let envelope: RestEnvelope<BookSnapshot> = serde_json::from_str(text)?;
    ensure_success(envelope.payload.success)?;
    let book = envelope.payload.data;
    if book.levels.len() < 2 {
        return Err(StartupError::MissingBookSide);
    }

    let mut events = Vec::with_capacity(book.levels[0].len() + book.levels[1].len());
    for level in &book.levels[0] {
        events.push(LiveEvent::Feed {
            symbol: book.symbol.clone(),
            event: event(
                LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
                book.timestamp_ms * 1_000_000,
                local_ts_ns,
                parse_f64("bid_px", &level.price)?,
                parse_f64("bid_qty", &level.amount)?,
            ),
        });
    }
    for level in &book.levels[1] {
        events.push(LiveEvent::Feed {
            symbol: book.symbol.clone(),
            event: event(
                LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
                book.timestamp_ms * 1_000_000,
                local_ts_ns,
                parse_f64("ask_px", &level.price)?,
                parse_f64("ask_qty", &level.amount)?,
            ),
        });
    }
    Ok(events)
}

pub fn validate_no_unknown_open_orders(text: &str) -> Result<(), StartupError> {
    let envelope: RestEnvelope<Vec<serde_json::Value>> = serde_json::from_str(text)?;
    ensure_success(envelope.payload.success)?;
    if envelope.payload.data.is_empty() {
        Ok(())
    } else {
        Err(StartupError::UnknownOpenOrders(envelope.payload.data.len()))
    }
}

pub fn position_snapshot_events(text: &str) -> Result<Vec<LiveEvent>, StartupError> {
    parse_positions(text)?
        .into_iter()
        .map(|position| LiveEvent::Position {
            symbol: position.symbol,
            qty: position.qty,
            exch_ts: position.exch_ts,
        })
        .collect::<Vec<_>>()
        .pipe(Ok)
}

pub fn account_info(text: &str) -> Result<AccountInfo, StartupError> {
    let envelope: RestEnvelope<AccountInfo> = serde_json::from_str(text)?;
    ensure_success(envelope.payload.success)?;
    Ok(envelope.payload.data)
}

fn parse_positions(text: &str) -> Result<Vec<StartupPosition>, StartupError> {
    let envelope: RestEnvelope<Vec<PositionSnapshot>> = serde_json::from_str(text)?;
    ensure_success(envelope.payload.success)?;
    envelope
        .payload
        .data
        .into_iter()
        .filter_map(|position| {
            let symbol = position.symbol.or(position.short_symbol)?;
            let amount = position
                .amount
                .or(position.short_amount)
                .unwrap_or_else(|| "0".to_string());
            Some((
                symbol,
                amount,
                position.side.or(position.short_side),
                position
                    .updated_at
                    .or(position.short_updated_at)
                    .or(position.created_at),
            ))
        })
        .map(|(symbol, amount, side, updated_at)| {
            let mut qty = parse_f64("position_amount", &amount)?;
            if matches!(side.as_deref(), Some("ask")) {
                qty = -qty;
            }
            Ok(StartupPosition {
                symbol,
                qty,
                exch_ts: updated_at.unwrap_or_default() * 1_000_000,
            })
        })
        .filter(|result| !matches!(result, Ok(position) if position.qty == 0.0))
        .collect()
}

fn ensure_success(success: Option<bool>) -> Result<(), StartupError> {
    match success {
        Some(false) => Err(StartupError::RestFailure),
        _ => Ok(()),
    }
}

fn event(ev: u64, exch_ts: i64, local_ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev,
        exch_ts,
        local_ts,
        px,
        qty,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    }
}

fn parse_f64(field: &'static str, value: &str) -> Result<f64, StartupError> {
    value.parse().map_err(|_| StartupError::InvalidNumber {
        field,
        value: value.to_string(),
    })
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use hftbacktest::prelude::{LOCAL_ASK_DEPTH_SNAPSHOT_EVENT, LOCAL_BID_DEPTH_SNAPSHOT_EVENT};

    #[test]
    fn startup_fetches_market_info_for_registered_symbol() {
        let market = market_info_for_symbol(
            include_str!("../../fixtures/pacifica/captured/rest_info_sample.json"),
            "BTC",
        )
        .unwrap();

        assert_eq!(market.symbol, "BTC");
        assert_eq!(market.tick_size, 1.0);
        assert_eq!(market.lot_size, 0.00001);
        assert_eq!(market.min_order_size, 10.0);
    }

    #[test]
    fn startup_book_snapshot_maps_depth_events() {
        let events = book_snapshot_events(
            include_str!("../../fixtures/pacifica/captured/rest_book_sample.json"),
            111,
        )
        .unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/pacifica/captured/rest_book_sample.json"
        ))
        .unwrap();
        let data = &fixture["payload"]["data"];
        let first_bid = &data["l"][0][0];
        let first_ask_fixture = &data["l"][1][0];

        assert!(events.len() >= 2);
        let LiveEvent::Feed { symbol, event } = &events[0] else {
            panic!("expected feed event");
        };
        assert_eq!(symbol, "BTC");
        assert!(event.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT));
        assert_eq!(event.exch_ts, data["t"].as_i64().unwrap() * 1_000_000);
        assert_eq!(event.local_ts, 111);
        assert_eq!(
            event.px,
            first_bid["p"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(
            event.qty,
            first_bid["a"].as_str().unwrap().parse::<f64>().unwrap()
        );

        let ask = events
            .iter()
            .find_map(|event| match event {
                LiveEvent::Feed { event, .. } if event.is(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT) => {
                    Some(event)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            ask.px,
            first_ask_fixture["p"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
        );
        assert_eq!(
            ask.qty,
            first_ask_fixture["a"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
        );
    }

    #[test]
    fn startup_rejects_unknown_open_order_by_default() {
        let error = validate_no_unknown_open_orders(
            r#"{"payload":{"success":true,"data":[{"symbol":"BTC","id":1}]}}"#,
        )
        .unwrap_err();

        assert!(matches!(error, StartupError::UnknownOpenOrders(1)));
    }

    #[test]
    fn empty_open_orders_and_positions_are_valid_from_capture() {
        validate_no_unknown_open_orders(include_str!(
            "../../fixtures/pacifica/captured/rest_orders_sample.json"
        ))
        .unwrap();

        let positions = position_snapshot_events(include_str!(
            "../../fixtures/pacifica/captured/rest_positions_sample.json"
        ))
        .unwrap();
        assert!(positions.is_empty());
    }

    #[test]
    fn startup_position_snapshot_maps_symbol_qty() {
        let positions = position_snapshot_events(
            r#"{"payload":{"success":true,"data":[{"s":"BTC","a":"0.003","d":"ask","ut":42}]}}"#,
        )
        .unwrap();

        assert_eq!(positions.len(), 1);
        let LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } = &positions[0]
        else {
            panic!("expected position event");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(*qty, -0.003);
        assert_eq!(*exch_ts, 42_000_000);
    }

    #[test]
    fn startup_position_snapshot_maps_captured_rest_position() {
        let positions = position_snapshot_events(include_str!(
            "../../fixtures/pacifica/captured/rest_positions_after_open_fill_probe.json"
        ))
        .unwrap();

        assert_eq!(positions.len(), 1);
        let LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } = &positions[0]
        else {
            panic!("expected position event");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(*qty, 0.0002);
        assert_eq!(*exch_ts, 1_780_919_672_151_000_000);
    }

    #[test]
    fn account_info_parses_capture() {
        let account = account_info(include_str!(
            "../../fixtures/pacifica/captured/rest_account_sample.json"
        ))
        .unwrap();

        assert_eq!(account.orders_count, 0);
        assert_eq!(account.positions_count, 0);
        assert!(account.updated_at > 0);
    }
}

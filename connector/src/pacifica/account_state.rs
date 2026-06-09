use std::collections::{HashMap, HashSet};

use hftbacktest::prelude::LiveEvent;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccountStateError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid numeric field {field}: {value}")]
    InvalidNumber { field: &'static str, value: String },
}

#[derive(Debug, Default)]
pub struct AccountState {
    positions: HashMap<String, PositionState>,
    initialized_symbols: HashSet<String>,
    last_private_li: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct PositionState {
    qty: f64,
    exch_ts: i64,
}

#[derive(Debug, Deserialize)]
struct AccountPositionsFrame {
    data: Vec<AccountPosition>,
    #[serde(rename = "li")]
    last_increment_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AccountPosition {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "a")]
    amount: String,
    #[serde(rename = "d")]
    side: String,
    #[serde(rename = "t")]
    timestamp_ms: i64,
}

impl AccountState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accept_frame_li(&mut self, li: Option<i64>) -> bool {
        let Some(li) = li else {
            return true;
        };
        if self
            .last_private_li
            .map(|last_li| li < last_li)
            .unwrap_or(false)
        {
            return false;
        }
        self.last_private_li = Some(li);
        true
    }

    pub fn apply_positions_text(
        &mut self,
        text: &str,
        tracked_symbols: &HashSet<String>,
    ) -> Result<Vec<LiveEvent>, AccountStateError> {
        let frame: AccountPositionsFrame = serde_json::from_str(text)?;
        if !self.accept_frame_li(frame.last_increment_id) {
            return Ok(Vec::new());
        }

        let mut present = HashSet::new();
        let mut events = Vec::new();
        for position in frame.data {
            if !tracked_symbols.contains(&position.symbol) {
                continue;
            }
            present.insert(position.symbol.clone());
            let qty = signed_qty(&position)?;
            let exch_ts = position.timestamp_ms * 1_000_000;
            self.positions
                .insert(position.symbol.clone(), PositionState { qty, exch_ts });
            self.initialized_symbols.insert(position.symbol.clone());
            events.push(LiveEvent::Position {
                symbol: position.symbol,
                qty,
                exch_ts,
            });
        }

        let mut tracked = tracked_symbols.iter().collect::<Vec<_>>();
        tracked.sort();
        for symbol in tracked {
            if present.contains(symbol) {
                continue;
            }
            if self.should_emit_flat(symbol) {
                let exch_ts = self.flat_exch_ts(symbol, frame.last_increment_id);
                self.positions
                    .insert(symbol.clone(), PositionState { qty: 0.0, exch_ts });
                self.initialized_symbols.insert(symbol.clone());
                events.push(LiveEvent::Position {
                    symbol: symbol.clone(),
                    qty: 0.0,
                    exch_ts,
                });
            }
        }
        Ok(events)
    }

    pub fn position_qty(&self, symbol: &str) -> Option<f64> {
        self.positions.get(symbol).map(|position| position.qty)
    }

    pub fn position_initialized(&self, symbol: &str) -> bool {
        self.initialized_symbols.contains(symbol)
    }

    fn should_emit_flat(&self, symbol: &str) -> bool {
        !self.position_initialized(symbol)
            || self
                .positions
                .get(symbol)
                .map(|position| position.qty != 0.0)
                .unwrap_or(true)
    }

    fn flat_exch_ts(&self, symbol: &str, frame_li: Option<i64>) -> i64 {
        let previous = self
            .positions
            .get(symbol)
            .map(|position| position.exch_ts)
            .unwrap_or_default();
        if previous > 0 {
            previous.saturating_add(1)
        } else {
            frame_li.unwrap_or_default() * 1_000_000
        }
    }
}

pub fn max_li_from_text(text: &str) -> Result<Option<i64>, AccountStateError> {
    let value: serde_json::Value = serde_json::from_str(text)?;
    let mut max_li = value.get("li").and_then(|li| li.as_i64());
    if let Some(data) = value.get("data").and_then(|data| data.as_array()) {
        for item in data {
            if let Some(li) = item.get("li").and_then(|li| li.as_i64()) {
                max_li = Some(max_li.map(|current| current.max(li)).unwrap_or(li));
            }
        }
    }
    Ok(max_li)
}

pub fn tracked_symbols_from_positions_text(
    text: &str,
) -> Result<HashSet<String>, AccountStateError> {
    let frame: AccountPositionsFrame = serde_json::from_str(text)?;
    Ok(frame
        .data
        .into_iter()
        .map(|position| position.symbol)
        .collect())
}

fn signed_qty(position: &AccountPosition) -> Result<f64, AccountStateError> {
    let mut qty = parse_f64("position_amount", &position.amount)?;
    if position.side == "ask" {
        qty = -qty;
    }
    Ok(qty)
}

fn parse_f64(field: &'static str, value: &str) -> Result<f64, AccountStateError> {
    value.parse().map_err(|_| AccountStateError::InvalidNumber {
        field,
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_state_initializes_from_positions_snapshot() {
        let mut state = AccountState::new();
        let tracked = tracked(["BTC"]);
        let events = state
            .apply_positions_text(open_position(), &tracked)
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(state.position_qty("BTC"), Some(0.0002));
        assert!(state.position_initialized("BTC"));
    }

    #[test]
    fn empty_positions_snapshot_emits_flat_for_tracked_symbol() {
        let mut state = AccountState::new();
        let tracked = tracked(["BTC"]);
        let events = state
            .apply_positions_text(
                r#"{"channel":"account_positions","data":[],"li":42}"#,
                &tracked,
            )
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_position(&events[0], "BTC", 0.0, 42_000_000);
        assert_eq!(state.position_qty("BTC"), Some(0.0));
    }

    #[test]
    fn empty_positions_snapshot_closes_previous_position_with_monotonic_timestamp() {
        let mut state = AccountState::new();
        let tracked = tracked(["BTC"]);
        state
            .apply_positions_text(open_position(), &tracked)
            .unwrap();
        let events = state
            .apply_positions_text(
                r#"{"channel":"account_positions","data":[],"li":336542133}"#,
                &tracked,
            )
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_position(&events[0], "BTC", 0.0, 1_780_919_672_151_000_001);
    }

    #[test]
    fn omitted_symbol_emits_flat_after_initialization() {
        let mut state = AccountState::new();
        let tracked = tracked(["BTC", "ETH"]);
        let events = state
            .apply_positions_text(open_position(), &tracked)
            .unwrap();

        assert_eq!(events.len(), 2);
        assert_position(&events[0], "BTC", 0.0002, 1_780_919_672_151_000_000);
        assert_position(&events[1], "ETH", 0.0, 336_542_010_000_000);
    }

    #[test]
    fn unknown_symbol_is_not_emitted_or_flattened() {
        let mut state = AccountState::new();
        let events = state
            .apply_positions_text(open_position(), &HashSet::new())
            .unwrap();

        assert!(events.is_empty());
        assert!(!state.position_initialized("BTC"));
    }

    #[test]
    fn stale_position_li_is_ignored() {
        let mut state = AccountState::new();
        let tracked = tracked(["BTC"]);
        state
            .apply_positions_text(open_position(), &tracked)
            .unwrap();
        let events = state
            .apply_positions_text(
                r#"{"channel":"account_positions","data":[{"a":"1","d":"ask","s":"BTC","t":1780919673000}],"li":336542009}"#,
                &tracked,
            )
            .unwrap();

        assert!(events.is_empty());
        assert_eq!(state.position_qty("BTC"), Some(0.0002));
    }

    #[test]
    fn equal_li_is_accepted_for_same_exchange_event() {
        let mut state = AccountState::new();

        assert!(state.accept_frame_li(Some(10)));
        assert!(state.accept_frame_li(Some(10)));
        assert!(!state.accept_frame_li(Some(9)));
    }

    #[test]
    fn missing_li_uses_explicit_accept_without_advancing_ordering() {
        let mut state = AccountState::new();

        assert!(state.accept_frame_li(None));
        assert!(state.accept_frame_li(Some(10)));
        assert!(state.accept_frame_li(None));
        assert!(!state.accept_frame_li(Some(9)));
    }

    #[test]
    fn max_li_uses_top_level_and_data_items() {
        let text = r#"{"channel":"account_order_updates","li":2,"data":[{"li":3},{"li":1}]}"#;

        assert_eq!(max_li_from_text(text).unwrap(), Some(3));
    }

    fn open_position() -> &'static str {
        r#"{"channel":"account_positions","data":[{"a":"0.0002","d":"bid","f":"0","i":false,"l":null,"m":"0","p":"63441","s":"BTC","t":1780919672151}],"li":336542010}"#
    }

    fn tracked<const N: usize>(symbols: [&str; N]) -> HashSet<String> {
        symbols.into_iter().map(ToString::to_string).collect()
    }

    fn assert_position(
        event: &LiveEvent,
        expected_symbol: &str,
        expected_qty: f64,
        expected_ts: i64,
    ) {
        let LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } = event
        else {
            panic!("expected position event");
        };
        assert_eq!(symbol, expected_symbol);
        assert_eq!(*qty, expected_qty);
        assert_eq!(*exch_ts, expected_ts);
    }
}

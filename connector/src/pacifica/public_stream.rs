use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    Event, LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_EVENT, LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT, LiveEvent,
};
use thiserror::Error;
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tracing::{info, warn};

use crate::{
    connector::PublishEvent,
    pacifica::{
        msg::{BboFrame, BookFrame, ChannelFrame, TradesFrame},
        websocket::{self, WebSocketError},
    },
};

#[derive(Debug, Error)]
pub enum PublicStreamError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid numeric field {field}: {value}")]
    InvalidNumber { field: &'static str, value: String },
    #[error("book frame must include bid and ask level arrays")]
    MissingBookSide,
    #[error("unsupported trade direction: {0}")]
    UnsupportedTradeDirection(String),
    #[error("unsupported public channel: {0}")]
    UnsupportedChannel(String),
    #[error("websocket: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error("tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedEvent {
    pub symbol: String,
    pub event: Event,
}

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    symbols: Arc<Mutex<HashSet<String>>>,
    agg_level: i64,
    seen_feed_messages: usize,
}

impl PublicStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        symbols: Arc<Mutex<HashSet<String>>>,
        agg_level: i64,
    ) -> Self {
        Self {
            ev_tx,
            symbol_rx,
            symbols,
            agg_level,
            seen_feed_messages: 0,
        }
    }

    pub async fn connect(mut self, url: &str) -> Result<(), PublicStreamError> {
        let stream = websocket::connect(url).await?;
        info!(%url, "Pacifica public websocket connected");
        let (mut write, mut read) = stream.split();
        let mut interval = time::interval(Duration::from_secs(30));
        let initial_symbols = self
            .symbols
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for symbol in initial_symbols {
            self.send_subscriptions(&mut write, &symbol).await?;
        }

        loop {
            select! {
                _ = interval.tick() => {
                    write.send(Message::Text(websocket::ping_payload().to_string().into())).await?;
                }
                symbol = self.symbol_rx.recv() => {
                    match symbol {
                        Ok(symbol) => {
                            self.send_subscriptions(&mut write, &symbol).await?;
                        }
                        Err(RecvError::Closed) => return Ok(()),
                        Err(RecvError::Lagged(_)) => {}
                    }
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

    async fn send_subscriptions<W>(
        &self,
        write: &mut W,
        symbol: &str,
    ) -> Result<(), PublicStreamError>
    where
        W: SinkExt<Message> + Unpin,
        <W as futures_util::Sink<Message>>::Error: Into<tokio_tungstenite::tungstenite::Error>,
    {
        for payload in websocket::public_subscriptions(symbol, self.agg_level) {
            info!(%symbol, payload = %payload, "Pacifica public subscription sent");
            write
                .send(Message::Text(payload.to_string().into()))
                .await
                .map_err(Into::into)?;
        }
        Ok(())
    }

    fn handle_text(&mut self, text: &str) -> Result<(), PublicStreamError> {
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let feeds = map_public_message(text, local_ts)?;
        if !feeds.is_empty() && self.seen_feed_messages < 5 {
            self.seen_feed_messages += 1;
            info!(
                feed_events = feeds.len(),
                "Pacifica public feed message mapped"
            );
        }
        for feed in feeds {
            let _ = self.ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Feed {
                symbol: feed.symbol,
                event: feed.event,
            }));
        }
        Ok(())
    }
}

pub fn map_public_message(
    text: &str,
    local_ts_ns: i64,
) -> Result<Vec<FeedEvent>, PublicStreamError> {
    let frame: ChannelFrame = serde_json::from_str(text)?;
    match frame.channel.as_str() {
        "bbo" => map_bbo_message(text, local_ts_ns),
        "book" => map_book_message(text, local_ts_ns),
        "trades" => map_trades_message(text, local_ts_ns),
        "subscribe" | "pong" => Ok(Vec::new()),
        other => {
            warn!(channel = %other, text, "Pacifica public stream ignored unsupported channel");
            Ok(Vec::new())
        }
    }
}

fn map_bbo_message(text: &str, local_ts_ns: i64) -> Result<Vec<FeedEvent>, PublicStreamError> {
    let frame: BboFrame = serde_json::from_str(text)?;
    let data = frame.data;
    let exch_ts = ms_to_ns(data.timestamp_ms);
    let ival = data.last_increment_id.unwrap_or_default();
    Ok(vec![
        FeedEvent {
            symbol: data.symbol.clone(),
            event: event(
                LOCAL_BID_DEPTH_BBO_EVENT,
                exch_ts,
                local_ts_ns,
                parse_f64("bid_px", &data.bid_px)?,
                parse_f64("bid_qty", &data.bid_qty)?,
                ival,
            ),
        },
        FeedEvent {
            symbol: data.symbol,
            event: event(
                LOCAL_ASK_DEPTH_BBO_EVENT,
                exch_ts,
                local_ts_ns,
                parse_f64("ask_px", &data.ask_px)?,
                parse_f64("ask_qty", &data.ask_qty)?,
                ival,
            ),
        },
    ])
}

fn map_book_message(text: &str, local_ts_ns: i64) -> Result<Vec<FeedEvent>, PublicStreamError> {
    let frame: BookFrame = serde_json::from_str(text)?;
    let data = frame.data;
    if data.levels.len() < 2 {
        return Err(PublicStreamError::MissingBookSide);
    }

    let exch_ts = ms_to_ns(data.timestamp_ms);
    let ival = data.last_increment_id.unwrap_or_default();
    let mut events = Vec::with_capacity(data.levels[0].len() + data.levels[1].len());

    for level in &data.levels[0] {
        events.push(FeedEvent {
            symbol: data.symbol.clone(),
            event: event(
                LOCAL_BID_DEPTH_EVENT,
                exch_ts,
                local_ts_ns,
                parse_f64("bid_px", &level.price)?,
                parse_f64("bid_qty", &level.amount)?,
                ival,
            ),
        });
    }

    for level in &data.levels[1] {
        events.push(FeedEvent {
            symbol: data.symbol.clone(),
            event: event(
                LOCAL_ASK_DEPTH_EVENT,
                exch_ts,
                local_ts_ns,
                parse_f64("ask_px", &level.price)?,
                parse_f64("ask_qty", &level.amount)?,
                ival,
            ),
        });
    }

    Ok(events)
}

fn map_trades_message(text: &str, local_ts_ns: i64) -> Result<Vec<FeedEvent>, PublicStreamError> {
    let frame: TradesFrame = serde_json::from_str(text)?;
    frame
        .data
        .into_iter()
        .map(|trade| {
            let ev = match trade.direction.as_str() {
                "open_long" | "close_short" | "buy" | "bid" => LOCAL_BUY_TRADE_EVENT,
                "open_short" | "close_long" | "sell" | "ask" => LOCAL_SELL_TRADE_EVENT,
                other => {
                    return Err(PublicStreamError::UnsupportedTradeDirection(
                        other.to_string(),
                    ));
                }
            };
            Ok(FeedEvent {
                symbol: trade.symbol,
                event: event(
                    ev,
                    ms_to_ns(trade.timestamp_ms),
                    local_ts_ns,
                    parse_f64("trade_px", &trade.price)?,
                    parse_f64("trade_qty", &trade.amount)?,
                    trade.last_increment_id.unwrap_or_default(),
                ),
            })
        })
        .collect()
}

fn event(ev: u64, exch_ts: i64, local_ts: i64, px: f64, qty: f64, ival: i64) -> Event {
    Event {
        ev,
        exch_ts,
        local_ts,
        px,
        qty,
        order_id: 0,
        ival,
        fval: 0.0,
    }
}

fn ms_to_ns(timestamp_ms: i64) -> i64 {
    timestamp_ms * 1_000_000
}

fn parse_f64(field: &'static str, value: &str) -> Result<f64, PublicStreamError> {
    value.parse().map_err(|_| PublicStreamError::InvalidNumber {
        field,
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hftbacktest::prelude::{
        LOCAL_ASK_DEPTH_BBO_EVENT, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_BBO_EVENT,
        LOCAL_BID_DEPTH_EVENT, LOCAL_BUY_TRADE_EVENT,
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CapturedMessages {
        messages: Vec<serde_json::Value>,
    }

    fn first_fixture_message(path: &str) -> String {
        let captured: CapturedMessages = serde_json::from_str(path).unwrap();
        serde_json::to_string(&captured.messages[0]).unwrap()
    }

    fn value_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> &'a serde_json::Value {
        path.iter().fold(value, |value, key| &value[*key])
    }

    #[test]
    fn bbo_frame_maps_to_depth_bbo_events() {
        let text = first_fixture_message(include_str!(
            "../../fixtures/pacifica/captured/public_bbo_sample.json"
        ));
        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
        let data = &frame["data"];

        let events = map_public_message(&text, 123).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].symbol, "BTC");
        assert!(events[0].event.is(LOCAL_BID_DEPTH_BBO_EVENT));
        assert_eq!(
            events[0].event.exch_ts,
            value_at(data, &["t"]).as_i64().unwrap() * 1_000_000
        );
        assert_eq!(events[0].event.local_ts, 123);
        assert_eq!(
            events[0].event.px,
            data["b"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(
            events[0].event.qty,
            data["B"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(events[0].event.ival, data["li"].as_i64().unwrap());

        assert!(events[1].event.is(LOCAL_ASK_DEPTH_BBO_EVENT));
        assert_eq!(
            events[1].event.px,
            data["a"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(
            events[1].event.qty,
            data["A"].as_str().unwrap().parse::<f64>().unwrap()
        );
    }

    #[test]
    fn depth_levels_map_to_ticks_and_qty() {
        let text = first_fixture_message(include_str!(
            "../../fixtures/pacifica/captured/public_book_sample.json"
        ));
        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
        let data = &frame["data"];
        let first_bid = &data["l"][0][0];
        let first_ask_fixture = &data["l"][1][0];

        let events = map_public_message(&text, 456).unwrap();

        assert!(events.len() >= 2);
        assert!(events[0].event.is(LOCAL_BID_DEPTH_EVENT));
        assert_eq!(
            events[0].event.exch_ts,
            data["t"].as_i64().unwrap() * 1_000_000
        );
        assert_eq!(
            events[0].event.px,
            first_bid["p"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(
            events[0].event.qty,
            first_bid["a"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(events[0].event.ival, data["li"].as_i64().unwrap());

        let first_ask = events
            .iter()
            .find(|event| event.event.is(LOCAL_ASK_DEPTH_EVENT))
            .unwrap();
        assert_eq!(
            first_ask.event.px,
            first_ask_fixture["p"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
        );
        assert_eq!(
            first_ask.event.qty,
            first_ask_fixture["a"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
        );
    }

    #[test]
    fn trade_frame_maps_aggressor_side() {
        let text = first_fixture_message(include_str!(
            "../../fixtures/pacifica/captured/public_trades_sample.json"
        ));
        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
        let trade = &frame["data"][0];

        let events = map_public_message(&text, 789).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "BTC");
        assert!(events[0].event.is(LOCAL_BUY_TRADE_EVENT));
        assert_eq!(
            events[0].event.exch_ts,
            trade["t"].as_i64().unwrap() * 1_000_000
        );
        assert_eq!(events[0].event.local_ts, 789);
        assert_eq!(
            events[0].event.px,
            trade["p"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(
            events[0].event.qty,
            trade["a"].as_str().unwrap().parse::<f64>().unwrap()
        );
        assert_eq!(events[0].event.ival, trade["li"].as_i64().unwrap());
    }
}

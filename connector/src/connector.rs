use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{LiveEvent, Order, Status};
use tokio::sync::mpsc::UnboundedSender;

/// A message will be received by the publisher thread and then published to the bots.
pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
    RegisterInstrument {
        id: u64,
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    },
}

/// Provides a build function for the Connector.
pub trait ConnectorBuilder {
    type Error: Debug;

    fn build_from(config: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Provides an interface for connecting with an exchange or broker for a live bot.
pub trait Connector {
    /// Registers an instrument to be traded through this connector.
    fn register(&mut self, symbol: String);

    /// Returns an [`OrderManager`].
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>>;

    /// Runs the connector, establishing the connection and preparing to exchange information such
    /// as data feed and orders. This method should not block, and any response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn run(&mut self, tx: UnboundedSender<PublishEvent>);

    /// Submits a new order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);

    /// Cancels an open order. This method should not block, and the response should be returned
    /// through the channel using [`PublishEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>);

    /// Modifies an open order. Connectors that do not support live replacement reject the request
    /// immediately so bots receive an order response instead of timing out.
    fn modify(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
        order.req = Status::None;
        order.status = Status::Rejected;
        let _ = tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }));
    }
}

/// Provides `orders` method to get the current working orders.
pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hftbacktest::types::{OrdType, Side, TimeInForce};
    use tokio::sync::mpsc::unbounded_channel;

    struct EmptyOrders;

    impl GetOrders for EmptyOrders {
        fn orders(&self, _symbol: Option<String>) -> Vec<Order> {
            vec![]
        }
    }

    struct UnsupportedModifyConnector {
        order_manager: Arc<Mutex<dyn GetOrders + Send + 'static>>,
    }

    impl UnsupportedModifyConnector {
        fn new() -> Self {
            Self {
                order_manager: Arc::new(Mutex::new(EmptyOrders)),
            }
        }
    }

    impl Connector for UnsupportedModifyConnector {
        fn register(&mut self, _symbol: String) {}

        fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
            self.order_manager.clone()
        }

        fn run(&mut self, _tx: UnboundedSender<PublishEvent>) {}

        fn submit(&self, _symbol: String, _order: Order, _tx: UnboundedSender<PublishEvent>) {}

        fn cancel(&self, _symbol: String, _order: Order, _tx: UnboundedSender<PublishEvent>) {}
    }

    #[test]
    fn default_modify_rejects_request_with_order_response() {
        let connector = UnsupportedModifyConnector::new();
        let (tx, mut rx) = unbounded_channel();
        let mut order = Order::new(
            42,
            100,
            1.0,
            0.001,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTX,
        );
        order.req = Status::Replaced;
        order.status = Status::New;

        connector.modify("BTC".to_string(), order, tx);

        let Some(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order })) = rx.blocking_recv()
        else {
            panic!("expected rejected order response");
        };
        assert_eq!(symbol, "BTC");
        assert_eq!(order.order_id, 42);
        assert_eq!(order.req, Status::None);
        assert_eq!(order.status, Status::Rejected);
    }
}

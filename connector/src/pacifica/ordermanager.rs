use std::collections::HashMap;

use hftbacktest::types::{OrdType, Order, OrderId, Side, Status, TimeInForce};
use rand::Rng;
use thiserror::Error;

use crate::{
    connector::GetOrders,
    pacifica::order_payload::{CancelOrderPayload, CreateOrderPayload, EditOrderPayload},
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OrderManagerError {
    #[error("invalid order argument: {0}")]
    InvalidArg(&'static str),
    #[error("order already exists")]
    OrderAlreadyExists,
    #[error("order not found")]
    OrderNotFound,
    #[error("order update could not be resolved")]
    UnresolvedUpdate,
}

#[derive(Debug, Clone)]
pub struct OrderExt {
    pub symbol: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<i64>,
    pub order: Order,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SymbolOrderId {
    symbol: String,
    order_id: OrderId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingCreateKey {
    symbol: String,
    side: String,
    price: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PacificaOrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PacificaOrderUpdate {
    pub symbol: String,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<i64>,
    pub side: Option<String>,
    pub price: Option<String>,
    pub status: PacificaOrderStatus,
    pub leaves_qty: Option<f64>,
    pub exec_qty: Option<f64>,
    pub exec_price: Option<f64>,
    pub exch_timestamp: i64,
}

#[derive(Debug, Default)]
pub struct OrderManager {
    orders: HashMap<String, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, String>,
    exchange_id_map: HashMap<i64, String>,
    pending_create_map: HashMap<PendingCreateKey, String>,
    pending_edit_map: HashMap<String, Order>,
}

impl OrderManager {
    pub fn new(_prefix: &str) -> Self {
        Self::default()
    }

    pub fn new_order(
        &mut self,
        symbol: &str,
        order: Order,
    ) -> Result<CreateOrderPayload, OrderManagerError> {
        validate_new_order(&order)?;
        let symbol_order_id = SymbolOrderId {
            symbol: symbol.to_string(),
            order_id: order.order_id,
        };
        if self.order_id_map.contains_key(&symbol_order_id) {
            return Err(OrderManagerError::OrderAlreadyExists);
        }

        let client_order_id = self.client_order_id(symbol, order.order_id);
        if self.orders.contains_key(&client_order_id) {
            return Err(OrderManagerError::OrderAlreadyExists);
        }

        let side = pacifica_side(order.side)?;
        let price = order.price().to_string();
        let amount = order.qty.to_string();
        let payload = CreateOrderPayload::new(
            symbol,
            price.clone(),
            amount,
            side.clone(),
            client_order_id.clone(),
        );

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.pending_create_map.insert(
            PendingCreateKey {
                symbol: symbol.to_string(),
                side,
                price,
            },
            client_order_id.clone(),
        );
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol: symbol.to_string(),
                client_order_id,
                exchange_order_id: None,
                order,
            },
        );
        Ok(payload)
    }

    pub fn cancel_order(
        &self,
        symbol: &str,
        order_id: OrderId,
    ) -> Result<CancelOrderPayload, OrderManagerError> {
        let client_order_id = self
            .order_id_map
            .get(&SymbolOrderId {
                symbol: symbol.to_string(),
                order_id,
            })
            .ok_or(OrderManagerError::OrderNotFound)?;
        Ok(CancelOrderPayload::new(symbol, client_order_id.clone()))
    }

    pub fn edit_order(
        &mut self,
        symbol: &str,
        order: Order,
    ) -> Result<EditOrderPayload, OrderManagerError> {
        validate_new_order(&order)?;
        if order.req != Status::Replaced {
            return Err(OrderManagerError::InvalidArg("req"));
        }
        let client_order_id = self
            .order_id_map
            .get(&SymbolOrderId {
                symbol: symbol.to_string(),
                order_id: order.order_id,
            })
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();
        let order_ext = self
            .orders
            .get_mut(&client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?;
        if !order_ext.order.active() {
            return Err(OrderManagerError::InvalidArg("status"));
        }

        let payload = EditOrderPayload::new(
            symbol,
            order.price().to_string(),
            order.qty.to_string(),
            client_order_id,
        );
        self.pending_edit_map
            .insert(payload.client_order_id.clone(), order_ext.order.clone());
        order_ext.order = order;
        Ok(payload)
    }

    pub fn update_edit_ack(
        &mut self,
        client_order_id: &str,
    ) -> Result<OrderExt, OrderManagerError> {
        self.pending_edit_map.remove(client_order_id);
        let mut order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::New;
        self.orders
            .insert(client_order_id.to_string(), order_ext.clone());
        Ok(order_ext)
    }

    pub fn update_edit_fail(
        &mut self,
        client_order_id: &str,
    ) -> Result<OrderExt, OrderManagerError> {
        let previous_order = self
            .pending_edit_map
            .remove(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?;
        let mut order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();
        order_ext.order = previous_order;
        order_ext.order.req = Status::None;
        if !order_ext.order.active() {
            order_ext.order.status = Status::New;
        }
        self.orders
            .insert(client_order_id.to_string(), order_ext.clone());
        Ok(order_ext)
    }

    pub fn update_exchange_order_id(
        &mut self,
        client_order_id: &str,
        exchange_order_id: i64,
    ) -> Result<OrderExt, OrderManagerError> {
        let mut order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();
        self.update_exchange_id_mapping(&mut order_ext, exchange_order_id, client_order_id);
        self.pending_edit_map.remove(client_order_id);
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::New;
        self.orders
            .insert(client_order_id.to_string(), order_ext.clone());
        Ok(order_ext)
    }

    pub fn apply_update(
        &mut self,
        update: PacificaOrderUpdate,
    ) -> Result<OrderExt, OrderManagerError> {
        let client_order_id = self.resolve_client_order_id(&update)?;
        let mut order_ext = self
            .orders
            .get_mut(&client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();

        if let Some(exchange_order_id) = update.exchange_order_id {
            self.update_exchange_id_mapping(&mut order_ext, exchange_order_id, &client_order_id);
        }

        self.pending_edit_map.remove(&client_order_id);
        order_ext.order.req = Status::None;
        order_ext.order.status = hft_status(&update.status);
        order_ext.order.exch_timestamp = update.exch_timestamp;
        if let Some(leaves_qty) = update.leaves_qty {
            order_ext.order.leaves_qty = leaves_qty;
        }
        if let Some(exec_qty) = update.exec_qty {
            if matches!(update.status, PacificaOrderStatus::PartiallyFilled)
                && update.leaves_qty.is_none()
            {
                order_ext.order.exec_qty += exec_qty;
                order_ext.order.leaves_qty = (order_ext.order.leaves_qty - exec_qty).max(0.0);
                if order_ext.order.leaves_qty <= f64::EPSILON {
                    order_ext.order.status = Status::Filled;
                }
            } else {
                order_ext.order.exec_qty = exec_qty;
            }
        }
        if let Some(exec_price) = update.exec_price {
            order_ext.order.exec_price_tick =
                (exec_price / order_ext.order.tick_size).round() as i64;
        }

        if let Some(key) = pending_key_from_update(&update) {
            self.pending_create_map.remove(&key);
        }

        let terminal = order_ext.order.status == Status::Filled
            || matches!(
                update.status,
                PacificaOrderStatus::Filled
                    | PacificaOrderStatus::Cancelled
                    | PacificaOrderStatus::Rejected
            );
        if terminal {
            self.remove_maps(&order_ext);
            self.orders.remove(&client_order_id);
        } else {
            self.orders.insert(client_order_id, order_ext.clone());
        }
        Ok(order_ext)
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &str,
    ) -> Result<OrderExt, OrderManagerError> {
        let mut order_ext = self
            .orders
            .remove(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?;
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::Rejected;
        self.remove_maps(&order_ext);
        Ok(order_ext)
    }

    pub fn update_cancel_fail(
        &mut self,
        client_order_id: &str,
    ) -> Result<OrderExt, OrderManagerError> {
        let mut order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OrderManagerError::OrderNotFound)?
            .clone();
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::New;
        self.orders
            .insert(client_order_id.to_string(), order_ext.clone());
        Ok(order_ext)
    }

    fn resolve_client_order_id(
        &self,
        update: &PacificaOrderUpdate,
    ) -> Result<String, OrderManagerError> {
        if let Some(client_order_id) = &update.client_order_id {
            if self.orders.contains_key(client_order_id) {
                return Ok(client_order_id.clone());
            }
            return Err(OrderManagerError::UnresolvedUpdate);
        }
        if let Some(exchange_order_id) = update.exchange_order_id {
            if let Some(client_order_id) = self.exchange_id_map.get(&exchange_order_id) {
                return Ok(client_order_id.clone());
            }
        }
        if let Some(key) = pending_key_from_update(update) {
            if let Some(client_order_id) = self.pending_create_map.get(&key) {
                return Ok(client_order_id.clone());
            }
        }
        Err(OrderManagerError::UnresolvedUpdate)
    }

    fn remove_maps(&mut self, order_ext: &OrderExt) {
        self.order_id_map.remove(&SymbolOrderId {
            symbol: order_ext.symbol.clone(),
            order_id: order_ext.order.order_id,
        });
        if let Some(exchange_order_id) = order_ext.exchange_order_id {
            self.exchange_id_map.remove(&exchange_order_id);
        }
    }

    fn client_order_id(&self, _symbol: &str, _order_id: OrderId) -> String {
        uuid_v4()
    }

    fn update_exchange_id_mapping(
        &mut self,
        order_ext: &mut OrderExt,
        exchange_order_id: i64,
        client_order_id: &str,
    ) {
        if let Some(previous_exchange_order_id) = order_ext.exchange_order_id
            && previous_exchange_order_id != exchange_order_id
        {
            self.exchange_id_map.remove(&previous_exchange_order_id);
        }
        order_ext.exchange_order_id = Some(exchange_order_id);
        self.exchange_id_map
            .insert(exchange_order_id, client_order_id.to_string());
    }
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

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|order| {
                symbol
                    .as_ref()
                    .map(|symbol| order.symbol == *symbol)
                    .unwrap_or(true)
            })
            .filter(|order| order.order.active())
            .map(|order| order.order.clone())
            .collect()
    }
}

fn validate_new_order(order: &Order) -> Result<(), OrderManagerError> {
    if order.order_type != OrdType::Limit {
        return Err(OrderManagerError::InvalidArg("order_type"));
    }
    if order.time_in_force != TimeInForce::GTX {
        return Err(OrderManagerError::InvalidArg("time_in_force"));
    }
    let _ = pacifica_side(order.side)?;
    Ok(())
}

fn pacifica_side(side: Side) -> Result<String, OrderManagerError> {
    match side {
        Side::Buy => Ok("bid".to_string()),
        Side::Sell => Ok("ask".to_string()),
        Side::None | Side::Unsupported => Err(OrderManagerError::InvalidArg("side")),
    }
}

fn hft_status(status: &PacificaOrderStatus) -> Status {
    match status {
        PacificaOrderStatus::Open => Status::New,
        PacificaOrderStatus::PartiallyFilled => Status::PartiallyFilled,
        PacificaOrderStatus::Filled => Status::Filled,
        PacificaOrderStatus::Cancelled => Status::Canceled,
        PacificaOrderStatus::Rejected => Status::Rejected,
    }
}

fn pending_key_from_update(update: &PacificaOrderUpdate) -> Option<PendingCreateKey> {
    Some(PendingCreateKey {
        symbol: update.symbol.clone(),
        side: update.side.clone()?,
        price: update.price.clone()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_order_maps_hft_id_to_client_id() {
        let mut manager = OrderManager::new("pfhbt-");
        let order = test_order(42, Side::Buy);

        let payload = manager.new_order("BTC", order).unwrap();

        assert_uuid_v4(&payload.client_order_id);
        assert_eq!(payload.side, "bid");
        assert_eq!(payload.tif, "ALO");
    }

    #[test]
    fn duplicate_hft_order_id_is_rejected() {
        let mut manager = OrderManager::new("pfhbt-");

        manager.new_order("BTC", test_order(42, Side::Buy)).unwrap();
        let error = manager
            .new_order("BTC", test_order(42, Side::Buy))
            .unwrap_err();

        assert_eq!(error, OrderManagerError::OrderAlreadyExists);
    }

    #[test]
    fn cancel_order_uses_existing_client_id() {
        let mut manager = OrderManager::new("pfhbt-");

        let created = manager
            .new_order("BTC", test_order(42, Side::Buy))
            .unwrap()
            .client_order_id;
        let payload = manager.cancel_order("BTC", 42).unwrap();

        assert_eq!(payload.client_order_id, created);
    }

    #[test]
    fn edit_preserves_hft_order_and_client_id() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = open_test_order(&mut manager, 42, 123);

        let payload = manager
            .edit_order("BTC", replacement_order(42, Side::Buy, 101, 0.02))
            .unwrap();
        let active = manager.orders(Some("BTC".to_string()));

        assert_eq!(payload.client_order_id, created);
        assert_eq!(payload.price, "101");
        assert_eq!(payload.amount, "0.02");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].order_id, 42);
        assert_eq!(active[0].price_tick, 101);
        assert_eq!(active[0].qty, 0.02);
        assert_eq!(active[0].req, Status::Replaced);
    }

    #[test]
    fn edit_ack_clears_pending_request() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = open_test_order(&mut manager, 42, 123);
        manager
            .edit_order("BTC", replacement_order(42, Side::Buy, 101, 0.02))
            .unwrap();

        let acknowledged = manager.update_edit_ack(&created).unwrap();

        assert_eq!(acknowledged.order.price_tick, 101);
        assert_eq!(acknowledged.order.qty, 0.02);
        assert_eq!(acknowledged.order.status, Status::New);
        assert_eq!(acknowledged.order.req, Status::None);
    }

    #[test]
    fn edit_failure_restores_previous_active_order() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = open_test_order(&mut manager, 42, 123);
        manager
            .edit_order("BTC", replacement_order(42, Side::Buy, 101, 0.02))
            .unwrap();

        let restored = manager.update_edit_fail(&created).unwrap();

        assert_eq!(restored.order.price_tick, 100);
        assert_eq!(restored.order.qty, 0.01);
        assert_eq!(restored.order.status, Status::New);
        assert_eq!(restored.order.req, Status::None);
        assert_eq!(
            manager.cancel_order("BTC", 42).unwrap().client_order_id,
            created
        );
    }

    #[test]
    fn cancel_after_edit_uses_preserved_client_id() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = open_test_order(&mut manager, 42, 123);

        manager
            .edit_order("BTC", replacement_order(42, Side::Buy, 101, 0.02))
            .unwrap();
        let payload = manager.cancel_order("BTC", 42).unwrap();

        assert_eq!(payload.client_order_id, created);
    }

    #[test]
    fn edit_response_updates_exchange_id() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = open_test_order(&mut manager, 42, 123);
        manager
            .edit_order("BTC", replacement_order(42, Side::Buy, 101, 0.02))
            .unwrap();

        let edited = manager.update_exchange_order_id(&created, 456).unwrap();
        let stale_error = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: None,
                exchange_order_id: Some(123),
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: Some(0.01),
                exec_qty: Some(0.01),
                exec_price: Some(101.0),
                exch_timestamp: 20,
            })
            .unwrap_err();
        let update_by_new_exchange_id = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: None,
                exchange_order_id: Some(456),
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: Some(0.01),
                exec_qty: Some(0.01),
                exec_price: Some(101.0),
                exch_timestamp: 30,
            })
            .unwrap();

        assert_eq!(edited.exchange_order_id, Some(456));
        assert_eq!(edited.order.req, Status::None);
        assert_eq!(stale_error, OrderManagerError::UnresolvedUpdate);
        assert_eq!(update_by_new_exchange_id.exchange_order_id, Some(456));
    }

    #[test]
    fn rejected_create_clears_pending_state() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order("BTC", test_order(42, Side::Buy))
            .unwrap()
            .client_order_id;

        let updated = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created),
                exchange_order_id: Some(123),
                side: Some("bid".to_string()),
                price: Some("100".to_string()),
                status: PacificaOrderStatus::Rejected,
                leaves_qty: None,
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 10,
            })
            .unwrap();

        assert_eq!(updated.order.status, Status::Rejected);
        assert!(manager.orders(Some("BTC".to_string())).is_empty());
        assert!(manager.cancel_order("BTC", 42).is_err());
    }

    #[test]
    fn missing_client_id_update_resolves_pending_create() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order("BTC", test_order(42, Side::Buy))
            .unwrap()
            .client_order_id;

        let updated = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: None,
                exchange_order_id: Some(123),
                side: Some("bid".to_string()),
                price: Some("100".to_string()),
                status: PacificaOrderStatus::Open,
                leaves_qty: Some(0.01),
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 10,
            })
            .unwrap();

        assert_eq!(updated.client_order_id, created);
        assert_eq!(updated.exchange_order_id, Some(123));
        assert_eq!(
            manager.cancel_order("BTC", 42).unwrap().client_order_id,
            updated.client_order_id
        );
    }

    #[test]
    fn exchange_id_update_resolves_known_order() {
        let mut manager = OrderManager::new("pfhbt-");
        manager.new_order("BTC", test_order(42, Side::Buy)).unwrap();
        manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: None,
                exchange_order_id: Some(123),
                side: Some("bid".to_string()),
                price: Some("100".to_string()),
                status: PacificaOrderStatus::Open,
                leaves_qty: None,
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 10,
            })
            .unwrap();

        let updated = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: None,
                exchange_order_id: Some(123),
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: Some(0.005),
                exec_qty: Some(0.005),
                exec_price: Some(100.0),
                exch_timestamp: 20,
            })
            .unwrap();

        assert_eq!(updated.order.status, Status::PartiallyFilled);
        assert_eq!(updated.order.exec_qty, 0.005);
        assert_eq!(updated.order.exec_price_tick, 100);
    }

    #[test]
    fn partial_trade_delta_keeps_order_active_until_remaining_quantity_is_filled() {
        let mut manager = OrderManager::new("pfhbt-");
        let created = manager
            .new_order("BTC", test_order(42, Side::Buy))
            .unwrap()
            .client_order_id;

        let first = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created.clone()),
                exchange_order_id: Some(123),
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: None,
                exec_qty: Some(0.004),
                exec_price: Some(100.0),
                exch_timestamp: 10,
            })
            .unwrap();

        assert_eq!(first.order.status, Status::PartiallyFilled);
        assert_eq!(first.order.exec_qty, 0.004);
        assert_eq!(first.order.leaves_qty, 0.006);
        assert_eq!(manager.orders(Some("BTC".to_string())).len(), 1);

        let second = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created),
                exchange_order_id: Some(123),
                side: None,
                price: None,
                status: PacificaOrderStatus::PartiallyFilled,
                leaves_qty: None,
                exec_qty: Some(0.006),
                exec_price: Some(100.0),
                exch_timestamp: 20,
            })
            .unwrap();

        assert_eq!(second.order.status, Status::Filled);
        assert_eq!(second.order.exec_qty, 0.01);
        assert_eq!(second.order.leaves_qty, 0.0);
        assert!(manager.orders(Some("BTC".to_string())).is_empty());
    }

    #[test]
    fn unknown_client_id_does_not_resolve_pending_create_by_price() {
        let mut manager = OrderManager::new("pfhbt-");
        manager.new_order("BTC", test_order(42, Side::Buy)).unwrap();

        let error = manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some("external-order".to_string()),
                exchange_order_id: Some(123),
                side: Some("bid".to_string()),
                price: Some("100".to_string()),
                status: PacificaOrderStatus::Open,
                leaves_qty: None,
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 10,
            })
            .unwrap_err();

        assert_eq!(error, OrderManagerError::UnresolvedUpdate);
    }

    fn test_order(order_id: OrderId, side: Side) -> Order {
        Order::new(
            order_id,
            100,
            1.0,
            0.01,
            side,
            OrdType::Limit,
            TimeInForce::GTX,
        )
    }

    fn replacement_order(order_id: OrderId, side: Side, price_tick: i64, qty: f64) -> Order {
        let mut order = Order::new(
            order_id,
            price_tick,
            1.0,
            qty,
            side,
            OrdType::Limit,
            TimeInForce::GTX,
        );
        order.status = Status::New;
        order.req = Status::Replaced;
        order
    }

    fn open_test_order(
        manager: &mut OrderManager,
        order_id: OrderId,
        exchange_order_id: i64,
    ) -> String {
        let created = manager
            .new_order("BTC", test_order(order_id, Side::Buy))
            .unwrap()
            .client_order_id;
        manager
            .apply_update(PacificaOrderUpdate {
                symbol: "BTC".to_string(),
                client_order_id: Some(created.clone()),
                exchange_order_id: Some(exchange_order_id),
                side: Some("bid".to_string()),
                price: Some("100".to_string()),
                status: PacificaOrderStatus::Open,
                leaves_qty: Some(0.01),
                exec_qty: None,
                exec_price: None,
                exch_timestamp: 10,
            })
            .unwrap();
        created
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

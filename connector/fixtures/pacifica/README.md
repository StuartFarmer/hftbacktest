# Pacifica Connector Fixtures

Fixtures in this directory are canonical inputs for the Rust connector tests.
They should be deterministic, sanitized, and safe to commit. Raw captures that
contain account identifiers, signatures, API keys, or private order details
must be redacted before becoming fixtures.

## Required Fixture Groups

- `signing_create_order.json`: Python SDK canonical signing fixture for a fixed create order.
- `signing_cancel_order.json`: Python SDK canonical signing fixture for a fixed cancel order.
- `public_bbo_*.json`: Pacifica BBO websocket frames.
- `public_depth_*.json`: Pacifica aggregated depth websocket frames.
- `public_trade_*.json`: Pacifica public trade websocket frames.
- `private_order_*.json`: Pacifica account order update frames covering open, partially filled, filled, cancelled, rejected, and missing client id.
- `private_trade_*.json`: Pacifica account trade/fill frames.
- `private_position_*.json`: Pacifica account position update frames.
- `rest_market_info_*.json`: market metadata snapshots.
- `rest_open_orders_*.json`: open-order startup snapshots.
- `rest_positions_*.json`: position startup snapshots.

## Fixture Installation

The installer copies this directory into:

```text
hftbacktest/connector/fixtures/pacifica/
```

Rust tests should read fixture data from the installed hftbacktest connector
crate path so they exercise the same tree that Cargo builds.

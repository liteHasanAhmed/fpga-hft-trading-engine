//! Coinbase Exchange market data.
//!
//! * [`parse`] turns one raw WebSocket text frame into a normalized [`MdMsg`].
//! * [`snapshot`] parses the REST level-3 book.
//! * [`live`] connects, subscribes and fetches snapshots (capture only; replay never
//!   touches the network).
//!
//! Reference: <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels>

pub mod advanced;
pub mod auth;
pub mod live;
pub mod parse;
pub mod snapshot;

pub use parse::{Parsed, Parser};

pub const WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";
pub const WS_DIRECT_URL: &str = "wss://ws-direct.exchange.coinbase.com";
pub const REST_URL: &str = "https://api.exchange.coinbase.com";

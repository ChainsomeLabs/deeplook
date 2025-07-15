use std::{ collections::HashMap, sync::{ Arc, Mutex } };

use crate::orderbook::OrderbookManager;

pub mod cache;
pub mod orderbook;
pub mod error;
pub mod checkpoint;
pub mod handlers;

/// Get orderbook manager by pool_id or pool_name
pub type OrderbookManagerMap = HashMap<String, Arc<Mutex<OrderbookManager>>>;

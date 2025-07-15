use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::orderbook::OrderbookManager;

pub mod checkpoint;
pub mod error;
pub mod handlers;
pub mod orderbook;

/// Get orderbook manager by pool_id or pool_name
pub type OrderbookManagerMap = HashMap<String, Arc<Mutex<OrderbookManager>>>;

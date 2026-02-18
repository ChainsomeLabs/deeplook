use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue, SuiParsedData};

use crate::orderbook::OrderbookManager;

pub mod catch_up;
pub mod checkpoint;
pub mod error;
pub mod handlers;
pub mod historic_orderbook;
pub mod keep_up;
pub mod orderbook;
pub mod replay;
pub mod runtime_store;

/// Get orderbook manager by pool_id or pool_name
pub type OrderbookManagerMap = HashMap<String, Arc<Mutex<OrderbookManager>>>;

pub fn extract_timestamp(input: &Option<SuiParsedData>) -> Result<u64, ()> {
    if let Some(SuiParsedData::MoveObject(obj)) = input {
        if let SuiMoveStruct::WithFields(btree) = &obj.fields {
            let ts_string_maybe = btree.get("timestamp_ms");
            if let Some(ts_string_move) = ts_string_maybe {
                if let SuiMoveValue::String(ts_string) = ts_string_move {
                    return match ts_string.parse::<u64>() {
                        Ok(ts) => Ok(ts),
                        Err(_) => Err(()),
                    };
                }
            }
        }
    }
    Err(())
}

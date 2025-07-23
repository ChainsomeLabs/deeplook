use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use deeplook_cache::Cache;
use deeplook_schema::models::{OrderFill, OrderUpdate, OrderUpdateStatus, Pool};
use diesel::{Connection, PgConnection};
use serde::Serialize;
use sui_sdk::{
    SuiClient,
    rpc_types::{SuiObjectData, SuiObjectDataOptions, SuiObjectResponse},
};
use sui_types::{
    TypeTag,
    base_types::{ObjectID, ObjectRef, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, TransactionKind},
    type_input::TypeInput,
};
use url::Url;

use crate::{
    error::DeepLookOrderbookError, extract_timestamp, historic_orderbook::get_latest_snapshot,
};

pub const DEEPBOOK_PACKAGE_ID: &str =
    "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";
pub const DEEP_TOKEN_PACKAGE_ID: &str =
    "0xdeeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270";
pub const LEVEL2_MODULE: &str = "pool";
pub const LEVEL2_FUNCTION: &str = "get_level2_ticks_from_mid";

#[derive(Debug, Serialize, Clone, Copy)]
pub struct Order {
    pub size: i64,
    pub price: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct Orderbook {
    pub asks: Vec<Order>,
    pub bids: Vec<Order>,
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct OrderReadable {
    pub size: f64,
    pub price: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct OrderbookReadable {
    pub asks: Vec<OrderReadable>,
    pub bids: Vec<OrderReadable>,
}

pub struct OrderbookManager {
    pub pool: Pool,
    pub orderbook: Orderbook,
    pub initial_checkpoint: i64,
    pub sui_client: Arc<SuiClient>,
    cache: Mutex<Cache>,
    price_factor: u64,
    size_factor: u64,
}

impl OrderbookManager {
    pub fn new(
        pool: Pool,
        sui_client: Arc<SuiClient>,
        cache: Mutex<Cache>,
        database_url: Url,
    ) -> Self {
        let base_decimals = pool.base_asset_decimals as u32;
        let quote_decimals = pool.quote_asset_decimals as u32;
        let price_factor = (10u64).pow(9 - base_decimals + quote_decimals);
        let size_factor = (10u64).pow(base_decimals);

        let snapshot = get_latest_snapshot(
            &mut PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB"),
            pool.pool_id.as_str(),
        )
        // TODO: handle this better
        .expect("failed getting snapshot")
        .expect("got None instead of snapshot");

        // TODO: maybe use HashMap instead of Orders?
        let asks_map: HashMap<i64, i64> =
            serde_json::from_value(snapshot.asks).expect("failed parsing asks");
        let bids_map: HashMap<i64, i64> =
            serde_json::from_value(snapshot.bids).expect("failed parsing bids");

        let asks: Vec<Order> = asks_map
            .iter()
            .map(|(&price, &size)| Order {
                price: price,
                size: size,
            })
            .collect();
        let bids: Vec<Order> = bids_map
            .iter()
            .map(|(&price, &size)| Order {
                price: price,
                size: size,
            })
            .collect();

        OrderbookManager {
            pool,
            initial_checkpoint: snapshot.checkpoint,
            sui_client,
            orderbook: Orderbook { asks, bids },
            cache,
            price_factor,
            size_factor,
        }
    }

    pub async fn get_onchain_orderbook(&self) -> Result<(Orderbook, u64), DeepLookOrderbookError> {
        let pool_id = &self.pool.pool_id;
        let pool_name = &self.pool.pool_name;
        let base_asset_id = &self.pool.base_asset_id;
        let quote_asset_id = &self.pool.quote_asset_id;
        let ticks_from_mid = u64::MAX;
        let pool_address = ObjectID::from_hex_literal(pool_id)?;

        let mut ptb = ProgrammableTransactionBuilder::new();

        let pool_object: SuiObjectResponse = self
            .sui_client
            .read_api()
            .get_object_with_options(pool_address, SuiObjectDataOptions::full_content())
            .await?;
        let pool_data: &SuiObjectData =
            pool_object
                .data
                .as_ref()
                .ok_or(DeepLookOrderbookError::InternalError(format!(
                    "Missing data in pool object response for '{}'",
                    pool_name
                )))?;
        let pool_object_ref: ObjectRef = (pool_data.object_id, pool_data.version, pool_data.digest);

        let pool_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(pool_object_ref));
        ptb.input(pool_input)?;

        let input_argument = CallArg::Pure(bcs::to_bytes(&ticks_from_mid).map_err(|_| {
            DeepLookOrderbookError::InternalError("Failed to serialize ticks_from_mid".to_string())
        })?);
        ptb.input(input_argument)?;

        let sui_clock_object_id = ObjectID::from_hex_literal(
            "0x0000000000000000000000000000000000000000000000000000000000000006",
        )?;
        let sui_clock_object: SuiObjectResponse = self
            .sui_client
            .read_api()
            .get_object_with_options(sui_clock_object_id, SuiObjectDataOptions::full_content())
            .await?;
        let clock_data: &SuiObjectData =
            sui_clock_object
                .data
                .as_ref()
                .ok_or(DeepLookOrderbookError::InternalError(
                    "Missing data in clock object response".to_string(),
                ))?;

        let sui_clock_object_ref: ObjectRef =
            (clock_data.object_id, clock_data.version, clock_data.digest);

        let clock_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(sui_clock_object_ref));
        ptb.input(clock_input)?;

        let base_coin_type = parse_type_input(&base_asset_id)?;
        let quote_coin_type = parse_type_input(&quote_asset_id)?;

        let package = ObjectID::from_hex_literal(DEEPBOOK_PACKAGE_ID).map_err(|e| {
            DeepLookOrderbookError::InternalError(format!("Invalid pool ID: {}", e))
        })?;
        let module = LEVEL2_MODULE.to_string();
        let function = LEVEL2_FUNCTION.to_string();

        ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
            package,
            module,
            function,
            type_arguments: vec![base_coin_type, quote_coin_type],
            arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
        })));

        let builder = ptb.finish();
        let tx = TransactionKind::ProgrammableTransaction(builder);

        let result = self
            .sui_client
            .read_api()
            .dev_inspect_transaction_block(SuiAddress::default(), tx, None, None, None)
            .await?;

        let _sui_clock_ts =
            extract_timestamp(&clock_data.content).expect("Failed to parse timestamp");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let mut binding: Vec<sui_sdk::rpc_types::SuiExecutionResult> =
            result.results.ok_or(DeepLookOrderbookError::InternalError(
                "No results from dev_inspect_transaction_block".to_string(),
            ))?;
        let bid_prices = &binding
            .first_mut()
            .ok_or(DeepLookOrderbookError::InternalError(
                "No return values for bid prices".to_string(),
            ))?
            .return_values
            .first_mut()
            .ok_or(DeepLookOrderbookError::InternalError(
                "No bid price data found".to_string(),
            ))?
            .0;
        let bid_parsed_prices: Vec<u64> = bcs::from_bytes(bid_prices).map_err(|_| {
            DeepLookOrderbookError::InternalError("Failed to deserialize bid prices".to_string())
        })?;
        let bid_quantities = &binding
            .first_mut()
            .ok_or(DeepLookOrderbookError::InternalError(
                "No return values for bid quantities".to_string(),
            ))?
            .return_values
            .get(1)
            .ok_or(DeepLookOrderbookError::InternalError(
                "No bid quantity data found".to_string(),
            ))?
            .0;
        let bid_parsed_quantities: Vec<u64> = bcs::from_bytes(bid_quantities).map_err(|_| {
            DeepLookOrderbookError::InternalError(
                "Failed to deserialize bid quantities".to_string(),
            )
        })?;

        let ask_prices = &binding
            .first_mut()
            .ok_or(DeepLookOrderbookError::InternalError(
                "No return values for ask prices".to_string(),
            ))?
            .return_values
            .get(2)
            .ok_or(DeepLookOrderbookError::InternalError(
                "No ask price data found".to_string(),
            ))?
            .0;
        let ask_parsed_prices: Vec<u64> = bcs::from_bytes(ask_prices).map_err(|_| {
            DeepLookOrderbookError::InternalError("Failed to deserialize ask prices".to_string())
        })?;
        let ask_quantities = &binding
            .first_mut()
            .ok_or(DeepLookOrderbookError::InternalError(
                "No return values for ask quantities".to_string(),
            ))?
            .return_values
            .get(3)
            .ok_or(DeepLookOrderbookError::InternalError(
                "No ask quantity data found".to_string(),
            ))?
            .0;
        let ask_parsed_quantities: Vec<u64> = bcs::from_bytes(ask_quantities).map_err(|_| {
            DeepLookOrderbookError::InternalError(
                "Failed to deserialize ask quantities".to_string(),
            )
        })?;

        let bids: Vec<Order> = bid_parsed_prices
            .into_iter()
            .zip(bid_parsed_quantities.into_iter())
            .take(ticks_from_mid as usize)
            .map(|(price, quantity)| Order {
                price: price as i64,
                size: quantity as i64,
            })
            .collect();

        let asks: Vec<Order> = ask_parsed_prices
            .into_iter()
            .zip(ask_parsed_quantities.into_iter())
            .take(ticks_from_mid as usize)
            .map(|(price, quantity)| Order {
                price: price as i64,
                size: quantity as i64,
            })
            .collect();

        Ok((Orderbook { asks, bids }, now))
    }

    fn should_skip_order(&self, checkpoint: i64) -> bool {
        if self.initial_checkpoint >= checkpoint {
            // old event, skip
            // start with initial_checkpoint + 1
            return true;
        }

        return false;
    }

    fn get_readable_orderbook(&self) -> OrderbookReadable {
        let convert = |order: &Order| OrderReadable {
            price: (order.price as f64) / (self.price_factor as f64),
            size: (order.size as f64) / (self.size_factor as f64),
        };

        OrderbookReadable {
            asks: self.orderbook.asks.iter().map(convert).collect(),
            bids: self.orderbook.bids.iter().map(convert).collect(),
        }
    }

    fn is_valid_orderbook(&self) -> bool {
        // All sizes must be non-negative
        let all_sizes_valid = self
            .orderbook
            .asks
            .iter()
            .chain(self.orderbook.bids.iter())
            .all(|o| o.size >= 0);

        // Get lowest ask price
        let min_ask = self.orderbook.asks.iter().map(|o| o.price).min();
        // Get highest bid price
        let max_bid = self.orderbook.bids.iter().map(|o| o.price).max();

        let prices_ok = match (min_ask, max_bid) {
            (Some(ask), Some(bid)) => ask > bid,
            _ => true, // Valid if either side is empty
        };

        all_sizes_valid && prices_ok
    }

    fn remove_zero_orders(&mut self) {
        self.orderbook.asks.retain(|o| o.size != 0);
        self.orderbook.bids.retain(|o| o.size != 0);
    }

    fn update_orderbook(&self) {
        let key = format!("orderbook::{}", self.pool.pool_name);
        let ob = self.get_readable_orderbook();
        if let Ok(mut locked_cache) = self.cache.lock() {
            let _ = locked_cache.set(&key, &ob);
        }
    }

    fn add_order(&mut self, price: i64, size: i64, is_bid: bool) {
        // Decide which side of the book we are working with
        let side = if is_bid {
            &mut self.orderbook.bids
        } else {
            &mut self.orderbook.asks
        };

        // Try to find an existing order at the same price level
        if let Some(order) = side.iter_mut().find(|o| o.price == price) {
            order.size += size;
        } else {
            side.push(Order { price, size });
        }
    }

    fn subtract_order(&mut self, price: i64, size: i64, is_bid: bool) {
        // Decide which side of the book we are working with
        let side = if is_bid {
            &mut self.orderbook.bids
        } else {
            &mut self.orderbook.asks
        };

        // Try to find an existing order at the same price level
        if let Some(order) = side.iter_mut().find(|o| o.price == price) {
            order.size -= size;
        } else {
            side.push(Order { price, size: -size });
        }
    }

    pub fn handle_fill(&mut self, order: OrderFill) {
        if self.should_skip_order(order.checkpoint) {
            return;
        }

        self.subtract_order(order.price, order.base_quantity, !order.taker_is_bid);
    }

    pub fn handle_update(&mut self, order: OrderUpdate) {
        if self.should_skip_order(order.checkpoint) {
            return;
        }
        match order.status {
            OrderUpdateStatus::Placed => {
                self.add_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Canceled => {
                self.subtract_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Expired => {
                self.subtract_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Modified => {
                let to_sub = order.original_quantity - order.quantity;
                self.subtract_order(order.price, to_sub, order.is_bid)
            }
        }
    }

    pub fn handle_batch(&mut self, updates: Vec<OrderUpdate>, fills: Vec<OrderFill>) {
        let updates_count = updates.len();
        let fills_count = fills.len();
        let checkpoint_maybe = match (updates.first(), fills.first()) {
            (Some(update), _) => Some(update.checkpoint),
            (_, Some(fill)) => Some(fill.checkpoint),
            (None, None) => None,
        };
        let mut is_valid_before = true;

        if !self.is_valid_orderbook() {
            is_valid_before = false;
        }

        for update in updates {
            self.handle_update(update);
        }

        for fill in fills {
            self.handle_fill(fill);
        }

        let is_valid_after = self.is_valid_orderbook();

        // orderbook stopped being valid after this update
        if is_valid_before && !is_valid_after {
            println!(
                "Orderbook STOPPED BEING VALID: pool {}, checkpoint {:?}, {} updates, {} fills",
                self.pool.pool_name, checkpoint_maybe, updates_count, fills_count
            );
        }
        // orderbook became valid after this update
        if !is_valid_before && is_valid_after {
            println!(
                "Orderbook BECAME VALID: pool {}, checkpoint {:?}, {} updates, {} fills",
                self.pool.pool_name, checkpoint_maybe, updates_count, fills_count
            );
        }

        self.remove_zero_orders();

        // upload new state to Redis
        self.update_orderbook();
    }
}

pub fn parse_type_input(type_str: &str) -> Result<TypeInput, DeepLookOrderbookError> {
    let type_tag = TypeTag::from_str(type_str)?;
    Ok(TypeInput::from(type_tag))
}

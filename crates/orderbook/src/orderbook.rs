use std::{ str::FromStr, sync::{ Arc, Mutex } };

use deeplook_cache::Cache;
use deeplook_schema::models::{ OrderFill, OrderUpdate, OrderUpdateStatus, Pool };
use serde::Serialize;
use sui_sdk::{ SuiClient, rpc_types::{ SuiObjectData, SuiObjectDataOptions, SuiObjectResponse } };
use sui_types::{
    TypeTag,
    base_types::{ ObjectID, ObjectRef, SuiAddress },
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, TransactionKind },
    type_input::TypeInput,
};

use crate::{ checkpoint::CheckpointDigest, error::DeepLookOrderbookError };

pub const DEEPBOOK_PACKAGE_ID: &str =
    "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";
pub const DEEP_TOKEN_PACKAGE_ID: &str =
    "0xdeeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270";
pub const LEVEL2_MODULE: &str = "pool";
pub const LEVEL2_FUNCTION: &str = "get_level2_ticks_from_mid";

#[derive(Debug, Serialize, Clone, Copy)]
pub struct Order {
    pub size: u64,
    pub price: u64,
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
    pub checkpoint: Option<CheckpointDigest>,
    pub sui_client: Arc<SuiClient>,
    cache: Mutex<Cache>,
    price_factor: u64,
    size_factor: u64,
}

impl OrderbookManager {
    pub fn new(pool: Pool, sui_client: Arc<SuiClient>, cache: Mutex<Cache>) -> Self {
        let base_decimals = pool.base_asset_decimals as u32;
        let quote_decimals = pool.quote_asset_decimals as u32;
        let price_factor = (10u64).pow(9 - base_decimals + quote_decimals);
        let size_factor = (10u64).pow(base_decimals);
        OrderbookManager {
            pool,
            checkpoint: None,
            sui_client,
            orderbook: Orderbook {
                asks: vec![],
                bids: vec![],
            },
            cache,
            price_factor,
            size_factor,
        }
    }

    pub async fn sync(&mut self) -> Result<(), DeepLookOrderbookError> {
        let (ob, check) = self.get_orderbook_with_checkpoint().await?;
        self.checkpoint = Some(check);
        self.orderbook = ob;
        Ok(())
    }

    pub async fn get_onchain_orderbook(&self) -> Result<Orderbook, DeepLookOrderbookError> {
        let pool_id = &self.pool.pool_id;
        let pool_name = &self.pool.pool_name;
        let base_asset_id = &self.pool.base_asset_id;
        let quote_asset_id = &self.pool.quote_asset_id;
        let ticks_from_mid = 100u64;
        let pool_address = ObjectID::from_hex_literal(pool_id)?;

        let mut ptb = ProgrammableTransactionBuilder::new();

        let pool_object: SuiObjectResponse = self.sui_client
            .read_api()
            .get_object_with_options(pool_address, SuiObjectDataOptions::full_content()).await?;
        let pool_data: &SuiObjectData = pool_object.data
            .as_ref()
            .ok_or(
                DeepLookOrderbookError::InternalError(
                    format!("Missing data in pool object response for '{}'", pool_name)
                )
            )?;
        let pool_object_ref: ObjectRef = (pool_data.object_id, pool_data.version, pool_data.digest);

        let pool_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(pool_object_ref));
        ptb.input(pool_input)?;

        let input_argument = CallArg::Pure(
            bcs
                ::to_bytes(&ticks_from_mid)
                .map_err(|_| {
                    DeepLookOrderbookError::InternalError(
                        "Failed to serialize ticks_from_mid".to_string()
                    )
                })?
        );
        ptb.input(input_argument)?;

        let sui_clock_object_id = ObjectID::from_hex_literal(
            "0x0000000000000000000000000000000000000000000000000000000000000006"
        )?;
        let sui_clock_object: SuiObjectResponse = self.sui_client
            .read_api()
            .get_object_with_options(
                sui_clock_object_id,
                SuiObjectDataOptions::full_content()
            ).await?;
        let clock_data: &SuiObjectData = sui_clock_object.data
            .as_ref()
            .ok_or(
                DeepLookOrderbookError::InternalError(
                    "Missing data in clock object response".to_string()
                )
            )?;

        let sui_clock_object_ref: ObjectRef = (
            clock_data.object_id,
            clock_data.version,
            clock_data.digest,
        );

        let clock_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(sui_clock_object_ref));
        ptb.input(clock_input)?;

        let base_coin_type = parse_type_input(&base_asset_id)?;
        let quote_coin_type = parse_type_input(&quote_asset_id)?;

        let package = ObjectID::from_hex_literal(DEEPBOOK_PACKAGE_ID).map_err(|e| {
            DeepLookOrderbookError::InternalError(format!("Invalid pool ID: {}", e))
        })?;
        let module = LEVEL2_MODULE.to_string();
        let function = LEVEL2_FUNCTION.to_string();

        ptb.command(
            Command::MoveCall(
                Box::new(ProgrammableMoveCall {
                    package,
                    module,
                    function,
                    type_arguments: vec![base_coin_type, quote_coin_type],
                    arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
                })
            )
        );

        let builder = ptb.finish();
        let tx = TransactionKind::ProgrammableTransaction(builder);

        let result = self.sui_client
            .read_api()
            .dev_inspect_transaction_block(SuiAddress::default(), tx, None, None, None).await?;

        let mut binding: Vec<sui_sdk::rpc_types::SuiExecutionResult> = result.results.ok_or(
            DeepLookOrderbookError::InternalError(
                "No results from dev_inspect_transaction_block".to_string()
            )
        )?;
        let bid_prices = &binding
            .first_mut()
            .ok_or(
                DeepLookOrderbookError::InternalError("No return values for bid prices".to_string())
            )?
            .return_values.first_mut()
            .ok_or(DeepLookOrderbookError::InternalError("No bid price data found".to_string()))?.0;
        let bid_parsed_prices: Vec<u64> = bcs
            ::from_bytes(bid_prices)
            .map_err(|_| {
                DeepLookOrderbookError::InternalError(
                    "Failed to deserialize bid prices".to_string()
                )
            })?;
        let bid_quantities = &binding
            .first_mut()
            .ok_or(
                DeepLookOrderbookError::InternalError(
                    "No return values for bid quantities".to_string()
                )
            )?
            .return_values.get(1)
            .ok_or(
                DeepLookOrderbookError::InternalError("No bid quantity data found".to_string())
            )?.0;
        let bid_parsed_quantities: Vec<u64> = bcs
            ::from_bytes(bid_quantities)
            .map_err(|_| {
                DeepLookOrderbookError::InternalError(
                    "Failed to deserialize bid quantities".to_string()
                )
            })?;

        let ask_prices = &binding
            .first_mut()
            .ok_or(
                DeepLookOrderbookError::InternalError("No return values for ask prices".to_string())
            )?
            .return_values.get(2)
            .ok_or(DeepLookOrderbookError::InternalError("No ask price data found".to_string()))?.0;
        let ask_parsed_prices: Vec<u64> = bcs
            ::from_bytes(ask_prices)
            .map_err(|_| {
                DeepLookOrderbookError::InternalError(
                    "Failed to deserialize ask prices".to_string()
                )
            })?;
        let ask_quantities = &binding
            .first_mut()
            .ok_or(
                DeepLookOrderbookError::InternalError(
                    "No return values for ask quantities".to_string()
                )
            )?
            .return_values.get(3)
            .ok_or(
                DeepLookOrderbookError::InternalError("No ask quantity data found".to_string())
            )?.0;
        let ask_parsed_quantities: Vec<u64> = bcs
            ::from_bytes(ask_quantities)
            .map_err(|_| {
                DeepLookOrderbookError::InternalError(
                    "Failed to deserialize ask quantities".to_string()
                )
            })?;

        let bids: Vec<Order> = bid_parsed_prices
            .into_iter()
            .zip(bid_parsed_quantities.into_iter())
            .take(ticks_from_mid as usize)
            .map(|(price, quantity)| Order {
                price,
                size: quantity,
            })
            .collect();

        let asks: Vec<Order> = ask_parsed_prices
            .into_iter()
            .zip(ask_parsed_quantities.into_iter())
            .take(ticks_from_mid as usize)
            .map(|(price, quantity)| Order {
                price,
                size: quantity,
            })
            .collect();

        Ok(Orderbook { asks, bids })
    }

    pub async fn get_orderbook_with_checkpoint(
        &self
    ) -> Result<(Orderbook, CheckpointDigest), DeepLookOrderbookError> {
        let before = CheckpointDigest::get_sequence_number(self.sui_client.clone().into()).await?;
        let ob = self.get_onchain_orderbook().await?;
        let after = CheckpointDigest::get_sequence_number(self.sui_client.clone().into()).await?;

        // TODO: find a deterministic way to find out in which checkpoint was this orderbook constructed.
        // Currently, there may be discrepency and there may be relevant txs missing around the origin checkpoint.
        if before == after {
            println!("checkpoint match: {}", before);
        } else {
            println!(
                "checkpoint discrepency, before: {}, after: {}, discrepency: {}",
                before,
                after,
                after - before
            );
        }
        let checkpoint = CheckpointDigest::from_sequence_number(
            self.sui_client.clone().into(),
            after
        ).await?;
        Ok((ob, checkpoint))
    }

    fn should_skip_order(&self, order_checkpoint_num: i64) -> bool {
        let checkpoint = match &self.checkpoint {
            Some(ch) => ch,
            None => {
                return true;
            }
        };

        if checkpoint.sequence_number > (order_checkpoint_num as u64) {
            // old event, skip
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

    fn update_orderbook(&self) {
        let key = format!("orderbook::{}", self.pool.pool_name);
        let ob = self.get_readable_orderbook();
        if let Ok(mut locked_cache) = self.cache.lock() {
            let _ = locked_cache.set(&key, &ob);
        }
    }

    fn add_order(&mut self, price: i64, size: i64, is_bid: bool) {
        let price_u64 = price as u64;
        let size_u64 = size as u64;

        // Decide which side of the book we are working with
        let side = if is_bid { &mut self.orderbook.bids } else { &mut self.orderbook.asks };

        // Try to find an existing order at the same price level
        if let Some(order) = side.iter_mut().find(|o| o.price == price_u64) {
            order.size += size_u64;
        } else {
            side.push(Order {
                price: price_u64,
                size: size_u64,
            });
        }
    }

    fn subtract_order(&mut self, price: i64, size: i64, is_bid: bool) {
        let price_u64 = price as u64;
        let size_u64 = size.unsigned_abs();

        // Decide which side of the book we are working with
        let side = if is_bid { &mut self.orderbook.bids } else { &mut self.orderbook.asks };

        // Try to find an existing order at the same price level
        if let Some(order) = side.iter_mut().find(|o| o.price == price_u64 && o.size >= size_u64) {
            order.size -= size_u64;
            if order.size == 0 {
                side.retain(|o| o.size > 0);
            }
        } else {
            // println!(
            //     "pool {} trying to subtract more than the size of price level\norderbook: {}\nprice: {}, size: {}, is_bid: {:?}",
            //     self.pool.pool_name,
            //     serde_json::to_string(&self.orderbook).unwrap(),
            //     price,
            //     size,
            //     is_bid
            // );
            println!(
                "pool {} trying to subtract more than the size of price level",
                self.pool.pool_name
            );
        }
    }

    pub fn handle_fill(&mut self, order: OrderFill) {
        if self.should_skip_order(order.checkpoint) {
            return;
        }

        self.subtract_order(order.price, order.base_quantity, !order.taker_is_bid);
        self.update_orderbook();
    }

    pub fn handle_update(&mut self, order: OrderUpdate) {
        if self.should_skip_order(order.checkpoint) {
            return;
        }
        match order.status {
            OrderUpdateStatus::Placed => {
                // println!(
                //     "placed, order: {:?}\norderbook: {}",
                //     order,
                //     serde_json::to_string(&self.orderbook).unwrap()
                // );
                self.add_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Canceled => {
                // if order.filled_quantity > 0 {
                //     println!("canceled, order: {:?}", order);
                // }
                // if order.filled_quantity > 0 {
                //     println!(
                //         "canceled, order: {:?}\norderbook: {}",
                //         order,
                //         serde_json::to_string(&self.orderbook).unwrap()
                //     );
                // }

                self.subtract_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Expired => {
                // println!(
                //     "expired, order: {:?}\norderbook: {}",
                //     order,
                //     serde_json::to_string(&self.orderbook).unwrap()
                // );
                self.subtract_order(order.price, order.quantity, order.is_bid);
            }
            OrderUpdateStatus::Modified => {
                println!(
                    "modified, order: {:?}\norderbook: {}",
                    order,
                    serde_json::to_string(&self.orderbook).unwrap()
                );
                // TODO: handle order modified
            }
        }
        // upload new state to Redis
        self.update_orderbook();
    }
}

pub fn parse_type_input(type_str: &str) -> Result<TypeInput, DeepLookOrderbookError> {
    let type_tag = TypeTag::from_str(type_str)?;
    Ok(TypeInput::from(type_tag))
}

use chrono::DateTime;
use serde_json::Value;
use url::Url;
use std::{collections::HashMap, i64, sync::Arc};

use sui_sdk::SuiClientBuilder;
use sui_json_rpc_types::{ SuiObjectData, SuiObjectDataOptions, SuiObjectResponse };
use std::time::{ SystemTime, UNIX_EPOCH };
use sui_types::{
    base_types::{ ObjectID, ObjectRef, SuiAddress },
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, TransactionKind },
};

use crate::server::{
    parse_type_input, DEEPBOOK_PACKAGE_ID, LEVEL2_FUNCTION, LEVEL2_MODULE
};

use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};

use axum::{
    extract::{Path, Query, State},
    Json,
};

use crate::error::DeepBookError;
use crate::server::{AppState, ParameterUtil};
use deepbook_schema::{models::OHLCV1min, schema, view};

// const ALLOWED_OHLCV_INTERVALS: &[&str] = &["1min", "15min", "1h"];

pub async fn get_ohlcv(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    let pool_id = match state.reader.get_pool_id_by_name(&pool_name.as_str()).await {
        Err(_) => {
            return Err(DeepBookError::InternalError(
                "No valid pool names provided".to_string(),
            ));
        }
        Ok(v) => v,
    };
    // Parse start_time and end_time from query parameters (in seconds) and convert to milliseconds
    let end_time = params.end_time();
    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let start_time_date = DateTime::from_timestamp_millis(start_time)
        .unwrap()
        .naive_utc();
    let end_time_date = DateTime::from_timestamp_millis(end_time)
        .unwrap()
        .naive_utc();

    // let aggregation = params
    //     .get("interval")
    //     .map(String::as_str).unwrap();

    // if !ALLOWED_OHLCV_INTERVALS.contains(&aggregation) {
    //     return Err(DeepBookError::InternalError(format!(
    //         "Invalid interval '{}'. Allowed values are: {}",
    //         aggregation,
    //         ALLOWED_OHLCV_INTERVALS.join(", ")
    //     )));
    // }

    let result: Vec<OHLCV1min> = state
        .reader
        .results(
            view::ohlcv_1min::table
                .select(OHLCV1min::as_select())
                .filter(view::ohlcv_1min::bucket.between(start_time_date, end_time_date))
                .filter(view::ohlcv_1min::pool_id.eq(pool_id)),
        )
        .await?;

    Ok(Json(
        result
            .into_iter()
            .map(|ohlc| {
                let vol_b = ohlc.volume_base.to_plain_string();
                let vol_q = ohlc.volume_quote.to_plain_string();
                HashMap::from([
                    (
                        "timestamp".to_string(),
                        Value::from(ohlc.bucket.and_utc().timestamp()),
                    ),
                    ("open".to_string(), Value::from(ohlc.open)),
                    ("high".to_string(), Value::from(ohlc.high)),
                    ("low".to_string(), Value::from(ohlc.low)),
                    ("close".to_string(), Value::from(ohlc.close)),
                    ("volume_base".to_string(), Value::from(vol_b)),
                    ("volume_quote".to_string(), Value::from(vol_q)),
                ])
            })
            .collect(),
    ))
}

pub async fn avg_trade_size(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, Value>>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, base_decimals, quote_decimals) =
        state.reader.get_pool_decimals(&pool_name).await?;

    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time()
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let base_decimals = base_decimals as u8;
    let quote_decimals = quote_decimals as u8;

    let query = schema::order_fills::table
        .filter(schema::order_fills::pool_id.eq(pool_id))
        .filter(schema::order_fills::checkpoint_timestamp_ms.between(start_time, end_time));

    let full_query = query.select(
      (
        schema::order_fills::base_quantity,
        schema::order_fills::quote_quantity,
      )  
    );

    let res: Vec<(i64, i64)> = state.reader.results(full_query).await?;
    let total_trades = res.len();

    if total_trades == 0 {
        return Ok(Json(
            HashMap::from([
                ("avg_base_volume".to_string(), Value::from(0)),
                ("avg_quote_volume".to_string(), Value::from(0)),
            ])
        ))
    }

    let (total_base, total_quote) = res
        .iter()
        .fold(
            (0, 0), 
            |(base_acc, quote_acc), (base_e, quote_e)| (base_acc + base_e, quote_acc + quote_e)
        );

    let mean_base: f64 = (total_base as f64) / (total_trades as f64);
    let mean_quote: f64 = (total_quote as f64) / (total_trades as f64);

    // Conversion factors for decimals
    let base_factor = (10f64).powf(base_decimals.into());
    let quote_factor = (10f64).powf(quote_decimals.into());

    let mean_base_scaled = mean_base / base_factor;
    let mean_quote_scaled = mean_quote / quote_factor;
    
    let data = HashMap::from([
        ("avg_base_volume".to_string(), Value::from(mean_base_scaled)),
        ("avg_quote_volume".to_string(), Value::from(mean_quote_scaled)),
    ]);

    Ok(Json(data))
}

pub async fn avg_duration_between_trades(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, _, _) = state.reader.get_pool_decimals(&pool_name).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time()
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);


    let trades = state
        .reader
        .get_orders(
            pool_name,
            pool_id,
            start_time,
            end_time,
            i64::MAX,
            None,
            None,
        )
        .await?;

    let timestamps: Vec<i64> = trades
        .into_iter()
        .map(
            |(
                _,
                _,
                _,
                _, 
                _, 
                timestamp,
                _,
                _,
                _,
            )| { timestamp },
        )
        .rev()
        .collect();

    let diffs: Vec<i64> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();

    let avg_diff = diffs.iter().sum::<i64>() / diffs.len() as i64;

    let data = Value::from(avg_diff);

    Ok(Json(data))
}

pub async fn get_vwap(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Option<f64>>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, base_decimals, quote_decimals) =
        state.reader.get_pool_decimals(&pool_name).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time()
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let trades = state
        .reader
        .get_orders(
            pool_name,
            pool_id,
            start_time,
            end_time,
            i64::MAX,
            None,
            None,
        )
        .await?;
    
    // Conversion factors for decimals
    let base_factor = (10u64).pow(base_decimals as u32);
    let price_factor = (10u64).pow(
        (9 - base_decimals + quote_decimals) as u32
    );

    let mut total_price_qty: f64 = 0.0;
    let mut total_qty: f64 = 0.0;

    for (_, _, price, base_quantity, _, _, _, _, _) in trades {

        let scaled_price = price as f64 / price_factor as f64;
        let scaled_base_quantity = base_quantity as f64 / base_factor as f64;

        total_price_qty += scaled_price * scaled_base_quantity;
        total_qty += scaled_base_quantity;
    }

    let vwap = if total_qty > 0.0 {
        Some(total_price_qty as f64 / total_qty as f64)
    } else {
        None
    };

    Ok(Json(vwap))
}


pub async fn orderbook_imbalance(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State((state, rpc_url)): State<(Arc<AppState>, Url)>
) -> Result<Json<HashMap<String, Value>>, DeepBookError> {
    let depth = params
        .get("depth")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| {
            DeepBookError::InternalError("Depth must be a non-negative integer".to_string())
        })?
        .map(|depth| if depth == 0 { 200 } else { depth });

    if let Some(depth) = depth {
        if depth == 1 {
            return Err(
                DeepBookError::InternalError(
                    "Depth cannot be 1. Use a value greater than 1 or 0 for the entire orderbook".to_string()
                )
            );
        }
    }

    let level = params
        .get("level")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| {
            DeepBookError::InternalError("Level must be an integer between 1 and 2".to_string())
        })?;

    if let Some(level) = level {
        if !(1..=2).contains(&level) {
            return Err(DeepBookError::InternalError("Level must be 1 or 2".to_string()));
        }
    }

    let ticks_from_mid = match (depth, level) {
        (Some(_), Some(1)) => 1u64, // Depth + Level 1 → Best bid and ask
        (Some(depth), Some(2)) | (Some(depth), None) => depth / 2, // Depth + Level 2 → Use depth
        (None, Some(1)) => 1u64, // Only Level 1 → Best bid and ask
        (None, Some(2)) | (None, None) => 100u64, // Level 2 or default → 100 ticks
        _ => 100u64, // Fallback to default
    };

    // Fetch the pool data from the `pools` table
    let query = schema::pools::table
        .filter(schema::pools::pool_name.eq(pool_name.clone()))
        .select((
            schema::pools::pool_id,
            schema::pools::base_asset_id,
            schema::pools::base_asset_decimals,
            schema::pools::quote_asset_id,
            schema::pools::quote_asset_decimals,
        ));
    let pool_data: (String, String, i16, String, i16) = state.reader.first(query).await?;
    let (pool_id, base_asset_id, base_decimals, quote_asset_id, quote_decimals) = pool_data;
    let base_decimals = base_decimals as u8;
    let quote_decimals = quote_decimals as u8;

    let pool_address = ObjectID::from_hex_literal(&pool_id)?;

    let sui_client = SuiClientBuilder::default().build(rpc_url.as_str()).await?;
    let mut ptb = ProgrammableTransactionBuilder::new();

    let pool_object: SuiObjectResponse = sui_client
        .read_api()
        .get_object_with_options(pool_address, SuiObjectDataOptions::full_content()).await?;
    let pool_data: &SuiObjectData = pool_object.data
        .as_ref()
        .ok_or(
            DeepBookError::InternalError(
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
                DeepBookError::InternalError("Failed to serialize ticks_from_mid".to_string())
            })?
    );
    ptb.input(input_argument)?;

    let sui_clock_object_id = ObjectID::from_hex_literal(
        "0x0000000000000000000000000000000000000000000000000000000000000006"
    )?;
    let sui_clock_object: SuiObjectResponse = sui_client
        .read_api()
        .get_object_with_options(sui_clock_object_id, SuiObjectDataOptions::full_content()).await?;
    let clock_data: &SuiObjectData = sui_clock_object.data
        .as_ref()
        .ok_or(DeepBookError::InternalError("Missing data in clock object response".to_string()))?;

    let sui_clock_object_ref: ObjectRef = (
        clock_data.object_id,
        clock_data.version,
        clock_data.digest,
    );

    let clock_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(sui_clock_object_ref));
    ptb.input(clock_input)?;

    let base_coin_type = parse_type_input(&base_asset_id)?;
    let quote_coin_type = parse_type_input(&quote_asset_id)?;

    let package = ObjectID::from_hex_literal(DEEPBOOK_PACKAGE_ID).map_err(|e|
        DeepBookError::InternalError(format!("Invalid pool ID: {}", e))
    )?;
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

    let result = sui_client
        .read_api()
        .dev_inspect_transaction_block(SuiAddress::default(), tx, None, None, None).await?;

    let mut binding = result.results.ok_or(
        DeepBookError::InternalError("No results from dev_inspect_transaction_block".to_string())
    )?;
    let bid_prices = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError("No return values for bid prices".to_string()))?
        .return_values.first_mut()
        .ok_or(DeepBookError::InternalError("No bid price data found".to_string()))?.0;
    let bid_parsed_prices: Vec<u64> = bcs
        ::from_bytes(bid_prices)
        .map_err(|_| {
            DeepBookError::InternalError("Failed to deserialize bid prices".to_string())
        })?;
    let bid_quantities = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError("No return values for bid quantities".to_string()))?
        .return_values.get(1)
        .ok_or(DeepBookError::InternalError("No bid quantity data found".to_string()))?.0;
    let bid_parsed_quantities: Vec<u64> = bcs
        ::from_bytes(bid_quantities)
        .map_err(|_| {
            DeepBookError::InternalError("Failed to deserialize bid quantities".to_string())
        })?;

    let ask_prices = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError("No return values for ask prices".to_string()))?
        .return_values.get(2)
        .ok_or(DeepBookError::InternalError("No ask price data found".to_string()))?.0;
    let ask_parsed_prices: Vec<u64> = bcs
        ::from_bytes(ask_prices)
        .map_err(|_| {
            DeepBookError::InternalError("Failed to deserialize ask prices".to_string())
        })?;
    let ask_quantities = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError("No return values for ask quantities".to_string()))?
        .return_values.get(3)
        .ok_or(DeepBookError::InternalError("No ask quantity data found".to_string()))?.0;
    let ask_parsed_quantities: Vec<u64> = bcs
        ::from_bytes(ask_quantities)
        .map_err(|_| {
            DeepBookError::InternalError("Failed to deserialize ask quantities".to_string())
        })?;

    let mut result = HashMap::new();

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DeepBookError::InternalError("System time error".to_string()))?
        .as_millis() as i64;
    result.insert("timestamp".to_string(), Value::from(timestamp.to_string()));

    let bids: Vec<Value> = bid_parsed_prices
        .into_iter()
        .zip(bid_parsed_quantities.into_iter())
        .take(ticks_from_mid as usize)
        .map(|(price, quantity)| {
            let price_factor = (10u64).pow((9 - base_decimals + quote_decimals).into());
            let quantity_factor = (10u64).pow(base_decimals.into());
            Value::Array(
                vec![
                    Value::from(((price as f64) / (price_factor as f64)).to_string()),
                    Value::from(((quantity as f64) / (quantity_factor as f64)).to_string())
                ]
            )
        })
        .collect();

    let asks: Vec<Value> = ask_parsed_prices
        .into_iter()
        .zip(ask_parsed_quantities.into_iter())
        .take(ticks_from_mid as usize)
        .map(|(price, quantity)| {
            let price_factor = (10u64).pow((9 - base_decimals + quote_decimals).into());
            let quantity_factor = (10u64).pow(base_decimals.into());
            Value::Array(
                vec![
                    Value::from(((price as f64) / (price_factor as f64)).to_string()),
                    Value::from(((quantity as f64) / (quantity_factor as f64)).to_string())
                ]
            )
        })
        .collect();

    let bid_volume = sum_quantities(&bids);
    let ask_volume = sum_quantities(&asks);
    let obi = if (bid_volume + ask_volume) > 0.0 {
        Some((bid_volume - ask_volume) / (bid_volume + ask_volume))
    } else {
        None
    };
    result.insert("orderbook_imbalance".to_string(), Value::from(obi));

    Ok(Json(result))
}

fn sum_quantities(orderbook_side: &[Value]) -> f64 {
    orderbook_side.iter().filter_map(|entry| {
        if let Value::Array(arr) = entry {
            if arr.len() == 2 {
                arr[1].as_str().and_then(|qty_str| qty_str.parse::<f64>().ok())
            } else {
                None
            }
        } else {
            None
        }
    }).sum()
}
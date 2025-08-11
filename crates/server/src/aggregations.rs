use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashMap, i64, sync::Arc};
use url::Url;

use std::time::{SystemTime, UNIX_EPOCH};
use sui_json_rpc_types::{SuiObjectData, SuiObjectDataOptions, SuiObjectResponse};
use sui_sdk::SuiClientBuilder;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, TransactionKind},
};

use crate::server::{
    naive_datetime_from_millis, parse_type_input, DEEPBOOK_PACKAGE_ID, LEVEL2_FUNCTION,
    LEVEL2_MODULE,
};

use diesel::prelude::*;
use diesel::query_dsl::JoinOnDsl;
use diesel::{
    dsl::{sql, sum},
    sql_query,
    sql_types::{Numeric, Text},
    ExpressionMethods, QueryDsl,
};

use axum::{
    extract::{Path, Query, State},
    Json,
};

use crate::error::DeepBookError;
use crate::server::{AppState, ParameterUtil};
use deeplook_schema::{
    models::{OrderFill24hSummary, OHLCV},
    schema, view,
};

pub async fn get_ohlcv(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    let (pool_id, base_decimals, quote_decimals) =
        state.reader.get_pool_decimals(&pool_name).await?;

    // Parse start_time and end_time from query parameters (in seconds) and convert to milliseconds
    let end_time = params.end_time();
    let start_time = params
        .start_time()
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let start_dt = DateTime::from_timestamp_millis(start_time)
        .unwrap()
        .naive_utc();
    let end_dt = DateTime::from_timestamp_millis(end_time)
        .unwrap()
        .naive_utc();

    // Decide granularity to target <= MAX_POINTS datapoints
    const MAX_POINTS: i64 = 1500;
    let dur = end_dt - start_dt;
    let n_min = dur.num_minutes().max(0);
    let n_15m = (n_min + 14) / 15;
    let n_1h = dur.num_hours().max(0);

    enum Bucket {
        Min1,
        Min15,
        Hour1,
        Hour4,
    }
    let bucket = if n_min <= MAX_POINTS {
        Bucket::Min1
    } else if n_15m <= MAX_POINTS {
        Bucket::Min15
    } else if n_1h <= MAX_POINTS {
        Bucket::Hour1
    } else {
        Bucket::Hour4
    };

    // Query the right cagg; reuse the same OHLCV (Queryable-only) model
    let rows: Vec<OHLCV> = match bucket {
        Bucket::Min1 => {
            state
                .reader
                .results(
                    view::ohlcv_1min::table
                        .select((
                            view::ohlcv_1min::bucket,
                            view::ohlcv_1min::pool_id,
                            view::ohlcv_1min::open,
                            view::ohlcv_1min::high,
                            view::ohlcv_1min::low,
                            view::ohlcv_1min::close,
                            view::ohlcv_1min::volume_base,
                            view::ohlcv_1min::volume_quote,
                        ))
                        .filter(view::ohlcv_1min::pool_id.eq(pool_id.to_string()))
                        .filter(view::ohlcv_1min::bucket.between(start_dt, end_dt)),
                )
                .await?
        }
        Bucket::Min15 => {
            state
                .reader
                .results(
                    view::ohlcv_15min::table
                        .select((
                            view::ohlcv_15min::bucket,
                            view::ohlcv_15min::pool_id,
                            view::ohlcv_15min::open,
                            view::ohlcv_15min::high,
                            view::ohlcv_15min::low,
                            view::ohlcv_15min::close,
                            view::ohlcv_15min::volume_base,
                            view::ohlcv_15min::volume_quote,
                        ))
                        .filter(view::ohlcv_15min::pool_id.eq(pool_id.to_string()))
                        .filter(view::ohlcv_15min::bucket.between(start_dt, end_dt)),
                )
                .await?
        }
        Bucket::Hour1 => {
            state
                .reader
                .results(
                    view::ohlcv_1h::table
                        .select((
                            view::ohlcv_1h::bucket,
                            view::ohlcv_1h::pool_id,
                            view::ohlcv_1h::open,
                            view::ohlcv_1h::high,
                            view::ohlcv_1h::low,
                            view::ohlcv_1h::close,
                            view::ohlcv_1h::volume_base,
                            view::ohlcv_1h::volume_quote,
                        ))
                        .filter(view::ohlcv_1h::pool_id.eq(pool_id.to_string()))
                        .filter(view::ohlcv_1h::bucket.between(start_dt, end_dt)),
                )
                .await?
        }
        Bucket::Hour4 => {
            state
                .reader
                .results(
                    view::ohlcv_4h::table
                        .select((
                            view::ohlcv_4h::bucket,
                            view::ohlcv_4h::pool_id,
                            view::ohlcv_4h::open,
                            view::ohlcv_4h::high,
                            view::ohlcv_4h::low,
                            view::ohlcv_4h::close,
                            view::ohlcv_4h::volume_base,
                            view::ohlcv_4h::volume_quote,
                        ))
                        .filter(view::ohlcv_4h::pool_id.eq(pool_id.to_string()))
                        .filter(view::ohlcv_4h::bucket.between(start_dt, end_dt)),
                )
                .await?
        }
    };

    // Same scaling math as before
    let bd = base_decimals as u8;
    let qd = quote_decimals as u8;
    let base_factor = (10f64).powf(bd.into());
    let quote_factor = (10f64).powf(qd.into());
    let price_factor = (10f64).powf((9i32 - bd as i32 + qd as i32) as f64);

    let out = rows
        .into_iter()
        .map(|ohlc| {
            let vol_b = (ohlc.volume_base / base_factor).to_plain_string();
            let vol_q = (ohlc.volume_quote / quote_factor).to_plain_string();
            let open = ohlc.open as f64 / price_factor;
            let high = ohlc.high as f64 / price_factor;
            let low = ohlc.low as f64 / price_factor;
            let close = ohlc.close as f64 / price_factor;

            HashMap::from([
                (
                    "timestamp".to_string(),
                    Value::from(ohlc.bucket.and_utc().timestamp()),
                ),
                ("open".to_string(), Value::from(open)),
                ("high".to_string(), Value::from(high)),
                ("low".to_string(), Value::from(low)),
                ("close".to_string(), Value::from(close)),
                ("volume_base".to_string(), Value::from(vol_b)),
                ("volume_quote".to_string(), Value::from(vol_q)),
            ])
        })
        .collect();

    Ok(Json(out))
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
        .filter(schema::order_fills::timestamp.between(
            naive_datetime_from_millis(start_time)?,
            naive_datetime_from_millis(end_time)?,
        ));

    let full_query = query.select((
        schema::order_fills::base_quantity,
        schema::order_fills::quote_quantity,
    ));

    let res: Vec<(i64, i64)> = state.reader.results(full_query).await?;
    let total_trades = res.len();

    if total_trades == 0 {
        return Ok(Json(HashMap::from([
            ("avg_base_volume".to_string(), Value::from(0)),
            ("avg_quote_volume".to_string(), Value::from(0)),
        ])));
    }

    let (total_base, total_quote) = res
        .iter()
        .fold((0, 0), |(base_acc, quote_acc), (base_e, quote_e)| {
            (base_acc + base_e, quote_acc + quote_e)
        });

    let mean_base: f64 = (total_base as f64) / (total_trades as f64);
    let mean_quote: f64 = (total_quote as f64) / (total_trades as f64);

    // Conversion factors for decimals
    let base_factor = (10f64).powf(base_decimals.into());
    let quote_factor = (10f64).powf(quote_decimals.into());

    let mean_base_scaled = mean_base / base_factor;
    let mean_quote_scaled = mean_quote / quote_factor;

    let data = HashMap::from([
        ("avg_base_volume".to_string(), Value::from(mean_base_scaled)),
        (
            "avg_quote_volume".to_string(),
            Value::from(mean_quote_scaled),
        ),
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
        .map(|(_, _, _, _, _, timestamp, _, _, _)| timestamp)
        .rev()
        .collect();

    let diffs: Vec<i64> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();

    let avg_diff = diffs.iter().sum::<i64>() / (diffs.len() as i64);

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
    let price_factor = (10u64).pow((9 - base_decimals + quote_decimals) as u32);

    let mut total_price_qty: f64 = 0.0;
    let mut total_qty: f64 = 0.0;

    for (_, _, price, base_quantity, _, _, _, _, _) in trades {
        let scaled_price = (price as f64) / (price_factor as f64);
        let scaled_base_quantity = (base_quantity as f64) / (base_factor as f64);

        total_price_qty += scaled_price * scaled_base_quantity;
        total_qty += scaled_base_quantity;
    }

    let vwap = if total_qty > 0.0 {
        Some((total_price_qty as f64) / (total_qty as f64))
    } else {
        None
    };

    Ok(Json(vwap))
}

pub async fn orderbook_imbalance(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State((state, rpc_url)): State<(Arc<AppState>, Url)>,
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
            return Err(DeepBookError::InternalError(
                "Depth cannot be 1. Use a value greater than 1 or 0 for the entire orderbook"
                    .to_string(),
            ));
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
            return Err(DeepBookError::InternalError(
                "Level must be 1 or 2".to_string(),
            ));
        }
    }

    let ticks_from_mid = match (depth, level) {
        (Some(_), Some(1)) => 1u64, // Depth + Level 1 → Best bid and ask
        (Some(depth), Some(2)) | (Some(depth), None) => depth / 2, // Depth + Level 2 → Use depth
        (None, Some(1)) => 1u64,    // Only Level 1 → Best bid and ask
        (None, Some(2)) | (None, None) => 100u64, // Level 2 or default → 100 ticks
        _ => 100u64,                // Fallback to default
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
        .get_object_with_options(pool_address, SuiObjectDataOptions::full_content())
        .await?;
    let pool_data: &SuiObjectData =
        pool_object
            .data
            .as_ref()
            .ok_or(DeepBookError::InternalError(format!(
                "Missing data in pool object response for '{}'",
                pool_name
            )))?;
    let pool_object_ref: ObjectRef = (pool_data.object_id, pool_data.version, pool_data.digest);

    let pool_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(pool_object_ref));
    ptb.input(pool_input)?;

    let input_argument = CallArg::Pure(bcs::to_bytes(&ticks_from_mid).map_err(|_| {
        DeepBookError::InternalError("Failed to serialize ticks_from_mid".to_string())
    })?);
    ptb.input(input_argument)?;

    let sui_clock_object_id = ObjectID::from_hex_literal(
        "0x0000000000000000000000000000000000000000000000000000000000000006",
    )?;
    let sui_clock_object: SuiObjectResponse = sui_client
        .read_api()
        .get_object_with_options(sui_clock_object_id, SuiObjectDataOptions::full_content())
        .await?;
    let clock_data: &SuiObjectData =
        sui_clock_object
            .data
            .as_ref()
            .ok_or(DeepBookError::InternalError(
                "Missing data in clock object response".to_string(),
            ))?;

    let sui_clock_object_ref: ObjectRef =
        (clock_data.object_id, clock_data.version, clock_data.digest);

    let clock_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(sui_clock_object_ref));
    ptb.input(clock_input)?;

    let base_coin_type = parse_type_input(&base_asset_id)?;
    let quote_coin_type = parse_type_input(&quote_asset_id)?;

    let package = ObjectID::from_hex_literal(DEEPBOOK_PACKAGE_ID)
        .map_err(|e| DeepBookError::InternalError(format!("Invalid pool ID: {}", e)))?;
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

    let result = sui_client
        .read_api()
        .dev_inspect_transaction_block(SuiAddress::default(), tx, None, None, None)
        .await?;

    let mut binding = result.results.ok_or(DeepBookError::InternalError(
        "No results from dev_inspect_transaction_block".to_string(),
    ))?;
    let bid_prices = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No return values for bid prices".to_string(),
        ))?
        .return_values
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No bid price data found".to_string(),
        ))?
        .0;
    let bid_parsed_prices: Vec<u64> = bcs::from_bytes(bid_prices).map_err(|_| {
        DeepBookError::InternalError("Failed to deserialize bid prices".to_string())
    })?;
    let bid_quantities = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No return values for bid quantities".to_string(),
        ))?
        .return_values
        .get(1)
        .ok_or(DeepBookError::InternalError(
            "No bid quantity data found".to_string(),
        ))?
        .0;
    let bid_parsed_quantities: Vec<u64> = bcs::from_bytes(bid_quantities).map_err(|_| {
        DeepBookError::InternalError("Failed to deserialize bid quantities".to_string())
    })?;

    let ask_prices = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No return values for ask prices".to_string(),
        ))?
        .return_values
        .get(2)
        .ok_or(DeepBookError::InternalError(
            "No ask price data found".to_string(),
        ))?
        .0;
    let ask_parsed_prices: Vec<u64> = bcs::from_bytes(ask_prices).map_err(|_| {
        DeepBookError::InternalError("Failed to deserialize ask prices".to_string())
    })?;
    let ask_quantities = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No return values for ask quantities".to_string(),
        ))?
        .return_values
        .get(3)
        .ok_or(DeepBookError::InternalError(
            "No ask quantity data found".to_string(),
        ))?
        .0;
    let ask_parsed_quantities: Vec<u64> = bcs::from_bytes(ask_quantities).map_err(|_| {
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
            Value::Array(vec![
                Value::from(((price as f64) / (price_factor as f64)).to_string()),
                Value::from(((quantity as f64) / (quantity_factor as f64)).to_string()),
            ])
        })
        .collect();

    let asks: Vec<Value> = ask_parsed_prices
        .into_iter()
        .zip(ask_parsed_quantities.into_iter())
        .take(ticks_from_mid as usize)
        .map(|(price, quantity)| {
            let price_factor = (10u64).pow((9 - base_decimals + quote_decimals).into());
            let quantity_factor = (10u64).pow(base_decimals.into());
            Value::Array(vec![
                Value::from(((price as f64) / (price_factor as f64)).to_string()),
                Value::from(((quantity as f64) / (quantity_factor as f64)).to_string()),
            ])
        })
        .collect();

    let bid_volume = sum_quantities(&bids);
    let ask_volume = sum_quantities(&asks);
    let obi = if bid_volume + ask_volume > 0.0 {
        Some((bid_volume - ask_volume) / (bid_volume + ask_volume))
    } else {
        None
    };
    result.insert("orderbook_imbalance".to_string(), Value::from(obi));

    Ok(Json(result))
}

fn sum_quantities(orderbook_side: &[Value]) -> f64 {
    orderbook_side
        .iter()
        .filter_map(|entry| {
            if let Value::Array(arr) = entry {
                if arr.len() == 2 {
                    arr[1]
                        .as_str()
                        .and_then(|qty_str| qty_str.parse::<f64>().ok())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .sum()
}

pub async fn get_order_fill_24h_summary(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    // Load results from the view
    let result: Vec<OrderFill24hSummary> = state
        .reader
        .results(view::order_fill_24h_summary_view::dsl::order_fill_24h_summary_view)
        .await
        .map_err(|e| DeepBookError::InternalError(e.to_string()))?;

    // Format into JSON-compatible HashMaps
    let rows: Vec<HashMap<String, Value>> = result
        .into_iter()
        .map(|row| {
            HashMap::from([
                ("pool_id".to_string(), json!(row.pool_id)),
                ("base_volume_24h".to_string(), json!(row.base_volume_24h)),
                (
                    "trade_count_24h".to_string(),
                    json!(row.trade_count_24h.unwrap_or(0.into())),
                ),
                ("price_open_24h".to_string(), json!(row.price_open_24h)),
                ("price_close_24h".to_string(), json!(row.price_close_24h)),
            ])
        })
        .collect();

    Ok(Json(rows))
}

pub async fn get_volume_last_n_days(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<f64>, DeepBookError> {
    // Lookup pool_id by name

    let (pool_id, base_decimals, _) = state.reader.get_pool_decimals(&pool_name).await?;

    // Parse days from query parameters
    let days = params.days();
    let now = Utc::now().naive_utc();
    let start_time = now - Duration::days(days);
    let result: Option<BigDecimal> = state
        .reader
        .results(
            view::ohlcv_1min::table
                .filter(view::ohlcv_1min::pool_id.eq(pool_id))
                .filter(view::ohlcv_1min::bucket.ge(start_time))
                .select(sum(view::ohlcv_1min::volume_base)),
        )
        .await
        .map(|rows: Vec<Option<BigDecimal>>| rows.into_iter().flatten().next())
        .map_err(|e| DeepBookError::InternalError(e.to_string()))?;

    let base_volume = result.to_decimal_f64(base_decimals as u32).unwrap_or(0.0);

    // Returns 0 if NULL
    Ok(Json(base_volume))
}

#[derive(Debug, Serialize, diesel::QueryableByName)]
pub struct VolumeWindowed {
    #[diesel(sql_type = Numeric)]
    pub volume_1d: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub volume_7d: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub volume_30d: BigDecimal,
}

pub async fn get_volume_multi_window(
    Path(pool_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, f64>>, DeepBookError> {
    // Lookup pool_id by name
    let (pool_id, base_decimals, _) = state.reader.get_pool_decimals(&pool_name).await?;

    // SQL query using FILTER clause for each time window
    let result: Option<VolumeWindowed> = state
        .reader
        .results(
            sql_query(
                r#"
            SELECT
                SUM(volume_base) FILTER (WHERE bucket >= now() - INTERVAL '1 day')  AS volume_1d,
                SUM(volume_base) FILTER (WHERE bucket >= now() - INTERVAL '7 day')  AS volume_7d,
                SUM(volume_base) FILTER (WHERE bucket >= now() - INTERVAL '30 day') AS volume_30d
            FROM ohlcv_1min
            WHERE pool_id = $1
            "#,
            )
            .bind::<Text, _>(pool_id.clone()),
        )
        .await
        .map(|mut rows: Vec<VolumeWindowed>| rows.pop())
        .map_err(|e| DeepBookError::InternalError(e.to_string()))?;

    // Prepare default values if missing
    let summary = result.unwrap_or(VolumeWindowed {
        volume_1d: BigDecimal::from(0),
        volume_7d: BigDecimal::from(0),
        volume_30d: BigDecimal::from(0),
    });

    let base_decimals = base_decimals as u32;

    let mut map = HashMap::new();
    map.insert(
        "1d".to_string(),
        summary
            .volume_1d
            .to_decimal_f64(base_decimals)
            .unwrap_or(0.0),
    );
    map.insert(
        "7d".to_string(),
        summary
            .volume_7d
            .to_decimal_f64(base_decimals)
            .unwrap_or(0.0),
    );
    map.insert(
        "30d".to_string(),
        summary
            .volume_30d
            .to_decimal_f64(base_decimals)
            .unwrap_or(0.0),
    );

    Ok(Json(map))
}

pub async fn get_avg_trade_size_multi_window(
    Path(pool_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, DeepBookError> {
    let (pool_id, base_decimals, _) = state.reader.get_pool_decimals(&pool_name).await?;

    let now = Utc::now().naive_utc();

    let durations = vec![
        ("5min", Duration::minutes(5)),
        ("15min", Duration::minutes(15)),
        ("1h", Duration::hours(1)),
        ("24h", Duration::hours(24)),
    ];

    let mut result_map = serde_json::Map::new();

    for (label, duration) in durations {
        let start_time = now - duration;

        let query = view::ohlcv_1min::table
            .inner_join(
                view::trade_count_1min::table.on(view::ohlcv_1min::bucket
                    .eq(view::trade_count_1min::bucket)
                    .and(view::ohlcv_1min::pool_id.eq(view::trade_count_1min::pool_id))),
            )
            .filter(view::ohlcv_1min::pool_id.eq(pool_id.clone()))
            .filter(view::ohlcv_1min::bucket.ge(start_time))
            .select(sql::<Numeric>(
                "COALESCE(AVG(volume_base / NULLIF(trade_count, 0)), 0)",
            ));

        let rows: Vec<BigDecimal> = state
            .reader
            .results(query)
            .await
            .map_err(|e| DeepBookError::InternalError(e.to_string()))?;

        let avg = rows
            .into_iter()
            .next()
            .to_decimal_f64(base_decimals as u32)
            .unwrap_or(0.0);

        result_map.insert(label.to_string(), json!(avg));
    }

    Ok(Json(serde_json::Value::Object(result_map)))
}

pub trait ToDecimalFloat64 {
    fn to_decimal_f64(self, decimals: u32) -> Option<f64>;
}

impl ToDecimalFloat64 for Option<BigDecimal> {
    fn to_decimal_f64(self, decimals: u32) -> Option<f64> {
        let factor = (10i64).pow(decimals);
        self.map(|x| x / factor).and_then(|x| x.to_f64())
    }
}

impl ToDecimalFloat64 for BigDecimal {
    fn to_decimal_f64(self, decimals: u32) -> Option<f64> {
        let factor = (10i64).pow(decimals);

        (self / factor).to_f64()
    }
}

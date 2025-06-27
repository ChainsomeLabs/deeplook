
use chrono::{ DateTime };
use serde_json::Value;
use std::{collections::HashMap, i64, sync::Arc};

use diesel::{ ExpressionMethods, QueryDsl, SelectableHelper };

use axum::{ extract::{ Path, Query, State }, Json};

use crate::server::{AppState, ParameterUtil};
use crate::error::DeepBookError;
use deepbook_schema::{models::OHLCV1min, view};


const DEFAULT_OHLCV_INTERVAL: &str = "1min";
const ALLOWED_OHLCV_INTERVALS: &[&str] = &["1min", "15min", "1h"];


pub async fn get_ohlcv(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    let pool_id = match state.reader.get_pool_id_by_name(&pool_name.as_str()).await {
        Err(_) => {
            return Err(DeepBookError::InternalError("No valid pool names provided".to_string()));
        }
        Ok(v) => v,
    };
    // Parse start_time and end_time from query parameters (in seconds) and convert to milliseconds
    let end_time = params.end_time();
    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let start_time_date = DateTime::from_timestamp_millis(start_time).unwrap().naive_utc();
    let end_time_date = DateTime::from_timestamp_millis(end_time).unwrap().naive_utc();


    let aggregation = params.get("interval").map(String::as_str).unwrap_or(DEFAULT_OHLCV_INTERVAL);

    if !ALLOWED_OHLCV_INTERVALS.contains(&aggregation) {
        return Err(DeepBookError::InternalError(format!(
            "Invalid interval '{}'. Allowed values are: {}",
            aggregation,
            ALLOWED_OHLCV_INTERVALS.join(", ")
        )));
    }

    let result: Vec<OHLCV1min> = state.reader.results(
        view::ohlcv_1min::table
            .select(OHLCV1min::as_select())
            .filter(view::ohlcv_1min::bucket.between(start_time_date, end_time_date))
            .filter(view::ohlcv_1min::pool_id.eq(pool_id))
    ).await?;

    Ok(
        Json(
            result
                .into_iter()
                .map(|ohlc| {
                    let vol_b = ohlc.volume_base.to_plain_string();
                    let vol_q = ohlc.volume_quote.to_plain_string();
                    HashMap::from([
                        ("timestamp".to_string(), Value::from(ohlc.bucket.and_utc().timestamp())),
                        ("open".to_string(), Value::from(ohlc.open)),
                        ("high".to_string(), Value::from(ohlc.high)),
                        ("low".to_string(), Value::from(ohlc.low)),
                        ("close".to_string(), Value::from(ohlc.close)),
                        ("volume_base".to_string(), Value::from(vol_b)),
                        ("volume_quote".to_string(), Value::from(vol_q)),
                    ])
                })
                .collect()
        )
    )
}

pub async fn avg_trade_size(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>
) -> Result<Json<HashMap<String, Value>>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, base_decimals, quote_decimals) = state.reader.get_pool_decimals(
        &pool_name
    ).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params.start_time().unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    // Parse limit (default to 1 if not provided)
    let limit = params.limit();

    // Parse optional filters for balance managers
    let maker_balance_manager_filter = params.get("maker_balance_manager_id").cloned();
    let taker_balance_manager_filter = params.get("taker_balance_manager_id").cloned();

    let base_decimals = base_decimals as u8;
    let quote_decimals = quote_decimals as u8;

    let trades = state.reader.get_orders(
        pool_name,
        pool_id,
        start_time,
        end_time,
        limit,
        maker_balance_manager_filter,
        taker_balance_manager_filter
    ).await?;

    // Conversion factors for decimals
    let base_factor = (10i64).pow(base_decimals as u32);
    let quote_factor = (10i64).pow(quote_decimals as u32);

    let means = trades
        .into_iter()
        .map(
            |(
                _, 
                _, 
                _, 
                base_quantity,
                quote_quantity,
                _, 
                _, 
                _, 
                _, 
            )| {
                (base_quantity / base_factor, quote_quantity / quote_factor)
            }
        );
    
    let vols_base: Vec<i64> = means.clone().into_iter().map(|(b, _)| b).collect();
    let vols_quote: Vec<i64> = means.into_iter().map(|(_, q)| q).collect();
    

    let avg_base = if vols_base.len() != 0 {
        vols_base.clone().into_iter().sum::<i64>() / (vols_base.len() as i64)
    } else {
        0
    };

    let avg_quote = if vols_quote.len() != 0 {
        vols_quote.clone().into_iter().sum::<i64>() / (vols_quote.len() as i64)
    } else {
        0
    };

    let data = HashMap::from([
        ("avg_base_volume".to_string(), Value::from(avg_base)),
        ("avg_quote_volume".to_string(), Value::from(avg_quote))
    ]);


    Ok(Json(data))
}


pub async fn avg_duration_between_trades(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>
) -> Result<Json<Value>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, _, _) = state.reader.get_pool_decimals(
        &pool_name
    ).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params.start_time().unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    // Parse optional filters for balance managers
    let maker_balance_manager_filter = params.get("maker_balance_manager_id").cloned();
    let taker_balance_manager_filter = params.get("taker_balance_manager_id").cloned();

    let trades = state.reader.get_orders(
        pool_name,
        pool_id,
        start_time,
        end_time,
        i64::MAX,
        maker_balance_manager_filter,
        taker_balance_manager_filter
    ).await?;

    let timestamps: Vec<i64> = trades
        .into_iter()
        .map(
            |(
                _, 
                _, 
                _, 
                _, // base_quantity,
                _, // quote_quantity,
                timestamp, 
                _, 
                _, 
                _, 
            )| {
                timestamp
            }
        ).collect();
    
    let diffs: Vec<i64> = timestamps
        .windows(2)
        .map(|w| w[1] - w[0])
        .collect();

    let avg_diff = diffs.iter().sum::<i64>() / diffs.len() as i64;
    
    let data = Value::from(avg_diff);

    Ok(Json(data))
}


pub async fn get_vwap(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>
) -> Result<Json<Option<i128>>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, _, _) = state.reader.get_pool_decimals(
        &pool_name
    ).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params.start_time().unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    // Parse optional filters for balance managers

    let trades = state.reader.get_orders(
        pool_name,
        pool_id,
        start_time,
        end_time,
        i64::MAX,
        None,
        None
    ).await?;

    let (total_price_qty, total_qty) = trades
        .into_iter()
        .map(
            |(
                _,
                _,
                price,
                base_quantity,
                _,
                _,
                _,
                _,
                _,
            )| (price * base_quantity, base_quantity),
        )
        .fold((0i128, 0i64), |(sum_price_qty, sum_qty), (px_qty, qty)| {
            (sum_price_qty + px_qty as i128, sum_qty + qty)
        });   

    let vwap = if total_qty > 0 {
        Some(total_price_qty / total_qty as i128)
    } else {
        None
    };

    Ok(Json(vwap))
}
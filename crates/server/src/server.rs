// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::error::DeepBookError;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::Method;
use axum::response::IntoResponse;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use deeplook_schema::models::{BalancesSummary, OrderFill, Pool};
use deeplook_schema::*;
use diesel::dsl::count_star;
use diesel::dsl::{max, min};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use futures::{FutureExt, StreamExt};
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, net::SocketAddr};
use sui_pg_db::DbArgs;
use tokio::net::TcpListener;
use tower_http::cors::{AllowMethods, Any, CorsLayer};
use url::Url;

use crate::metrics::middleware::track_metrics;
use crate::metrics::RpcMetrics;
use crate::reader::Reader;
use axum::middleware::from_fn_with_state;
use futures::future::join_all;
use prometheus::Registry;
use std::str::FromStr;
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use sui_json_rpc_types::{SuiObjectData, SuiObjectDataOptions, SuiObjectResponse};
use sui_sdk::SuiClientBuilder;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, TransactionKind},
    type_input::TypeInput,
    TypeTag,
};
use tokio::join;
use tokio_util::sync::CancellationToken;

use crate::aggregations::{
    avg_duration_between_trades, avg_trade_size, get_avg_trade_size_multi_window, get_ohlcv,
    get_order_fill_24h_summary, get_volume_last_n_days, get_volume_multi_window, get_vwap,
    orderbook_imbalance,
};

pub const SUI_MAINNET_URL: &str = "https://fullnode.mainnet.sui.io:443";
pub const GET_POOLS_PATH: &str = "/get_pools";
pub const GET_HISTORICAL_VOLUME_BY_BALANCE_MANAGER_ID_WITH_INTERVAL: &str =
    "/historical_volume_by_balance_manager_id_with_interval/:pool_names/:balance_manager_id";
pub const GET_HISTORICAL_VOLUME_BY_BALANCE_MANAGER_ID: &str =
    "/historical_volume_by_balance_manager_id/:pool_names/:balance_manager_id";
pub const HISTORICAL_VOLUME_PATH: &str = "/historical_volume/:pool_names";
pub const ALL_HISTORICAL_VOLUME_PATH: &str = "/all_historical_volume";
pub const GET_NET_DEPOSITS: &str = "/get_net_deposits/:asset_ids/:timestamp";
pub const TICKER_PATH: &str = "/ticker";
pub const TRADES_PATH: &str = "/trades/:pool_name";
pub const ORDER_UPDATES_PATH: &str = "/order_updates/:pool_name";
pub const TRADE_COUNT_PATH: &str = "/trade_count";
pub const ASSETS_PATH: &str = "/assets";
pub const SUMMARY_PATH: &str = "/summary";
pub const LEVEL2_PATH: &str = "/orderbook/:pool_name";
pub const LEVEL2_MODULE: &str = "pool";
pub const LEVEL2_FUNCTION: &str = "get_level2_ticks_from_mid";
pub const DEEPBOOK_PACKAGE_ID: &str =
    "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";
pub const DEEP_TOKEN_PACKAGE_ID: &str =
    "0xdeeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270";
pub const DEEP_TREASURY_ID: &str =
    "0x032abf8948dda67a271bcc18e776dbbcfb0d58c8d288a700ff0d5521e57a1ffe";
pub const DEEP_SUPPLY_MODULE: &str = "deep";
pub const DEEP_SUPPLY_FUNCTION: &str = "total_supply";
pub const DEEP_SUPPLY_PATH: &str = "/deep_supply";
pub const ORDER_FILLS_PATH: &str = "/order_fills/:pool_name";
pub const WEBSOCKET_ORDERBOOK: &str = "/ws_orderbook/:pool_name";
pub const WEBSOCKET_ORDERBOOK_BESTS: &str = "/ws_orderbook_bests/:pool_name";
pub const WEBSOCKET_ORDERBOOK_SPREAD: &str = "/ws_orderbook_spread/:pool_name";
pub const WEBSOCKET_LATEST_TRADES: &str = "/latest_trades/:pool_name";

// Data Aggregation
pub const OHLCV_PATH: &str = "/ohlcv/:pool_name";
pub const AVG_TRADE_PATH: &str = "/get_avg_trade_size/:pool_name";
pub const AVG_DURATION_BETWEEN_TRADES_PATH: &str = "/get_avg_duration_between_trades/:pool_name";
pub const VWAP: &str = "/get_vwap/:pool_name";
pub const OBI: &str = "/orderbook_imbalance/:pool_name";
pub const FILLS_24H_SUMMARY: &str = "/fills_24h_summary";
pub const VOLUME: &str = "/volume/:pool_name";
pub const VOLUME_MULTI_WINDOW: &str = "/volume_multi_window/:pool_name";
pub const AVERAGE_TRADE_SIZE_MULTI_WINDOW: &str = "/average_trade_multi_window/:pool_name";

#[derive(Clone)]
pub struct AppState {
    pub reader: Reader,
    metrics: Arc<RpcMetrics>,
}

impl AppState {
    pub async fn new(
        database_url: Url,
        args: DbArgs,
        registry: &Registry,
        redis_url: Url,
    ) -> Result<Self, anyhow::Error> {
        let metrics = RpcMetrics::new(registry);
        let reader = Reader::new(database_url, args, metrics.clone(), registry, redis_url).await?;
        Ok(Self { reader, metrics })
    }
    pub(crate) fn metrics(&self) -> &RpcMetrics {
        &self.metrics
    }
}

pub async fn run_server(
    server_port: u16,
    database_url: Url,
    db_arg: DbArgs,
    rpc_url: Url,
    cancellation_token: CancellationToken,
    metrics_address: SocketAddr,
    redis_url: Url,
) -> Result<(), anyhow::Error> {
    let registry = Registry::new_custom(Some("deeplook_api".into()), None)
        .expect("Failed to create Prometheus registry.");

    let metrics = MetricsService::new(
        MetricsArgs { metrics_address },
        registry,
        cancellation_token.clone(),
    );

    let state = AppState::new(database_url, db_arg, metrics.registry(), redis_url).await?;
    let socket_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), server_port);

    println!("🚀 Server started successfully on port {}", server_port);

    let _handle = tokio::spawn(async move {
        let _ = metrics.run().await;
    });

    let listener = TcpListener::bind(socket_address).await?;
    axum::serve(listener, make_router(Arc::new(state), rpc_url))
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
        })
        .await?;

    Ok(())
}
pub(crate) fn make_router(state: Arc<AppState>, rpc_url: Url) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(AllowMethods::list(vec![Method::GET, Method::OPTIONS]))
        .allow_headers(Any)
        .allow_origin(Any);

    let db_routes = Router::new()
        .route("/", get(health_check))
        .route(GET_POOLS_PATH, get(get_pools))
        .route(HISTORICAL_VOLUME_PATH, get(historical_volume))
        .route(ALL_HISTORICAL_VOLUME_PATH, get(all_historical_volume))
        .route(
            GET_HISTORICAL_VOLUME_BY_BALANCE_MANAGER_ID_WITH_INTERVAL,
            get(get_historical_volume_by_balance_manager_id_with_interval),
        )
        .route(
            GET_HISTORICAL_VOLUME_BY_BALANCE_MANAGER_ID,
            get(get_historical_volume_by_balance_manager_id),
        )
        .route(GET_NET_DEPOSITS, get(get_net_deposits))
        .route(TICKER_PATH, get(ticker))
        .route(TRADES_PATH, get(trades))
        .route(TRADE_COUNT_PATH, get(trade_count))
        .route(ORDER_UPDATES_PATH, get(order_updates))
        .route(ASSETS_PATH, get(assets))
        .route(ORDER_FILLS_PATH, get(get_order_fills))
        .with_state(state.clone());

    let rpc_routes = Router::new()
        .route(LEVEL2_PATH, get(orderbook))
        .route(DEEP_SUPPLY_PATH, get(deep_supply))
        .route(SUMMARY_PATH, get(summary))
        .route(OBI, get(orderbook_imbalance))
        .route(WEBSOCKET_ORDERBOOK, get(orderbook_ws))
        .route(WEBSOCKET_ORDERBOOK_BESTS, get(orderbook_bests_ws))
        .route(WEBSOCKET_ORDERBOOK_SPREAD, get(orderbook_spread_ws))
        .route(WEBSOCKET_LATEST_TRADES, get(latest_trades_ws))
        .with_state((state.clone(), rpc_url));

    let aggregation_routes = Router::new()
        .route(OHLCV_PATH, get(get_ohlcv))
        .route(AVG_TRADE_PATH, get(avg_trade_size))
        .route(
            AVG_DURATION_BETWEEN_TRADES_PATH,
            get(avg_duration_between_trades),
        )
        .route(VWAP, get(get_vwap))
        .route(FILLS_24H_SUMMARY, get(get_order_fill_24h_summary))
        .route(VOLUME, get(get_volume_last_n_days))
        .route(VOLUME_MULTI_WINDOW, get(get_volume_multi_window))
        .route(
            AVERAGE_TRADE_SIZE_MULTI_WINDOW,
            get(get_avg_trade_size_multi_window),
        )
        .with_state(state.clone());

    db_routes
        .merge(rpc_routes)
        .merge(aggregation_routes)
        .layer(cors)
        .layer(from_fn_with_state(state, track_metrics))
}

impl axum::response::IntoResponse for DeepBookError {
    // TODO: distinguish client error.
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {:?}", self),
        )
            .into_response()
    }
}

impl<E> From<E> for DeepBookError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::InternalError(err.into().to_string())
    }
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// Get all pools stored in database
async fn get_pools(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Pool>>, DeepBookError> {
    Ok(Json(state.reader.get_pools().await?))
}

async fn historical_volume(
    Path(pool_names): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, u64>>, DeepBookError> {
    // Fetch all pools to map names to IDs
    let pools = state.reader.get_pools().await?;
    let pool_name_to_id = pools
        .into_iter()
        .map(|pool| (pool.pool_name, pool.pool_id))
        .collect::<HashMap<_, _>>();

    // Map provided pool names to pool IDs
    let pool_ids: Vec<String> = pool_names
        .split(',')
        .filter_map(|name| pool_name_to_id.get(name).cloned())
        .collect();

    if pool_ids.is_empty() {
        return Err(DeepBookError::InternalError(
            "No valid pool names provided".to_string(),
        ));
    }

    // Parse start_time and end_time from query parameters (in seconds) and convert to milliseconds
    let end_time = params.end_time();
    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    // Determine whether to query volume in base or quote
    let volume_in_base = params.volume_in_base();

    // Query the database for the historical volume
    let results = state
        .reader
        .get_historical_volume(start_time, end_time, &pool_ids, volume_in_base)
        .await?;

    // Aggregate volume by pool ID and map back to pool names
    let mut volume_by_pool = HashMap::new();
    for (pool_id, volume) in results {
        if let Some(pool_name) = pool_name_to_id
            .iter()
            .find(|(_, id)| **id == pool_id)
            .map(|(name, _)| name)
        {
            *volume_by_pool.entry(pool_name.clone()).or_insert(0) += volume as u64;
        }
    }

    Ok(Json(volume_by_pool))
}

/// Get all historical volume for all pools
async fn all_historical_volume(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, u64>>, DeepBookError> {
    let pools = state.reader.get_pools().await?;

    let pool_names: String = pools
        .into_iter()
        .map(|pool| pool.pool_name)
        .collect::<Vec<String>>()
        .join(",");

    historical_volume(Path(pool_names), Query(params), State(state)).await
}

async fn get_historical_volume_by_balance_manager_id(
    Path((pool_names, balance_manager_id)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, Vec<i64>>>, DeepBookError> {
    let pools = state.reader.get_pools().await?;
    let pool_name_to_id = pools
        .into_iter()
        .map(|pool| (pool.pool_name, pool.pool_id))
        .collect::<HashMap<_, _>>();

    let pool_ids: Vec<String> = pool_names
        .split(',')
        .filter_map(|name| pool_name_to_id.get(name).cloned())
        .collect();

    if pool_ids.is_empty() {
        return Err(DeepBookError::InternalError(
            "No valid pool names provided".to_string(),
        ));
    }

    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let volume_in_base = params.volume_in_base();

    let results = state
        .reader
        .get_order_fill_summary(
            start_time,
            end_time,
            &pool_ids,
            &balance_manager_id,
            volume_in_base,
        )
        .await?;

    let mut volume_by_pool: HashMap<String, Vec<i64>> = HashMap::new();
    for order_fill in results {
        if let Some(pool_name) = pool_name_to_id
            .iter()
            .find(|(_, id)| **id == order_fill.pool_id)
            .map(|(name, _)| name)
        {
            let entry = volume_by_pool
                .entry(pool_name.clone())
                .or_insert(vec![0, 0]);
            if order_fill.maker_balance_manager_id == balance_manager_id {
                entry[0] += order_fill.quantity;
            }
            if order_fill.taker_balance_manager_id == balance_manager_id {
                entry[1] += order_fill.quantity;
            }
        }
    }

    Ok(Json(volume_by_pool))
}

async fn get_historical_volume_by_balance_manager_id_with_interval(
    Path((pool_names, balance_manager_id)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, HashMap<String, Vec<i64>>>>, DeepBookError> {
    let pools = state.reader.get_pools().await?;
    let pool_name_to_id: HashMap<String, String> = pools
        .into_iter()
        .map(|pool| (pool.pool_name, pool.pool_id))
        .collect();

    let pool_ids = pool_names
        .split(',')
        .filter_map(|name| pool_name_to_id.get(name).cloned())
        .collect::<Vec<_>>();

    if pool_ids.is_empty() {
        return Err(DeepBookError::InternalError(
            "No valid pool names provided".to_string(),
        ));
    }

    // Parse interval
    let interval = params
        .get("interval")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(3600); // Default interval: 1 hour

    if interval <= 0 {
        return Err(DeepBookError::InternalError(
            "Interval must be greater than 0".to_string(),
        ));
    }

    let interval_ms = interval * 1000;
    // Parse start_time and end_time
    let end_time = params.end_time();

    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let mut metrics_by_interval: HashMap<String, HashMap<String, Vec<i64>>> = HashMap::new();

    let mut current_start = start_time;
    while current_start + interval_ms <= end_time {
        let current_end = current_start + interval_ms;

        let volume_in_base = params.volume_in_base();

        let results = state
            .reader
            .get_order_fill_summary(
                start_time,
                end_time,
                &pool_ids,
                &balance_manager_id,
                volume_in_base,
            )
            .await?;

        let mut volume_by_pool: HashMap<String, Vec<i64>> = HashMap::new();
        for order_fill in results {
            if let Some(pool_name) = pool_name_to_id
                .iter()
                .find(|(_, id)| **id == order_fill.pool_id)
                .map(|(name, _)| name)
            {
                let entry = volume_by_pool
                    .entry(pool_name.clone())
                    .or_insert(vec![0, 0]);
                if order_fill.maker_balance_manager_id == balance_manager_id {
                    entry[0] += order_fill.quantity;
                }
                if order_fill.taker_balance_manager_id == balance_manager_id {
                    entry[1] += order_fill.quantity;
                }
            }
        }

        metrics_by_interval.insert(
            format!("[{}, {}]", current_start / 1000, current_end / 1000),
            volume_by_pool,
        );

        current_start = current_end;
    }

    Ok(Json(metrics_by_interval))
}

async fn ticker(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, HashMap<String, Value>>>, DeepBookError> {
    // Fetch base and quote historical volumes
    let base_volumes = fetch_historical_volume(&params, true, &state).await?;
    let quote_volumes = fetch_historical_volume(&params, false, &state).await?;

    // Fetch pools data for metadata
    let pools = state.reader.get_pools().await?;
    let pool_map: HashMap<String, &Pool> = pools
        .iter()
        .map(|pool| (pool.pool_id.clone(), pool))
        .collect();

    let end_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DeepBookError::InternalError("System time error".to_string()))?
        .as_millis() as i64;

    // Calculate the start time for 24 hours ago
    let start_time = end_time - 24 * 60 * 60 * 1000;

    // Fetch last prices for all pools in a single query. Only trades in the last 24 hours will count.
    let query = schema::order_fills::table
        .filter(schema::order_fills::checkpoint_timestamp_ms.between(start_time, end_time))
        .select((schema::order_fills::pool_id, schema::order_fills::price))
        .order_by((
            schema::order_fills::pool_id.asc(),
            schema::order_fills::checkpoint_timestamp_ms.desc(),
        ))
        .distinct_on(schema::order_fills::pool_id);

    let last_prices: Vec<(String, i64)> = state.reader.results(query).await?;
    let last_price_map: HashMap<String, i64> = last_prices.into_iter().collect();

    let mut response = HashMap::new();

    for (pool_id, pool) in &pool_map {
        let pool_name = &pool.pool_name;
        let base_volume = base_volumes.get(pool_name).copied().unwrap_or(0);
        let quote_volume = quote_volumes.get(pool_name).copied().unwrap_or(0);
        let last_price = last_price_map.get(pool_id).copied();

        // Conversion factors based on decimals
        let base_factor = (10u64).pow(pool.base_asset_decimals as u32);
        let quote_factor = (10u64).pow(pool.quote_asset_decimals as u32);
        let price_factor =
            (10u64).pow((9 - pool.base_asset_decimals + pool.quote_asset_decimals) as u32);

        response.insert(
            pool_name.clone(),
            HashMap::from([
                (
                    "last_price".to_string(),
                    Value::from(
                        last_price
                            .map(|price| (price as f64) / (price_factor as f64))
                            .unwrap_or(0.0),
                    ),
                ),
                (
                    "base_volume".to_string(),
                    Value::from((base_volume as f64) / (base_factor as f64)),
                ),
                (
                    "quote_volume".to_string(),
                    Value::from((quote_volume as f64) / (quote_factor as f64)),
                ),
                ("isFrozen".to_string(), Value::from(0)), // Fixed to 0 because all pools in pools table are active
            ]),
        );
    }

    Ok(Json(response))
}

async fn fetch_historical_volume(
    params: &HashMap<String, String>,
    volume_in_base: bool,
    state: &Arc<AppState>,
) -> Result<HashMap<String, u64>, DeepBookError> {
    let mut params_with_volume = params.clone();
    params_with_volume.insert("volume_in_base".to_string(), volume_in_base.to_string());

    all_historical_volume(Query(params_with_volume), State(state.clone()))
        .await
        .map(|Json(volumes)| volumes)
}

#[allow(clippy::get_first)]
async fn summary(
    State((state, rpc_url)): State<(Arc<AppState>, Url)>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    // Fetch pools metadata first since it's required for other functions
    let pools = state.reader.get_pools().await?;
    let pool_metadata: HashMap<String, (String, (i16, i16))> = pools
        .iter()
        .map(|pool| {
            (
                pool.pool_name.clone(),
                (
                    pool.pool_id.clone(),
                    (pool.base_asset_decimals, pool.quote_asset_decimals),
                ),
            )
        })
        .collect();

    // Prepare pool decimals for scaling
    let pool_decimals: HashMap<String, (i16, i16)> = pool_metadata
        .iter()
        .map(|(_, (pool_id, decimals))| (pool_id.clone(), *decimals))
        .collect();

    // Parallelize fetching ticker, price changes, and high/low prices
    let (ticker_result, price_change_result, high_low_result) = join!(
        ticker(Query(HashMap::new()), State(state.clone())),
        price_change_24h(&pool_metadata, State(state.clone())),
        high_low_prices_24h(&pool_decimals, State(state.clone()))
    );

    let Json(ticker_map) = ticker_result?;
    let price_change_map = price_change_result?;
    let high_low_map = high_low_result?;

    // Prepare futures for orderbook queries
    let orderbook_futures: Vec<_> = ticker_map
        .keys()
        .map(|pool_name| {
            let pool_name_clone = pool_name.clone();
            orderbook(
                Path(pool_name_clone),
                Query(HashMap::from([("level".to_string(), "1".to_string())])),
                State((state.clone(), rpc_url.clone())),
            )
        })
        .collect();

    // Run all orderbook queries concurrently
    let orderbook_results = join_all(orderbook_futures).await;

    let mut response = Vec::new();

    for ((pool_name, ticker_info), orderbook_result) in ticker_map.iter().zip(orderbook_results) {
        if let Some((pool_id, _)) = pool_metadata.get(pool_name) {
            // Extract data from the ticker function response
            let last_price = ticker_info
                .get("last_price")
                .and_then(|price| price.as_f64())
                .unwrap_or(0.0);

            let base_volume = ticker_info
                .get("base_volume")
                .and_then(|volume| volume.as_f64())
                .unwrap_or(0.0);

            let quote_volume = ticker_info
                .get("quote_volume")
                .and_then(|volume| volume.as_f64())
                .unwrap_or(0.0);

            // Fetch the 24-hour price change percent
            let price_change_percent = price_change_map.get(pool_name).copied().unwrap_or(0.0);

            // Fetch the highest and lowest prices in the last 24 hours
            let (highest_price, lowest_price) =
                high_low_map.get(pool_id).copied().unwrap_or((0.0, 0.0));

            // Process the parallel orderbook result
            let orderbook_data = orderbook_result.ok().map(|Json(data)| data);

            let highest_bid = orderbook_data
                .as_ref()
                .and_then(|data| data.get("bids"))
                .and_then(|bids| bids.as_array())
                .and_then(|bids| bids.get(0))
                .and_then(|bid| bid.as_array())
                .and_then(|bid| bid.get(0))
                .and_then(|price| price.as_str()?.parse::<f64>().ok())
                .unwrap_or(0.0);

            let lowest_ask = orderbook_data
                .as_ref()
                .and_then(|data| data.get("asks"))
                .and_then(|asks| asks.as_array())
                .and_then(|asks| asks.get(0))
                .and_then(|ask| ask.as_array())
                .and_then(|ask| ask.get(0))
                .and_then(|price| price.as_str()?.parse::<f64>().ok())
                .unwrap_or(0.0);

            let mut summary_data = HashMap::new();
            summary_data.insert(
                "trading_pairs".to_string(),
                Value::String(pool_name.clone()),
            );
            let parts: Vec<&str> = pool_name.split('_').collect();
            let base_currency = parts.get(0).unwrap_or(&"Unknown").to_string();
            let quote_currency = parts.get(1).unwrap_or(&"Unknown").to_string();

            summary_data.insert("base_currency".to_string(), Value::String(base_currency));
            summary_data.insert("quote_currency".to_string(), Value::String(quote_currency));
            summary_data.insert("last_price".to_string(), Value::from(last_price));
            summary_data.insert("base_volume".to_string(), Value::from(base_volume));
            summary_data.insert("quote_volume".to_string(), Value::from(quote_volume));
            summary_data.insert(
                "price_change_percent_24h".to_string(),
                Value::from(price_change_percent),
            );
            summary_data.insert("highest_price_24h".to_string(), Value::from(highest_price));
            summary_data.insert("lowest_price_24h".to_string(), Value::from(lowest_price));
            summary_data.insert("highest_bid".to_string(), Value::from(highest_bid));
            summary_data.insert("lowest_ask".to_string(), Value::from(lowest_ask));

            response.push(summary_data);
        }
    }

    Ok(Json(response))
}

async fn high_low_prices_24h(
    pool_decimals: &HashMap<String, (i16, i16)>,
    State(state): State<Arc<AppState>>,
) -> Result<HashMap<String, (f64, f64)>, DeepBookError> {
    // Get the current timestamp in milliseconds
    let end_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DeepBookError::InternalError("System time error".to_string()))?
        .as_millis() as i64;

    // Calculate the start time for 24 hours ago
    let start_time = end_time - 24 * 60 * 60 * 1000;

    // Query for trades within the last 24 hours for all pools
    let query = schema::order_fills::table
        .filter(schema::order_fills::checkpoint_timestamp_ms.between(start_time, end_time))
        .group_by(schema::order_fills::pool_id)
        .select((
            schema::order_fills::pool_id,
            max(schema::order_fills::price),
            min(schema::order_fills::price),
        ));
    let results: Vec<(String, Option<i64>, Option<i64>)> = state.reader.results(query).await?;

    // Aggregate the highest and lowest prices for each pool
    let mut price_map: HashMap<String, (f64, f64)> = HashMap::new();

    for (pool_id, max_price_opt, min_price_opt) in results {
        if let Some((base_decimals, quote_decimals)) = pool_decimals.get(&pool_id) {
            let scaling_factor = (10f64).powi((9 - base_decimals + quote_decimals) as i32);

            let max_price_f64 = (max_price_opt.unwrap_or(0) as f64) / scaling_factor;
            let min_price_f64 = (min_price_opt.unwrap_or(0) as f64) / scaling_factor;

            price_map.insert(pool_id, (max_price_f64, min_price_f64));
        }
    }

    Ok(price_map)
}

async fn price_change_24h(
    pool_metadata: &HashMap<String, (String, (i16, i16))>,
    State(state): State<Arc<AppState>>,
) -> Result<HashMap<String, f64>, DeepBookError> {
    // Calculate the timestamp for 24 hours ago
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DeepBookError::InternalError("System time error".to_string()))?
        .as_millis() as i64;

    let timestamp_24h_ago = now - 24 * 60 * 60 * 1000; // 24 hours in milliseconds
    let timestamp_48h_ago = now - 48 * 60 * 60 * 1000; // 48 hours in milliseconds

    let mut response = HashMap::new();

    for (pool_name, (pool_id, (base_decimals, quote_decimals))) in pool_metadata.iter() {
        // Get the latest price <= 24 hours ago. Only trades until 48 hours ago will count.
        let earliest_trade_24h = state
            .reader
            .get_price(timestamp_48h_ago, timestamp_24h_ago, pool_id)
            .await;
        // Get the most recent price. Only trades until 24 hours ago will count.
        let most_recent_trade = state
            .reader
            .get_price(timestamp_24h_ago, now, pool_id)
            .await;

        if let (Ok(earliest_price), Ok(most_recent_price)) = (earliest_trade_24h, most_recent_trade)
        {
            let price_factor = (10u64).pow((9 - base_decimals + quote_decimals) as u32);

            // Scale the prices
            let earliest_price_scaled = (earliest_price as f64) / (price_factor as f64);
            let most_recent_price_scaled = (most_recent_price as f64) / (price_factor as f64);

            // Calculate price change percentage
            let price_change_percent =
                (most_recent_price_scaled / earliest_price_scaled - 1.0) * 100.0;

            response.insert(pool_name.clone(), price_change_percent);
        } else {
            // If there's no price data for 24 hours or recent trades, insert 0.0 as price change
            response.insert(pool_name.clone(), 0.0);
        }
    }

    Ok(response)
}

async fn order_updates(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    // Fetch pool data with proper error handling
    let (pool_id, base_decimals, quote_decimals) =
        state.reader.get_pool_decimals(&pool_name).await?;
    let base_decimals = base_decimals as u8;
    let quote_decimals = quote_decimals as u8;

    let end_time = params.end_time();

    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let limit = params.limit();

    let balance_manager_filter = params.get("balance_manager_id").cloned();
    let status_filter = params.get("status").cloned();

    let trades = state
        .reader
        .get_order_updates(
            pool_id,
            start_time,
            end_time,
            limit,
            balance_manager_filter,
            status_filter,
        )
        .await?;

    let base_factor = (10u64).pow(base_decimals as u32);
    let price_factor = (10u64).pow((9 - base_decimals + quote_decimals) as u32);

    let trade_data: Vec<HashMap<String, Value>> = trades
        .into_iter()
        .map(
            |(
                order_id,
                price,
                original_quantity,
                quantity,
                filled_quantity,
                timestamp,
                is_bid,
                balance_manager_id,
                status,
            )| {
                let trade_type = if is_bid { "buy" } else { "sell" };
                HashMap::from([
                    ("order_id".to_string(), Value::from(order_id)),
                    (
                        "price".to_string(),
                        Value::from((price as f64) / (price_factor as f64)),
                    ),
                    (
                        "original_quantity".to_string(),
                        Value::from((original_quantity as f64) / (base_factor as f64)),
                    ),
                    (
                        "remaining_quantity".to_string(),
                        Value::from((quantity as f64) / (base_factor as f64)),
                    ),
                    (
                        "filled_quantity".to_string(),
                        Value::from((filled_quantity as f64) / (base_factor as f64)),
                    ),
                    ("timestamp".to_string(), Value::from(timestamp as u64)),
                    ("type".to_string(), Value::from(trade_type)),
                    (
                        "balance_manager_id".to_string(),
                        Value::from(balance_manager_id),
                    ),
                    ("status".to_string(), Value::from(status)),
                ])
            },
        )
        .collect();

    Ok(Json(trade_data))
}

async fn trades(
    Path(pool_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<HashMap<String, Value>>>, DeepBookError> {
    // Fetch all pools to map names to IDs and decimals
    let (pool_id, base_decimals, quote_decimals) =
        state.reader.get_pool_decimals(&pool_name).await?;
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time()
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    // Parse limit (default to 1 if not provided)
    let limit = params.limit();

    // Parse optional filters for balance managers
    let maker_balance_manager_filter = params.get("maker_balance_manager_id").cloned();
    let taker_balance_manager_filter = params.get("taker_balance_manager_id").cloned();

    let base_decimals = base_decimals as u8;
    let quote_decimals = quote_decimals as u8;

    let trades = state
        .reader
        .get_orders(
            pool_name,
            pool_id,
            start_time,
            end_time,
            limit,
            maker_balance_manager_filter,
            taker_balance_manager_filter,
        )
        .await?;

    // Conversion factors for decimals
    let base_factor = (10u64).pow(base_decimals as u32);
    let quote_factor = (10u64).pow(quote_decimals as u32);
    let price_factor = (10u64).pow((9 - base_decimals + quote_decimals) as u32);

    // Map trades to JSON format
    let trade_data = trades
        .into_iter()
        .map(
            |(
                maker_order_id,
                taker_order_id,
                price,
                base_quantity,
                quote_quantity,
                timestamp,
                taker_is_bid,
                maker_balance_manager_id,
                taker_balance_manager_id,
            )| {
                let trade_id = calculate_trade_id(&maker_order_id, &taker_order_id).unwrap_or(0);
                let trade_type = if taker_is_bid { "buy" } else { "sell" };

                HashMap::from([
                    ("trade_id".to_string(), Value::from(trade_id.to_string())),
                    ("maker_order_id".to_string(), Value::from(maker_order_id)),
                    ("taker_order_id".to_string(), Value::from(taker_order_id)),
                    (
                        "maker_balance_manager_id".to_string(),
                        Value::from(maker_balance_manager_id),
                    ),
                    (
                        "taker_balance_manager_id".to_string(),
                        Value::from(taker_balance_manager_id),
                    ),
                    (
                        "price".to_string(),
                        Value::from((price as f64) / (price_factor as f64)),
                    ),
                    (
                        "base_volume".to_string(),
                        Value::from((base_quantity as f64) / (base_factor as f64)),
                    ),
                    (
                        "quote_volume".to_string(),
                        Value::from((quote_quantity as f64) / (quote_factor as f64)),
                    ),
                    ("timestamp".to_string(), Value::from(timestamp as u64)),
                    ("type".to_string(), Value::from(trade_type)),
                ])
            },
        )
        .collect();

    Ok(Json(trade_data))
}

async fn trade_count(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<i64>, DeepBookError> {
    // Parse start_time and end_time
    let end_time = params.end_time();
    let start_time = params
        .start_time() // Convert to milliseconds
        .unwrap_or_else(|| end_time - 24 * 60 * 60 * 1000);

    let query = schema::order_fills::table
        .select(count_star())
        .filter(schema::order_fills::checkpoint_timestamp_ms.between(start_time, end_time));

    let result = state.reader.first(query).await?;
    Ok(Json(result))
}

fn calculate_trade_id(maker_id: &str, taker_id: &str) -> Result<u128, DeepBookError> {
    // Parse maker_id and taker_id as u128
    let maker_id = maker_id
        .parse::<u128>()
        .map_err(|_| DeepBookError::InternalError("Invalid maker_id".to_string()))?;
    let taker_id = taker_id
        .parse::<u128>()
        .map_err(|_| DeepBookError::InternalError("Invalid taker_id".to_string()))?;

    // Ignore the most significant bit for both IDs
    let maker_id = maker_id & !(1 << 127);
    let taker_id = taker_id & !(1 << 127);

    // Return the sum of the modified IDs as the trade_id
    Ok(maker_id + taker_id)
}

pub async fn assets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, HashMap<String, Value>>>, DeepBookError> {
    let query = schema::assets::table.select((
        schema::assets::symbol,
        schema::assets::name,
        schema::assets::ucid,
        schema::assets::package_address_url,
        schema::assets::package_id,
    ));
    let assets: Vec<(String, String, Option<i32>, Option<String>, Option<String>)> =
        state.reader.results(query).await.map_err(|err| {
            DeepBookError::InternalError(format!("Failed to query assets: {}", err))
        })?;
    let mut response = HashMap::new();

    for (symbol, name, ucid, package_address_url, package_id) in assets {
        let mut asset_info = HashMap::new();
        asset_info.insert("name".to_string(), Value::String(name));
        asset_info.insert(
            "can_withdraw".to_string(),
            Value::String("true".to_string()),
        );
        asset_info.insert("can_deposit".to_string(), Value::String("true".to_string()));

        if let Some(ucid) = ucid {
            asset_info.insert(
                "unified_cryptoasset_id".to_string(),
                Value::String(ucid.to_string()),
            );
        }
        if let Some(addresses) = package_address_url {
            asset_info.insert("contractAddressUrl".to_string(), Value::String(addresses));
        }

        if let Some(addresses) = package_id {
            asset_info.insert("contractAddress".to_string(), Value::String(addresses));
        }

        response.insert(symbol, asset_info);
    }

    Ok(Json(response))
}

/// Level2 data for all pools
async fn orderbook(
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
    result.insert("bids".to_string(), Value::Array(bids));

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
    result.insert("asks".to_string(), Value::Array(asks));

    Ok(Json(result))
}

/// DEEP total supply
async fn deep_supply(
    State((_, rpc_url)): State<(Arc<AppState>, Url)>,
) -> Result<Json<u64>, DeepBookError> {
    let sui_client = SuiClientBuilder::default().build(rpc_url.as_str()).await?;
    let mut ptb = ProgrammableTransactionBuilder::new();

    let deep_treasury_object_id = ObjectID::from_hex_literal(DEEP_TREASURY_ID)?;
    let deep_treasury_object: SuiObjectResponse = sui_client
        .read_api()
        .get_object_with_options(
            deep_treasury_object_id,
            SuiObjectDataOptions::full_content(),
        )
        .await?;
    let deep_treasury_data: &SuiObjectData =
        deep_treasury_object
            .data
            .as_ref()
            .ok_or(DeepBookError::InternalError(
                "Incorrect Treasury ID".to_string(),
            ))?;

    let deep_treasury_ref: ObjectRef = (
        deep_treasury_data.object_id,
        deep_treasury_data.version,
        deep_treasury_data.digest,
    );

    let deep_treasury_input = CallArg::Object(ObjectArg::ImmOrOwnedObject(deep_treasury_ref));
    ptb.input(deep_treasury_input)?;

    let package = ObjectID::from_hex_literal(DEEP_TOKEN_PACKAGE_ID).map_err(|e| {
        DeepBookError::InternalError(format!("Invalid deep token package ID: {}", e))
    })?;
    let module = DEEP_SUPPLY_MODULE.to_string();
    let function = DEEP_SUPPLY_FUNCTION.to_string();

    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0)],
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

    let total_supply = &binding
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No return values for total supply".to_string(),
        ))?
        .return_values
        .first_mut()
        .ok_or(DeepBookError::InternalError(
            "No total supply data found".to_string(),
        ))?
        .0;

    let total_supply_value: u64 = bcs::from_bytes(total_supply).map_err(|_| {
        DeepBookError::InternalError("Failed to deserialize total supply".to_string())
    })?;

    Ok(Json(total_supply_value))
}

async fn get_net_deposits(
    Path((asset_ids, timestamp)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<HashMap<String, i64>>, DeepBookError> {
    let mut query =
        "SELECT asset, SUM(amount)::bigint AS amount, deposit FROM balances WHERE checkpoint_timestamp_ms < ".to_string();
    query.push_str(&timestamp);
    query.push_str("000 AND asset in (");
    for asset in asset_ids.split(",") {
        if asset.starts_with("0x") {
            let len = asset.len();
            query.push_str(&format!("'{}',", &asset[2..len]));
        } else {
            query.push_str(&format!("'{}',", asset));
        }
    }
    query.pop();
    query.push_str(") GROUP BY asset, deposit");

    let results: Vec<BalancesSummary> = state.reader.results(diesel::sql_query(query)).await?;
    let mut net_deposits = HashMap::new();
    for result in results {
        let mut asset = result.asset;
        if !asset.starts_with("0x") {
            asset.insert_str(0, "0x");
        }
        let amount = result.amount;
        if result.deposit {
            *net_deposits.entry(asset).or_insert(0) += amount;
        } else {
            *net_deposits.entry(asset).or_insert(0) -= amount;
        }
    }

    Ok(Json(net_deposits))
}

pub async fn get_order_fills(
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

    let result: Vec<OrderFill> = state
        .reader
        .results(
            schema::order_fills::table
                .select(OrderFill::as_select())
                .filter(schema::order_fills::checkpoint_timestamp_ms.between(start_time, end_time))
                .filter(schema::order_fills::pool_id.eq(pool_id)),
        )
        .await?;

    Ok(Json(
        result
            .into_iter()
            .map(|fill| {
                let mut map = HashMap::new();
                map.insert("event_digest".into(), Value::String(fill.event_digest));
                map.insert("digest".into(), Value::String(fill.digest));
                map.insert("sender".into(), Value::String(fill.sender));
                map.insert("checkpoint".into(), Value::from(fill.checkpoint));
                map.insert(
                    "checkpoint_timestamp_ms".into(),
                    Value::from(fill.checkpoint_timestamp_ms),
                );
                map.insert(
                    "timestamp".into(),
                    Value::from(((fill.checkpoint_timestamp_ms as f64) / 1000.0).round() as i64),
                );
                map.insert("package".into(), Value::String(fill.package));
                map.insert("pool_id".into(), Value::String(fill.pool_id));
                map.insert("maker_order_id".into(), Value::String(fill.maker_order_id));
                map.insert("taker_order_id".into(), Value::String(fill.taker_order_id));
                map.insert(
                    "maker_client_order_id".into(),
                    Value::from(fill.maker_client_order_id),
                );
                map.insert(
                    "taker_client_order_id".into(),
                    Value::from(fill.taker_client_order_id),
                );
                map.insert("price".into(), Value::from(fill.price));
                map.insert("taker_fee".into(), Value::from(fill.taker_fee));
                map.insert(
                    "taker_fee_is_deep".into(),
                    Value::from(fill.taker_fee_is_deep),
                );
                map.insert("maker_fee".into(), Value::from(fill.maker_fee));
                map.insert(
                    "maker_fee_is_deep".into(),
                    Value::from(fill.maker_fee_is_deep),
                );
                map.insert("taker_is_bid".into(), Value::from(fill.taker_is_bid));
                map.insert("base_quantity".into(), Value::from(fill.base_quantity));
                map.insert("quote_quantity".into(), Value::from(fill.quote_quantity));
                map.insert(
                    "maker_balance_manager_id".into(),
                    Value::String(fill.maker_balance_manager_id),
                );
                map.insert(
                    "taker_balance_manager_id".into(),
                    Value::String(fill.taker_balance_manager_id),
                );
                map.insert(
                    "onchain_timestamp".into(),
                    Value::from(fill.onchain_timestamp),
                );
                map
            })
            .collect(),
    ))
}

pub fn parse_type_input(type_str: &str) -> Result<TypeInput, DeepBookError> {
    let type_tag = TypeTag::from_str(type_str)?;
    Ok(TypeInput::from(type_tag))
}

// --- WebSocket handlers for orderbook and other data ---

async fn orderbook_ws(
    ws: WebSocketUpgrade,
    Path(pool_name): Path<String>,
    State(state): State<(Arc<AppState>, Url)>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_orderbook_socket(socket, pool_name, state.0.clone()))
}

async fn latest_trades_ws(
    ws: WebSocketUpgrade,
    Path(pool_name): Path<String>,
    State(state): State<(Arc<AppState>, Url)>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_latest_trades_socket(socket, pool_name, state.0.clone()))
}

async fn orderbook_bests_ws(
    ws: WebSocketUpgrade,
    Path(pool_name): Path<String>,
    State(state): State<(Arc<AppState>, Url)>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_bests_socket(socket, pool_name, state.0.clone()))
}

async fn orderbook_spread_ws(
    ws: WebSocketUpgrade,
    Path(pool_name): Path<String>,
    State(state): State<(Arc<AppState>, Url)>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_spread_socket(socket, pool_name, state.0.clone()))
}

async fn handle_orderbook_socket(mut socket: WebSocket, pool_name: String, state: Arc<AppState>) {
    // Redis key that stores the order‑book JSON
    let redis_key = format!("orderbook::{}", pool_name);

    // Clone the async cache and extract the underlying Redis client
    let cache = state.reader.cache.clone();
    let mut pubsub = cache
        .client
        .get_async_pubsub()
        .await
        .expect("Failed getting pubsub");
    let channel = format!("__keyspace@0__:{}", redis_key);

    pubsub
        .subscribe(&channel)
        .await
        .expect("Failed to subscribe to key‑space");

    // Helper to grab latest JSON
    let fetch_latest = || async {
        cache
            .get::<serde_json::Value>(&redis_key)
            .await
            .ok()
            .flatten()
            .map(|v| v.to_string())
    };

    // Send initial snapshot if present
    let mut last_sent = fetch_latest().await;
    if let Some(snapshot) = &last_sent {
        let _ = socket.send(Message::Text(snapshot.clone())).await;
    }

    // Stream of Redis events
    let mut redis_stream = pubsub.on_message();

    loop {
        tokio::select! {
            // Client closed WebSocket
            maybe_msg = socket.recv().fuse() => {
                if maybe_msg.is_none() {
                    break;
                }
            }
            // Redis published an event
            Some(_msg) = redis_stream.next() => {
                if let Some(current) = fetch_latest().await {
                    if Some(&current) != last_sent.as_ref() {
                        last_sent = Some(current.clone());
                        let _ = socket.send(Message::Text(current)).await;
                    }
                }
            }
        }
    }
}

async fn handle_bests_socket(mut socket: WebSocket, pool_name: String, state: Arc<AppState>) {
    // Redis key that stores the order‑book JSON
    let redis_key = format!("orderbook::{}", pool_name);

    // // Clone the async cache and extract the underlying Redis client
    let cache = state.reader.cache.clone();
    let mut pubsub = cache
        .client
        .get_async_pubsub()
        .await
        .expect("Failed getting pubsub");
    let channel = format!("__keyspace@0__:{}", redis_key);

    pubsub
        .subscribe(&channel)
        .await
        .expect("Failed to subscribe to key‑space");

    // Helper to grab latest JSON
    let fetch_latest = || async {
        cache
            .get::<serde_json::Value>(&redis_key)
            .await
            .ok()
            .flatten()
    };

    let mut last_sent = fetch_latest().await;

    let bests = get_bests_from_redis_orderbook(last_sent.clone());
    let stringified = serde_json::to_string(&bests);

    if let Ok(message) = stringified {
        let _ = socket.send(Message::Text(message)).await;
    };

    // Stream of Redis events
    let mut redis_stream = pubsub.on_message();

    loop {
        tokio::select! {
            // Client closed WebSocket
            maybe_msg = socket.recv().fuse() => {
                if maybe_msg.is_none() {
                    break;
                }
            }
            // Redis published an event
            Some(_msg) = redis_stream.next() => {
                if let Some(current) = fetch_latest().await {
                    if Some(&current) != last_sent.as_ref() {
                        last_sent = Some(current.clone());

                        let bests = get_bests_from_redis_orderbook(last_sent.clone());
                        let stringified = serde_json::to_string(&bests);

                        if let Ok(message) = stringified {
                            let _ = socket.send(Message::Text(message)).await;
                        }
                    }
                }
            }
        }
    }
}

async fn handle_latest_trades_socket(
    mut socket: WebSocket,
    pool_name: String,
    state: Arc<AppState>,
) {
    // Redis key that stores the order‑book JSON
    let redis_key = format!("latest_trades::{}", pool_name);

    // Clone the async cache and extract the underlying Redis client
    let cache = state.reader.cache.clone();
    let mut pubsub = cache
        .client
        .get_async_pubsub()
        .await
        .expect("Failed getting pubsub");
    let channel = format!("__keyspace@0__:{}", redis_key);

    pubsub
        .subscribe(&channel)
        .await
        .expect("Failed to subscribe to key‑space");

    let mut redis_stream = pubsub.on_message();

    // Helper to fetch the full JSON array from Redis
    let fetch_latest = || async {
        cache
            .get_array::<serde_json::Value>(&redis_key)
            .await
            .ok()
            .flatten()
            .map(|array| serde_json::to_string(&array).ok())
            .flatten()
    };

    // Send initial array if present
    let mut last_sent = fetch_latest().await;
    if let Some(snapshot) = &last_sent {
        let _ = socket.send(Message::Text(snapshot.clone())).await;
    }

    // Main loop: respond to Redis events or client disconnect
    loop {
        tokio::select! {
            // Client closed WebSocket
            maybe_msg = socket.recv() => {
                if maybe_msg.is_none() {
                    break;
                }
            }

            // Redis published a change event
            Some(_msg) = redis_stream.next() => {
                if let Some(current) = fetch_latest().await {
                    if Some(&current) != last_sent.as_ref() {
                        last_sent = Some(current.clone());
                        let _ = socket.send(Message::Text(current)).await;
                    }
                }
            }
        }
    }
}

async fn handle_spread_socket(mut socket: WebSocket, pool_name: String, state: Arc<AppState>) {
    // Redis key that stores the order‑book JSON
    let redis_key = format!("orderbook::{}", pool_name);

    // // Clone the async cache and extract the underlying Redis client
    let cache = state.reader.cache.clone();
    let mut pubsub = cache
        .client
        .get_async_pubsub()
        .await
        .expect("Failed getting pubsub");
    let channel = format!("__keyspace@0__:{}", redis_key);

    pubsub
        .subscribe(&channel)
        .await
        .expect("Failed to subscribe to key‑space");

    // Helper to grab latest JSON
    let fetch_latest = || async {
        cache
            .get::<serde_json::Value>(&redis_key)
            .await
            .ok()
            .flatten()
    };

    let mut last_sent = fetch_latest().await;

    let bests = get_bests_from_redis_orderbook(last_sent.clone());
    let spread = get_spread_from_bests(bests);

    let stringified = serde_json::to_string(&spread);

    if let Ok(message) = stringified {
        let _ = socket.send(Message::Text(message)).await;
    };

    // Stream of Redis events
    let mut redis_stream = pubsub.on_message();

    loop {
        tokio::select! {
            // Client closed WebSocket
            maybe_msg = socket.recv().fuse() => {
                if maybe_msg.is_none() {
                    break;
                }
            }
            // Redis published an event
            Some(_msg) = redis_stream.next() => {
                if let Some(current) = fetch_latest().await {
                    if Some(&current) != last_sent.as_ref() {
                        last_sent = Some(current.clone());

                        let bests = get_bests_from_redis_orderbook(last_sent.clone());
                        let spread = get_spread_from_bests(bests);
                        let stringified = serde_json::to_string(&spread);

                        if let Ok(message) = stringified {
                            let _ = socket.send(Message::Text(message)).await;
                        }
                    }
                }
            }
        }
    }
}

fn get_spread_from_bests(bests: Option<HashMap<String, HashMap<String, f64>>>) -> Option<f64> {
    let map = bests?;

    let ask_price = map.get("asks")?.get("price")?;
    let bid_price = map.get("bids")?.get("price")?;

    Some(ask_price - bid_price)
}

fn get_bests_from_redis_orderbook(
    value: Option<Value>,
) -> Option<HashMap<String, HashMap<String, f64>>> {
    let ob = parse_orderbook_from_redis(value?)?;

    let best_ask = ob.get("asks")?.first()?;
    let best_bid = ob.get("bids")?.first()?;

    let res = HashMap::from([
        ("asks".to_string(), best_ask.clone()),
        ("bids".to_string(), best_bid.clone()),
    ]);

    Some(res)
}

fn parse_orderbook_from_redis(value: Value) -> Option<HashMap<String, Vec<HashMap<String, f64>>>> {
    let ob = value.as_object()?;

    let asks_raw = ob.get("asks")?.as_array()?;
    let bids_raw = ob.get("bids")?.as_array()?;

    let mut asks: Vec<HashMap<String, f64>> = asks_raw
        .iter()
        .filter_map(|v| serde_json::from_value(v.clone()).ok())
        .collect();

    let mut bids: Vec<HashMap<String, f64>> = bids_raw
        .iter()
        .filter_map(|v| serde_json::from_value(v.clone()).ok())
        .collect();

    asks.sort_by(|a, b| a.get("price").partial_cmp(&b.get("price")).unwrap());

    bids.sort_by(|a, b| b.get("price").partial_cmp(&a.get("price")).unwrap());

    Some(HashMap::from([
        ("asks".to_string(), asks),
        ("bids".to_string(), bids),
    ]))
}

pub trait ParameterUtil {
    fn start_time(&self) -> Option<i64>;
    fn end_time(&self) -> i64;
    fn volume_in_base(&self) -> bool;

    fn limit(&self) -> i64;
    fn days(&self) -> i64;
}

impl ParameterUtil for HashMap<String, String> {
    fn start_time(&self) -> Option<i64> {
        self.get("start_time")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|t| t * 1000) // Convert
    }

    fn end_time(&self) -> i64 {
        self.get("end_time")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|t| t * 1000) // Convert to milliseconds
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            })
    }

    fn volume_in_base(&self) -> bool {
        self.get("volume_in_base")
            .map(|v| v == "true")
            .unwrap_or_default()
    }

    fn limit(&self) -> i64 {
        self.get("limit")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1)
    }
    // defaults to 1
    fn days(&self) -> i64 {
        self.get("days")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1)
    }
}

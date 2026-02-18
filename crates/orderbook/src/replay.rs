use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use chrono::NaiveDateTime;
use deeplook_schema::models::{OrderFill, OrderUpdate, OrderUpdateStatus};
use deeplook_schema::schema;
use diesel::{
    Connection, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
    deserialize::Queryable,
};
use tracing::warn;
use url::Url;

use crate::OrderbookManagerMap;

pub const ORDER_UPDATE_PIPELINE: &str = "order_update";
pub const ORDER_FILL_PIPELINE: &str = "order_fill";
pub const REPLAY_BATCH_SIZE: i64 = 2_000;

#[derive(Queryable)]
struct ReplayOrderUpdateRow {
    event_digest: String,
    digest: String,
    sender: String,
    checkpoint: i64,
    checkpoint_timestamp_ms: i64,
    timestamp: NaiveDateTime,
    package: String,
    status: String,
    pool_id: String,
    order_id: String,
    client_order_id: i64,
    price: i64,
    is_bid: bool,
    original_quantity: i64,
    quantity: i64,
    filled_quantity: i64,
    onchain_timestamp: i64,
    trader: String,
    balance_manager_id: String,
}

#[derive(Queryable)]
struct ReplayOrderFillRow {
    event_digest: String,
    digest: String,
    sender: String,
    checkpoint: i64,
    checkpoint_timestamp_ms: i64,
    timestamp: NaiveDateTime,
    package: String,
    pool_id: String,
    maker_order_id: String,
    taker_order_id: String,
    maker_client_order_id: i64,
    taker_client_order_id: i64,
    price: i64,
    taker_fee: i64,
    taker_fee_is_deep: bool,
    maker_fee: i64,
    maker_fee_is_deep: bool,
    taker_is_bid: bool,
    base_quantity: i64,
    quote_quantity: i64,
    maker_balance_manager_id: String,
    taker_balance_manager_id: String,
    onchain_timestamp: i64,
}

fn event_index(digest: &str, event_digest: &str) -> u64 {
    event_digest
        .strip_prefix(digest)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn establish_connection(database_url: &Url) -> Result<PgConnection, anyhow::Error> {
    PgConnection::establish(database_url.as_str()).context("Error connecting to DB")
}

pub fn get_unique_pool_ids(orderbook_managers: &Arc<OrderbookManagerMap>) -> Vec<String> {
    let mut ids = HashSet::new();
    for manager in orderbook_managers.values() {
        if let Ok(locked) = manager.lock() {
            ids.insert(locked.pool.pool_id.clone());
        }
    }
    let mut ids: Vec<String> = ids.into_iter().collect();
    ids.sort();
    ids
}

pub fn get_lowest_manager_checkpoint(
    orderbook_managers: &Arc<OrderbookManagerMap>,
) -> Result<i64, anyhow::Error> {
    orderbook_managers
        .values()
        .filter_map(|manager| manager.lock().ok().map(|m| m.initial_checkpoint))
        .min()
        .context("failed getting starting checkpoint")
}

pub fn get_replay_upper_checkpoint(
    conn: &mut PgConnection,
) -> Result<Option<i64>, diesel::result::Error> {
    let updates_hi = schema::watermarks::table
        .filter(schema::watermarks::pipeline.eq(ORDER_UPDATE_PIPELINE))
        .select(schema::watermarks::checkpoint_hi_inclusive)
        .first::<i64>(conn)
        .optional()?;

    let fills_hi = schema::watermarks::table
        .filter(schema::watermarks::pipeline.eq(ORDER_FILL_PIPELINE))
        .select(schema::watermarks::checkpoint_hi_inclusive)
        .first::<i64>(conn)
        .optional()?;

    Ok(match (updates_hi, fills_hi) {
        (Some(u), Some(f)) => Some(u.min(f)),
        _ => None,
    })
}

pub fn apply_range(
    conn: &mut PgConnection,
    orderbook_managers: &Arc<OrderbookManagerMap>,
    pool_ids: &[String],
    start_checkpoint: i64,
    end_checkpoint: i64,
) -> Result<(), anyhow::Error> {
    if start_checkpoint > end_checkpoint || pool_ids.is_empty() {
        return Ok(());
    }

    let update_rows = schema::order_updates::table
        .filter(schema::order_updates::pool_id.eq_any(pool_ids))
        .filter(schema::order_updates::checkpoint.ge(start_checkpoint))
        .filter(schema::order_updates::checkpoint.le(end_checkpoint))
        .select((
            schema::order_updates::event_digest,
            schema::order_updates::digest,
            schema::order_updates::sender,
            schema::order_updates::checkpoint,
            schema::order_updates::checkpoint_timestamp_ms,
            schema::order_updates::timestamp,
            schema::order_updates::package,
            schema::order_updates::status,
            schema::order_updates::pool_id,
            schema::order_updates::order_id,
            schema::order_updates::client_order_id,
            schema::order_updates::price,
            schema::order_updates::is_bid,
            schema::order_updates::original_quantity,
            schema::order_updates::quantity,
            schema::order_updates::filled_quantity,
            schema::order_updates::onchain_timestamp,
            schema::order_updates::trader,
            schema::order_updates::balance_manager_id,
        ))
        .load::<ReplayOrderUpdateRow>(conn)?;

    let mut updates = Vec::with_capacity(update_rows.len());
    for row in update_rows {
        let status = match OrderUpdateStatus::from_str(&row.status) {
            Ok(status) => status,
            Err(_) => {
                warn!("Unknown order update status '{}', skipping row", row.status);
                continue;
            }
        };

        updates.push(OrderUpdate {
            event_digest: row.event_digest,
            digest: row.digest,
            sender: row.sender,
            checkpoint: row.checkpoint,
            checkpoint_timestamp_ms: row.checkpoint_timestamp_ms,
            timestamp: row.timestamp,
            package: row.package,
            status,
            pool_id: row.pool_id,
            order_id: row.order_id,
            client_order_id: row.client_order_id,
            price: row.price,
            is_bid: row.is_bid,
            original_quantity: row.original_quantity,
            quantity: row.quantity,
            filled_quantity: row.filled_quantity,
            onchain_timestamp: row.onchain_timestamp,
            trader: row.trader,
            balance_manager_id: row.balance_manager_id,
        });
    }

    let fill_rows = schema::order_fills::table
        .filter(schema::order_fills::pool_id.eq_any(pool_ids))
        .filter(schema::order_fills::checkpoint.ge(start_checkpoint))
        .filter(schema::order_fills::checkpoint.le(end_checkpoint))
        .select((
            schema::order_fills::event_digest,
            schema::order_fills::digest,
            schema::order_fills::sender,
            schema::order_fills::checkpoint,
            schema::order_fills::checkpoint_timestamp_ms,
            schema::order_fills::timestamp,
            schema::order_fills::package,
            schema::order_fills::pool_id,
            schema::order_fills::maker_order_id,
            schema::order_fills::taker_order_id,
            schema::order_fills::maker_client_order_id,
            schema::order_fills::taker_client_order_id,
            schema::order_fills::price,
            schema::order_fills::taker_fee,
            schema::order_fills::taker_fee_is_deep,
            schema::order_fills::maker_fee,
            schema::order_fills::maker_fee_is_deep,
            schema::order_fills::taker_is_bid,
            schema::order_fills::base_quantity,
            schema::order_fills::quote_quantity,
            schema::order_fills::maker_balance_manager_id,
            schema::order_fills::taker_balance_manager_id,
            schema::order_fills::onchain_timestamp,
        ))
        .load::<ReplayOrderFillRow>(conn)?;

    let mut fills = Vec::with_capacity(fill_rows.len());
    for row in fill_rows {
        fills.push(OrderFill {
            event_digest: row.event_digest,
            digest: row.digest,
            sender: row.sender,
            checkpoint: row.checkpoint,
            checkpoint_timestamp_ms: row.checkpoint_timestamp_ms,
            timestamp: row.timestamp,
            package: row.package,
            pool_id: row.pool_id,
            maker_order_id: row.maker_order_id,
            taker_order_id: row.taker_order_id,
            maker_client_order_id: row.maker_client_order_id,
            taker_client_order_id: row.taker_client_order_id,
            price: row.price,
            taker_fee: row.taker_fee,
            taker_fee_is_deep: row.taker_fee_is_deep,
            maker_fee: row.maker_fee,
            maker_fee_is_deep: row.maker_fee_is_deep,
            taker_is_bid: row.taker_is_bid,
            base_quantity: row.base_quantity,
            quote_quantity: row.quote_quantity,
            maker_balance_manager_id: row.maker_balance_manager_id,
            taker_balance_manager_id: row.taker_balance_manager_id,
            onchain_timestamp: row.onchain_timestamp,
        });
    }

    updates.sort_by(|a, b| {
        a.checkpoint
            .cmp(&b.checkpoint)
            .then_with(|| a.pool_id.cmp(&b.pool_id))
            .then_with(|| a.digest.cmp(&b.digest))
            .then_with(|| event_index(&a.digest, &a.event_digest).cmp(&event_index(&b.digest, &b.event_digest)))
    });

    fills.sort_by(|a, b| {
        a.checkpoint
            .cmp(&b.checkpoint)
            .then_with(|| a.pool_id.cmp(&b.pool_id))
            .then_with(|| a.digest.cmp(&b.digest))
            .then_with(|| event_index(&a.digest, &a.event_digest).cmp(&event_index(&b.digest, &b.event_digest)))
    });

    let mut updates_by_checkpoint: BTreeMap<i64, BTreeMap<String, Vec<OrderUpdate>>> = BTreeMap::new();
    let mut fills_by_checkpoint: BTreeMap<i64, BTreeMap<String, Vec<OrderFill>>> = BTreeMap::new();

    for update in updates {
        updates_by_checkpoint
            .entry(update.checkpoint)
            .or_default()
            .entry(update.pool_id.clone())
            .or_default()
            .push(update);
    }

    for fill in fills {
        fills_by_checkpoint
            .entry(fill.checkpoint)
            .or_default()
            .entry(fill.pool_id.clone())
            .or_default()
            .push(fill);
    }

    let mut checkpoints: BTreeSet<i64> = updates_by_checkpoint.keys().copied().collect();
    checkpoints.extend(fills_by_checkpoint.keys().copied());

    for checkpoint in checkpoints {
        let mut updates_by_pool = updates_by_checkpoint.remove(&checkpoint).unwrap_or_default();
        let mut fills_by_pool = fills_by_checkpoint.remove(&checkpoint).unwrap_or_default();

        let mut pools: Vec<String> = updates_by_pool
            .keys()
            .chain(fills_by_pool.keys())
            .cloned()
            .collect();
        pools.sort();
        pools.dedup();

        for pool_id in pools {
            let updates = updates_by_pool.remove(&pool_id).unwrap_or_default();
            let fills = fills_by_pool.remove(&pool_id).unwrap_or_default();

            if let Some(manager) = orderbook_managers.get(&pool_id) {
                if let Ok(mut locked) = manager.lock() {
                    locked.handle_batch(updates, fills);
                }
            } else {
                warn!("Missing orderbook manager for pool {}", pool_id);
            }
        }
    }

    Ok(())
}

use std::collections::HashMap;

use chrono::NaiveDateTime;
use clap::Parser;
use diesel::{Connection, PgConnection, Queryable};
use tracing::{info, warn};
use url::Url;

use deeplook_schema::{models::OrderbookSnapshot, schema};
use diesel::prelude::*;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/deeplook"
    )]
    database_url: Url,
}

#[derive(Queryable, Debug)]
struct OrderUpdateSummary {
    pub price: i64,
    pub quantity: i64,
    pub original_quantity: i64,
    pub status: String,
    pub timestamp: NaiveDateTime,
    pub checkpoint: i64,
    pub is_bid: bool,
}

#[derive(Queryable, Debug)]
struct OrderFillSummary {
    pub price: i64,
    pub quantity: i64,
    pub timestamp: NaiveDateTime,
    pub checkpoint: i64,
    pub taker_is_bid: bool,
}

#[derive(Debug)]
enum Op {
    Add,
    Subtract,
}

#[derive(Debug)]
struct OrderStep {
    pub price: i64,
    pub quantity: i64,
    pub op: Op,
    #[allow(dead_code)]
    pub checkpoint: i64,
    pub is_bid: bool,
    pub timestamp: NaiveDateTime,
}

#[derive(Debug)]
pub enum HistoricOrderbookError {
    StartGreaterThanEnd,
    FailedSerializeSide,
    NegativeOrder,
    Overlap,
    NoTimestampInRange,
    FailedReadingFromDatabase(diesel::result::Error),
}

fn status_to_operation(status: &str) -> Op {
    match status {
        "Placed" => Op::Add,
        "Canceled" => Op::Subtract,
        "Expired" => Op::Subtract,
        "Modified" => Op::Subtract,
        _ => {
            unreachable!("unknown status {}", status);
        }
    }
}

fn get_txs(
    pool_id: &str,
    start_checkpoint: i64,
    end_checkpoint: i64,
    mut conn: PgConnection,
) -> Result<(Vec<OrderStep>, Option<NaiveDateTime>), HistoricOrderbookError> {
    let updates: Vec<OrderStep> = schema::order_updates::table
        .filter(schema::order_updates::pool_id.eq(&pool_id))
        .filter(schema::order_updates::checkpoint.gt(start_checkpoint))
        .filter(schema::order_updates::checkpoint.le(end_checkpoint))
        .select((
            schema::order_updates::price,
            schema::order_updates::quantity,
            schema::order_updates::original_quantity,
            schema::order_updates::status,
            schema::order_updates::timestamp,
            schema::order_updates::checkpoint,
            schema::order_updates::is_bid,
        ))
        .load::<OrderUpdateSummary>(&mut conn)
        .map_err(|e| HistoricOrderbookError::FailedReadingFromDatabase(e))?
        .into_iter()
        .map(|u| OrderStep {
            price: u.price,
            quantity: if u.status == "Modified" {
                u.original_quantity - u.quantity
            } else {
                u.quantity
            },
            op: status_to_operation(u.status.as_str()),
            checkpoint: u.checkpoint,
            is_bid: u.is_bid,
            timestamp: u.timestamp,
        })
        .collect();
    let fills: Vec<OrderStep> = schema::order_fills::table
        .filter(schema::order_fills::pool_id.eq(&pool_id))
        .filter(schema::order_fills::checkpoint.gt(start_checkpoint))
        .filter(schema::order_fills::checkpoint.le(end_checkpoint))
        .select((
            schema::order_fills::price,
            schema::order_fills::base_quantity,
            schema::order_fills::timestamp,
            schema::order_fills::checkpoint,
            schema::order_fills::taker_is_bid,
        ))
        .load::<OrderFillSummary>(&mut conn)
        .map_err(|e| HistoricOrderbookError::FailedReadingFromDatabase(e))?
        .into_iter()
        .map(|u| OrderStep {
            price: u.price,
            quantity: u.quantity,
            op: Op::Subtract,
            checkpoint: u.checkpoint,
            is_bid: !u.taker_is_bid,
            timestamp: u.timestamp,
        })
        .collect();
    let mut combined = updates;
    combined.extend(fills);

    let max_timestamp = match combined.iter().map(|step| step.timestamp).max() {
        Some(v) => v,
        None => return Ok((vec![], None)),
    };

    info!(
        "Checkpoints {} - {} inclusive",
        start_checkpoint + 1,
        end_checkpoint
    );

    Ok((combined, Some(max_timestamp)))
}

fn has_overlap(asks: &HashMap<i64, i64>, bids: &HashMap<i64, i64>) -> bool {
    let max_bid = bids.keys().max();
    let min_ask = asks.keys().min();

    if let (Some(&bid), Some(&ask)) = (max_bid, min_ask) {
        if bid >= ask {
            println!("Orderbook overlap: max bid {} >= min ask {}", bid, ask);
            return true;
        }
    }
    false
}

fn values_from_orderbook_option(
    initial_orderbook: Option<OrderbookSnapshot>,
) -> (NaiveDateTime, i64, HashMap<i64, i64>, HashMap<i64, i64>) {
    if let Some(ob) = initial_orderbook {
        return (
            ob.timestamp,
            ob.checkpoint,
            serde_json::from_value(ob.asks).expect("failed parsing asks"),
            serde_json::from_value(ob.bids).expect("failed parsing bids"),
        );
    }
    (
        NaiveDateTime::parse_from_str("2024-10-13 00:00:00", "%Y-%m-%d %H:%M:%S")
            .expect("failed parsing initial time"),
        -1,
        HashMap::new(),
        HashMap::new(),
    )
}

pub fn get_latest_snapshot(
    conn: &mut PgConnection,
    target_pool_id: &str,
) -> Result<Option<OrderbookSnapshot>, diesel::result::Error> {
    match schema::orderbook_snapshots::table
        .filter(schema::orderbook_snapshots::pool_id.eq(target_pool_id))
        .order(schema::orderbook_snapshots::checkpoint.desc())
        .first::<OrderbookSnapshot>(conn)
    {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(diesel::result::Error::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn get_historic_orderbook(
    database_url: Url,
    pool_id: &str,
    end_checkpoint: i64,
) -> Result<OrderbookSnapshot, HistoricOrderbookError> {
    let mut conn = PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB");

    let last_snapshot =
        get_latest_snapshot(&mut conn, pool_id).expect("failed getting last snapshot");

    let (current_time, start_checkpoint, mut asks, mut bids) =
        values_from_orderbook_option(last_snapshot);

    if start_checkpoint >= end_checkpoint {
        return Err(HistoricOrderbookError::StartGreaterThanEnd);
    }

    let (orders, ts) = get_txs(
        pool_id,
        start_checkpoint,
        end_checkpoint,
        PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB"),
    )?;

    let timestamp = match ts {
        Some(t) => t,
        None => current_time,
    };

    for order in orders {
        let side = if order.is_bid { &mut bids } else { &mut asks };

        side.entry(order.price)
            .and_modify(|q| match order.op {
                Op::Add => *q += order.quantity,
                Op::Subtract => *q -= order.quantity,
            })
            .or_insert_with(|| match order.op {
                Op::Add => order.quantity,
                Op::Subtract => -order.quantity,
            });
    }

    bids.retain(|_, &mut qty| qty != 0);
    asks.retain(|_, &mut qty| qty != 0);

    for (price, qty) in bids.iter().filter(|(_, q)| q < &&0) {
        warn!(
            "Negative bid at price {}: {}, pool id {}",
            price, qty, pool_id
        );
        return Err(HistoricOrderbookError::NegativeOrder);
    }

    for (price, qty) in asks.iter().filter(|(_, q)| q < &&0) {
        warn!(
            "Negative ask at price {}: {}, pool id {}",
            price, qty, pool_id
        );
        return Err(HistoricOrderbookError::NegativeOrder);
    }

    if has_overlap(&asks, &bids) {
        warn!("Orderbook {} has overlap", pool_id);
        return Err(HistoricOrderbookError::Overlap);
    }

    let asks_serde = match serde_json::to_value(&asks) {
        Ok(v) => v,
        Err(_) => return Err(HistoricOrderbookError::FailedSerializeSide),
    };
    let bids_serde = match serde_json::to_value(&bids) {
        Ok(v) => v,
        Err(_) => return Err(HistoricOrderbookError::FailedSerializeSide),
    };

    Ok(OrderbookSnapshot {
        checkpoint: end_checkpoint,
        pool_id: pool_id.to_string(),
        asks: asks_serde,
        bids: bids_serde,
        timestamp,
    })
}

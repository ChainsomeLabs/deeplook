use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use clap::Parser;
use diesel::{Connection, PgConnection, Queryable};
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
    pub checkpoint: i64,
    pub is_bid: bool,
    pub timestamp: NaiveDateTime,
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
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    start_checkpoint: i64,
    mut conn: PgConnection,
) -> Result<(Vec<OrderStep>, i64, NaiveDateTime), anyhow::Error> {
    let updates: Vec<OrderStep> = schema::order_updates::table
        .filter(schema::order_updates::pool_id.eq(&pool_id))
        .filter(schema::order_updates::timestamp.between(start_time, end_time))
        .filter(schema::order_updates::checkpoint.gt(start_checkpoint))
        .select((
            schema::order_updates::price,
            schema::order_updates::quantity,
            schema::order_updates::status,
            schema::order_updates::timestamp,
            schema::order_updates::checkpoint,
            schema::order_updates::is_bid,
        ))
        .load::<OrderUpdateSummary>(&mut conn)?
        .into_iter()
        .map(|u| OrderStep {
            price: u.price,
            quantity: u.quantity,
            op: status_to_operation(u.status.as_str()),
            checkpoint: u.checkpoint,
            is_bid: u.is_bid,
            timestamp: u.timestamp,
        })
        .collect();
    let fills: Vec<OrderStep> = schema::order_fills::table
        .filter(schema::order_fills::pool_id.eq(&pool_id))
        .filter(schema::order_fills::timestamp.between(start_time, end_time))
        .filter(schema::order_fills::checkpoint.gt(start_checkpoint))
        .select((
            schema::order_fills::price,
            schema::order_fills::base_quantity,
            schema::order_fills::timestamp,
            schema::order_fills::checkpoint,
            schema::order_fills::taker_is_bid,
        ))
        .load::<OrderFillSummary>(&mut conn)?
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

    let max_checkpoint = combined
        .iter()
        .map(|step| step.checkpoint)
        .max()
        .expect("No checkpoints");

    // this will discard max_checkpoint orders since we cannot be certain
    // that all orders from max_checkpoint are included
    combined.retain(|o| o.checkpoint < max_checkpoint);

    let max_timestamp = combined
        .iter()
        .map(|step| step.timestamp)
        .max()
        .expect("No timestamp");

    println!(
        "Checkpoints {} - {} inclusive",
        start_checkpoint + 1,
        max_checkpoint - 1
    );

    Ok((combined, max_checkpoint - 1, max_timestamp))
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
        NaiveDateTime::parse_from_str("2024-10-15 00:00:00", "%Y-%m-%d %H:%M:%S")
            .expect("failed parsing initial time"),
        -1,
        HashMap::new(),
        HashMap::new(),
    )
}

pub fn get_historic_orderbook(
    database_url: Url,
    pool_id: &str,
    end: NaiveDateTime,
    initial_orderbook: Option<OrderbookSnapshot>,
) -> Result<OrderbookSnapshot, anyhow::Error> {
    let offset = Duration::minutes(30);
    let period = Duration::days(1);
    let (mut current_time, mut current_checkpoint, mut asks, mut bids) =
        values_from_orderbook_option(initial_orderbook);
    let mut timestamp = None;

    while current_time < end {
        let call_start = current_time - offset;
        let call_end = current_time + period + offset;

        let (orders, checkpoint, ts) = get_txs(
            pool_id,
            call_start,
            call_end,
            current_checkpoint,
            PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB"),
        )
        .expect("failed getting txs");

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

        current_checkpoint = checkpoint;
        timestamp = Some(ts);

        current_time += period;
    }

    bids.retain(|_, &mut qty| qty != 0);
    asks.retain(|_, &mut qty| qty != 0);

    for (price, qty) in bids.iter().filter(|(_, q)| q < &&0) {
        panic!(
            "Negative bid at price {}: {}, pool id {}",
            price, qty, pool_id
        );
    }

    for (price, qty) in asks.iter().filter(|(_, q)| q < &&0) {
        panic!(
            "Negative ask at price {}: {}, pool id {}",
            price, qty, pool_id
        );
    }

    if has_overlap(&asks, &bids) {
        panic!("Orderbook {} has overlap", pool_id);
    }

    Ok(OrderbookSnapshot {
        checkpoint: current_checkpoint,
        pool_id: pool_id.to_string(),
        asks: serde_json::to_value(&asks)?,
        bids: serde_json::to_value(&bids)?,
        timestamp: timestamp.unwrap(),
    })
}

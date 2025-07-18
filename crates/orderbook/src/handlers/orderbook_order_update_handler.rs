use crate::OrderbookManagerMap;
use crate::handlers::{is_deepbook_tx, try_extract_move_call_package};

use deeplook_indexer::DeeplookEnv;
use deeplook_indexer::models::deepbook::order::{OrderCanceled, OrderModified};
use deeplook_indexer::models::deepbook::order_info::{OrderExpired, OrderFilled, OrderPlaced};
use deeplook_indexer::utils::ms_to_secs;
use deeplook_schema::models::{OrderFill, OrderUpdate, OrderUpdateStatus};
use move_core_types::language_storage::StructTag;
use std::collections::HashMap;
use std::sync::Arc;
use sui_indexer_alt_framework::db::{Connection, Db};
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_types::full_checkpoint_content::CheckpointData;

type TransactionMetadata = (String, u64, u64, String, String);

pub struct OrderbookOrderUpdateHandler {
    order_placed_type: StructTag,
    order_modified_type: StructTag,
    order_canceled_type: StructTag,
    order_expired_type: StructTag,
    order_filled_type: StructTag,
    orderbook_managers: Arc<OrderbookManagerMap>,
}

impl OrderbookOrderUpdateHandler {
    pub fn new(env: DeeplookEnv, orderbook_managers: Arc<OrderbookManagerMap>) -> Self {
        Self {
            order_placed_type: env.order_placed_event_type(),
            order_modified_type: env.order_modified_event_type(),
            order_canceled_type: env.order_canceled_event_type(),
            order_expired_type: env.order_expired_event_type(),
            order_filled_type: env.order_filled_event_type(),
            orderbook_managers,
        }
    }
}

impl Processor for OrderbookOrderUpdateHandler {
    const NAME: &'static str = "order_update";
    type Value = OrderUpdate;
    fn process(&self, checkpoint: &Arc<CheckpointData>) -> anyhow::Result<Vec<Self::Value>> {
        let mut updates: HashMap<String, Vec<OrderUpdate>> = HashMap::new();
        let mut fills: HashMap<String, Vec<OrderFill>> = HashMap::new();

        for tx in checkpoint.transactions.iter() {
            if !is_deepbook_tx(tx) {
                continue;
            }
            let Some(events) = &tx.events else {
                continue;
            };

            let package = try_extract_move_call_package(tx).unwrap_or_default();
            let metadata = (
                tx.transaction.sender_address().to_string(),
                checkpoint.checkpoint_summary.sequence_number,
                checkpoint.checkpoint_summary.timestamp_ms,
                tx.transaction.digest().to_string(),
                package.clone(),
            );

            for (index, ev) in events.data.iter().enumerate() {
                if ev.type_ == self.order_placed_type {
                    if let Ok(event) = bcs::from_bytes(&ev.contents) {
                        let order_update = process_order_placed(event, metadata.clone(), index);

                        updates
                            .entry(order_update.pool_id.clone())
                            .or_insert_with(Vec::new)
                            .push(order_update);
                    }
                } else if ev.type_ == self.order_modified_type {
                    if let Ok(event) = bcs::from_bytes(&ev.contents) {
                        let order_update = process_order_modified(event, metadata.clone(), index);

                        updates
                            .entry(order_update.pool_id.clone())
                            .or_insert_with(Vec::new)
                            .push(order_update);
                    }
                } else if ev.type_ == self.order_canceled_type {
                    if let Ok(event) = bcs::from_bytes(&ev.contents) {
                        let order_update = process_order_canceled(event, metadata.clone(), index);

                        updates
                            .entry(order_update.pool_id.clone())
                            .or_insert_with(Vec::new)
                            .push(order_update);
                    }
                } else if ev.type_ == self.order_expired_type {
                    if let Ok(event) = bcs::from_bytes(&ev.contents) {
                        let order_update = process_order_expired(event, metadata.clone(), index);

                        updates
                            .entry(order_update.pool_id.clone())
                            .or_insert_with(Vec::new)
                            .push(order_update);
                    }
                } else if ev.type_ == self.order_filled_type {
                    if let Ok(event) = bcs::from_bytes(&ev.contents) {
                        let order_filled = process_order_filled(event, metadata.clone(), index);

                        fills
                            .entry(order_filled.pool_id.clone())
                            .or_insert_with(Vec::new)
                            .push(order_filled);
                    }
                }
            }
        }

        for (pool_id, orders) in updates {
            if let Some(ob_m) = self.orderbook_managers.get(&pool_id) {
                if let Ok(mut locked) = ob_m.lock() {
                    locked.handle_update_multiple(orders);
                }
            }
        }
        for (pool_id, orders) in fills {
            if let Some(ob_m) = self.orderbook_managers.get(&pool_id) {
                if let Ok(mut locked) = ob_m.lock() {
                    locked.handle_fill_multiple(orders);
                }
            }
        }
        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl Handler for OrderbookOrderUpdateHandler {
    type Store = Db;

    async fn commit<'a>(
        _values: &[Self::Value],
        _conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        // do not store in db
        Ok(0)
    }
}

fn process_order_placed(
    order_placed: OrderPlaced,
    (sender, checkpoint, checkpoint_timestamp_ms, digest, package): TransactionMetadata,
    event_index: usize,
) -> OrderUpdate {
    let event_digest = format!("{digest}{event_index}");
    OrderUpdate {
        event_digest,
        digest,
        sender,
        checkpoint: checkpoint as i64,
        checkpoint_timestamp_ms: checkpoint_timestamp_ms as i64,
        timestamp: ms_to_secs(checkpoint_timestamp_ms as i64),
        package,
        status: OrderUpdateStatus::Placed,
        pool_id: order_placed.pool_id.to_string(),
        order_id: order_placed.order_id.to_string(),
        client_order_id: order_placed.client_order_id as i64,
        price: order_placed.price as i64,
        is_bid: order_placed.is_bid,
        onchain_timestamp: order_placed.timestamp as i64,
        original_quantity: order_placed.placed_quantity as i64,
        quantity: order_placed.placed_quantity as i64,
        filled_quantity: 0,
        trader: order_placed.trader.to_string(),
        balance_manager_id: order_placed.balance_manager_id.to_string(),
    }
}

fn process_order_modified(
    order_modified: OrderModified,
    (sender, checkpoint, checkpoint_timestamp_ms, digest, package): TransactionMetadata,
    event_index: usize,
) -> OrderUpdate {
    let event_digest = format!("{digest}{event_index}");
    OrderUpdate {
        digest,
        event_digest,
        sender,
        checkpoint: checkpoint as i64,
        checkpoint_timestamp_ms: checkpoint_timestamp_ms as i64,
        timestamp: ms_to_secs(checkpoint_timestamp_ms as i64),
        package,
        status: OrderUpdateStatus::Modified,
        pool_id: order_modified.pool_id.to_string(),
        order_id: order_modified.order_id.to_string(),
        client_order_id: order_modified.client_order_id as i64,
        price: order_modified.price as i64,
        is_bid: order_modified.is_bid,
        onchain_timestamp: order_modified.timestamp as i64,
        original_quantity: order_modified.previous_quantity as i64,
        quantity: order_modified.new_quantity as i64,
        filled_quantity: order_modified.filled_quantity as i64,
        trader: order_modified.trader.to_string(),
        balance_manager_id: order_modified.balance_manager_id.to_string(),
    }
}

fn process_order_canceled(
    order_canceled: OrderCanceled,
    (sender, checkpoint, checkpoint_timestamp_ms, digest, package): TransactionMetadata,
    event_index: usize,
) -> OrderUpdate {
    let event_digest = format!("{digest}{event_index}");
    OrderUpdate {
        digest,
        event_digest,
        sender,
        checkpoint: checkpoint as i64,
        checkpoint_timestamp_ms: checkpoint_timestamp_ms as i64,
        timestamp: ms_to_secs(checkpoint_timestamp_ms as i64),
        package,
        status: OrderUpdateStatus::Canceled,
        pool_id: order_canceled.pool_id.to_string(),
        order_id: order_canceled.order_id.to_string(),
        client_order_id: order_canceled.client_order_id as i64,
        price: order_canceled.price as i64,
        is_bid: order_canceled.is_bid,
        onchain_timestamp: order_canceled.timestamp as i64,
        original_quantity: order_canceled.original_quantity as i64,
        quantity: order_canceled.base_asset_quantity_canceled as i64,
        filled_quantity: (order_canceled.original_quantity
            - order_canceled.base_asset_quantity_canceled) as i64,
        trader: order_canceled.trader.to_string(),
        balance_manager_id: order_canceled.balance_manager_id.to_string(),
    }
}

fn process_order_expired(
    order_expired: OrderExpired,
    (sender, checkpoint, checkpoint_timestamp_ms, digest, package): TransactionMetadata,
    event_index: usize,
) -> OrderUpdate {
    let event_digest = format!("{digest}{event_index}");
    OrderUpdate {
        digest,
        event_digest,
        sender,
        checkpoint: checkpoint as i64,
        checkpoint_timestamp_ms: checkpoint_timestamp_ms as i64,
        timestamp: ms_to_secs(checkpoint_timestamp_ms as i64),
        package,
        status: OrderUpdateStatus::Expired,
        pool_id: order_expired.pool_id.to_string(),
        order_id: order_expired.order_id.to_string(),
        client_order_id: order_expired.client_order_id as i64,
        price: order_expired.price as i64,
        is_bid: order_expired.is_bid,
        onchain_timestamp: order_expired.timestamp as i64,
        original_quantity: order_expired.original_quantity as i64,
        quantity: order_expired.base_asset_quantity_canceled as i64,
        filled_quantity: (order_expired.original_quantity
            - order_expired.base_asset_quantity_canceled) as i64,
        trader: order_expired.trader.to_string(),
        balance_manager_id: order_expired.balance_manager_id.to_string(),
    }
}

fn process_order_filled(
    order_filled: OrderFilled,
    (sender, checkpoint, checkpoint_timestamp_ms, digest, package): TransactionMetadata,
    event_index: usize,
) -> OrderFill {
    let event_digest = format!("{digest}{event_index}");
    OrderFill {
        digest,
        event_digest,
        sender,
        checkpoint: checkpoint as i64,
        checkpoint_timestamp_ms: checkpoint_timestamp_ms as i64,
        timestamp: ms_to_secs(checkpoint_timestamp_ms as i64),
        package,
        pool_id: order_filled.pool_id.to_string(),
        maker_order_id: order_filled.maker_order_id.to_string(),
        taker_order_id: order_filled.taker_order_id.to_string(),
        maker_client_order_id: order_filled.maker_client_order_id as i64,
        taker_client_order_id: order_filled.taker_client_order_id as i64,
        price: order_filled.price as i64,
        taker_is_bid: order_filled.taker_is_bid,
        taker_fee: order_filled.taker_fee as i64,
        taker_fee_is_deep: order_filled.taker_fee_is_deep,
        maker_fee: order_filled.maker_fee as i64,
        maker_fee_is_deep: order_filled.maker_fee_is_deep,
        base_quantity: order_filled.base_quantity as i64,
        quote_quantity: order_filled.quote_quantity as i64,
        maker_balance_manager_id: order_filled.maker_balance_manager_id.to_string(),
        taker_balance_manager_id: order_filled.taker_balance_manager_id.to_string(),
        onchain_timestamp: order_filled.timestamp as i64,
    }
}

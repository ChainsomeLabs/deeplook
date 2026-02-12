use crate::OrderbookManagerMap;

use async_trait::async_trait;
use deeplook_indexer::DeepbookEnv;
use deeplook_indexer::handlers::order_fill_handler::OrderFillHandler;
use deeplook_indexer::handlers::order_update_handler::OrderUpdateHandler;
use deeplook_schema::models::{OrderFill, OrderUpdate};
use std::collections::HashMap;
use std::sync::Arc;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::postgres::Connection;
use sui_indexer_alt_framework::postgres::handler::Handler as PgHandler;
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

pub struct OrderbookOrderUpdateHandler {
    update_handler: OrderUpdateHandler,
    fill_handler: OrderFillHandler,
    orderbook_managers: Arc<OrderbookManagerMap>,
}

impl OrderbookOrderUpdateHandler {
    pub fn new(env: DeepbookEnv, orderbook_managers: Arc<OrderbookManagerMap>) -> Self {
        Self {
            update_handler: OrderUpdateHandler::new(env),
            fill_handler: OrderFillHandler::new(env),
            orderbook_managers,
        }
    }
}

#[async_trait]
impl Processor for OrderbookOrderUpdateHandler {
    const NAME: &'static str = "order_update";
    type Value = OrderUpdate;
    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let updates = self.update_handler.process(checkpoint).await?;
        let fills = self.fill_handler.process(checkpoint).await?;

        let mut updates_by_pool: HashMap<String, Vec<OrderUpdate>> = HashMap::new();
        let mut fills_by_pool: HashMap<String, Vec<OrderFill>> = HashMap::new();

        for update in updates {
            updates_by_pool
                .entry(update.pool_id.clone())
                .or_insert_with(Vec::new)
                .push(update);
        }

        for fill in fills {
            fills_by_pool
                .entry(fill.pool_id.clone())
                .or_insert_with(Vec::new)
                .push(fill);
        }

        let mut all_pool_ids: Vec<String> = updates_by_pool
            .keys()
            .chain(fills_by_pool.keys())
            .cloned()
            .collect();
        all_pool_ids.sort();
        all_pool_ids.dedup();

        for pool_id in all_pool_ids {
            let updates: Vec<OrderUpdate> =
                updates_by_pool.remove(&pool_id).unwrap_or_else(Vec::new);
            let fills: Vec<OrderFill> = fills_by_pool.remove(&pool_id).unwrap_or_else(Vec::new);
            if let Some(ob_m) = self.orderbook_managers.get(&pool_id) {
                if let Ok(mut locked) = ob_m.lock() {
                    locked.handle_batch(updates, fills);
                }
            }
        }

        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl PgHandler for OrderbookOrderUpdateHandler {
    async fn commit<'a>(
        _values: &[Self::Value],
        _conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        // do not store in db
        Ok(0)
    }
}

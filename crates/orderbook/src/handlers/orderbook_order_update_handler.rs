use crate::OrderbookManagerMap;
use crate::runtime_store::RuntimeStore;

use async_trait::async_trait;
use deeplook_indexer::DeepbookEnv;
use deeplook_indexer::handlers::order_fill_handler::OrderFillHandler;
use deeplook_indexer::handlers::order_update_handler::OrderUpdateHandler;
use deeplook_schema::models::{OrderFill, OrderUpdate};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sui_indexer_alt_framework::pipeline::{Processor, sequential};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

pub struct PoolOrderbookEvents {
    checkpoint: u64,
    pool_id: String,
    updates: Vec<OrderUpdate>,
    fills: Vec<OrderFill>,
}

pub struct OrderbookOrderUpdateHandler {
    update_handler: OrderUpdateHandler,
    fill_handler: OrderFillHandler,
    orderbook_managers: Arc<OrderbookManagerMap>,
    // Keep track of the highest fully applied checkpoint to make keep_up idempotent
    // across sequential committer retries.
    last_applied_checkpoint: Mutex<Option<u64>>,
}

impl OrderbookOrderUpdateHandler {
    pub fn new(env: DeepbookEnv, orderbook_managers: Arc<OrderbookManagerMap>) -> Self {
        Self {
            update_handler: OrderUpdateHandler::new(env),
            fill_handler: OrderFillHandler::new(env),
            orderbook_managers,
            last_applied_checkpoint: Mutex::new(None),
        }
    }

    fn apply_pool_events(&self, pool_id: &str, updates: Vec<OrderUpdate>, fills: Vec<OrderFill>) {
        if let Some(ob_m) = self.orderbook_managers.get(pool_id) {
            if let Ok(mut locked) = ob_m.lock() {
                locked.handle_batch(updates, fills);
            }
        }
    }

    fn apply_in_commit_order(&self, values: Vec<PoolOrderbookEvents>) {
        let mut checkpoint_guard = match self.last_applied_checkpoint.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };

        let mut checkpoint_in_progress: Option<u64> = None;

        for value in values {
            if checkpoint_guard.is_some_and(|last| value.checkpoint <= last) {
                continue;
            }

            if checkpoint_in_progress != Some(value.checkpoint) {
                if let Some(done_checkpoint) = checkpoint_in_progress {
                    *checkpoint_guard = Some(done_checkpoint);
                }
                checkpoint_in_progress = Some(value.checkpoint);
            }

            self.apply_pool_events(&value.pool_id, value.updates, value.fills);
        }

        if let Some(done_checkpoint) = checkpoint_in_progress {
            *checkpoint_guard = Some(done_checkpoint);
        }
    }
}

#[async_trait]
impl Processor for OrderbookOrderUpdateHandler {
    const NAME: &'static str = "order_update";
    type Value = PoolOrderbookEvents;
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

        let mut values = Vec::with_capacity(all_pool_ids.len());
        for pool_id in all_pool_ids {
            values.push(PoolOrderbookEvents {
                checkpoint: checkpoint.summary.sequence_number,
                updates: updates_by_pool.remove(&pool_id).unwrap_or_else(Vec::new),
                fills: fills_by_pool.remove(&pool_id).unwrap_or_else(Vec::new),
                pool_id,
            });
        }

        Ok(values)
    }
}

#[async_trait]
impl sequential::Handler for OrderbookOrderUpdateHandler {
    type Store = RuntimeStore;
    type Batch = Mutex<Vec<PoolOrderbookEvents>>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        if let Ok(mut locked) = batch.lock() {
            locked.extend(values);
        }
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        _conn: &mut <Self::Store as sui_indexer_alt_framework::store::Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        let values = if let Ok(mut locked) = batch.lock() {
            std::mem::take(&mut *locked)
        } else {
            vec![]
        };
        self.apply_in_commit_order(values);
        Ok(0)
    }
}

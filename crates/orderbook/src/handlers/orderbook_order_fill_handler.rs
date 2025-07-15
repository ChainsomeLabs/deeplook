use async_trait::async_trait;
use deeplook_indexer::models::deepbook::order_info::OrderFilled;
use deeplook_indexer::{ utils::ms_to_secs, DeeplookEnv };
use deeplook_schema::models::OrderFill;
use move_core_types::language_storage::StructTag;
use sui_indexer_alt_framework::db::{ Connection, Db };
use std::sync::Arc;
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_types::full_checkpoint_content::CheckpointData;

use crate::OrderbookManagerMap;
use crate::{ handlers::{ is_deepbook_tx, try_extract_move_call_package } };

pub struct OrderbookOrderFillHandler {
    event_type: StructTag,
    orderbook_managers: Arc<OrderbookManagerMap>,
}

impl OrderbookOrderFillHandler {
    pub fn new(env: DeeplookEnv, orderbook_managers: Arc<OrderbookManagerMap>) -> Self {
        Self {
            event_type: env.order_filled_event_type(),
            orderbook_managers,
        }
    }
}

impl Processor for OrderbookOrderFillHandler {
    const NAME: &'static str = "order_fill";
    type Value = OrderFill;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> anyhow::Result<Vec<Self::Value>> {
        checkpoint.transactions.iter().try_fold(vec![], |result, tx| {
            if !is_deepbook_tx(tx) {
                return Ok(result);
            }
            let Some(events) = &tx.events else {
                return Ok(result);
            };

            let package = try_extract_move_call_package(tx).unwrap_or_default();
            let checkpoint_timestamp_ms = checkpoint.checkpoint_summary.timestamp_ms as i64;
            let checkpoint = checkpoint.checkpoint_summary.sequence_number as i64;
            let digest = tx.transaction.digest();

            let result: Result<Vec<OrderFill>, bcs::Error> = events.data
                .iter()
                .filter(|ev| ev.type_ == self.event_type)
                .enumerate()
                .try_fold(result, |mut result, (index, ev)| {
                    let event: OrderFilled = bcs::from_bytes(&ev.contents)?;
                    let data = OrderFill {
                        digest: digest.to_string(),
                        event_digest: format!("{digest}{index}"),
                        sender: tx.transaction.sender_address().to_string(),
                        checkpoint,
                        checkpoint_timestamp_ms,
                        timestamp: ms_to_secs(checkpoint_timestamp_ms),
                        package: package.clone(),
                        pool_id: event.pool_id.to_string(),
                        maker_order_id: event.maker_order_id.to_string(),
                        taker_order_id: event.taker_order_id.to_string(),
                        maker_client_order_id: event.maker_client_order_id as i64,
                        taker_client_order_id: event.taker_client_order_id as i64,
                        price: event.price as i64,
                        taker_is_bid: event.taker_is_bid,
                        taker_fee: event.taker_fee as i64,
                        taker_fee_is_deep: event.taker_fee_is_deep,
                        maker_fee: event.maker_fee as i64,
                        maker_fee_is_deep: event.maker_fee_is_deep,
                        base_quantity: event.base_quantity as i64,
                        quote_quantity: event.quote_quantity as i64,
                        maker_balance_manager_id: event.maker_balance_manager_id.to_string(),
                        taker_balance_manager_id: event.taker_balance_manager_id.to_string(),
                        onchain_timestamp: event.timestamp as i64,
                    };
                    result.push(data);
                    Ok(result)
                });

            if let Ok(fills) = result {
                if fills.len() > 0 {
                    for fill in fills {
                        if let Some(ob_m) = self.orderbook_managers.get(&fill.pool_id) {
                            if let Ok(mut locked) = ob_m.lock() {
                                locked.handle_fill(fill);
                            }
                        }
                    }
                }
            }
            Ok(vec![])
        })
    }
}

#[async_trait]
impl Handler for OrderbookOrderFillHandler {
    type Store = Db;

    async fn commit<'a>(
        _values: &[Self::Value],
        _conn: &mut Connection<'a>
    ) -> anyhow::Result<usize> {
        // do not store in db
        Ok(0)
    }
}

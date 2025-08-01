use crate::handlers::{is_deepbook_tx, try_extract_move_call_package};
use crate::models::deepbook::state::RebateEvent;
use crate::utils::ms_to_secs;
use crate::DeeplookEnv;
use async_trait::async_trait;
use deeplook_schema::models::Rebates;
use deeplook_schema::schema::rebates;
use diesel_async::RunQueryDsl;
use move_core_types::language_storage::StructTag;
use std::sync::Arc;
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_pg_db::{Connection, Db};
use sui_types::full_checkpoint_content::CheckpointData;
use tracing::debug;

pub struct RebatesHandler {
    event_type: StructTag,
}

impl RebatesHandler {
    pub fn new(env: DeeplookEnv) -> Self {
        Self {
            event_type: env.rebate_event_type(),
        }
    }
}

impl Processor for RebatesHandler {
    const NAME: &'static str = "rebates";
    type Value = Rebates;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> anyhow::Result<Vec<Self::Value>> {
        checkpoint
            .transactions
            .iter()
            .try_fold(vec![], |result, tx| {
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

                return events
                    .data
                    .iter()
                    .filter(|ev| ev.type_ == self.event_type)
                    .enumerate()
                    .try_fold(result, |mut result, (index, ev)| {
                        let event: RebateEvent = bcs::from_bytes(&ev.contents)?;
                        let data = Rebates {
                            digest: digest.to_string(),
                            event_digest: format!("{digest}{index}"),
                            sender: tx.transaction.sender_address().to_string(),
                            checkpoint,
                            checkpoint_timestamp_ms,
                            timestamp: ms_to_secs(checkpoint_timestamp_ms),
                            package: package.clone(),
                            pool_id: event.pool_id.to_string(),
                            balance_manager_id: event.balance_manager_id.to_string(),
                            epoch: event.epoch as i64,
                            claim_amount: event.claim_amount as i64,
                        };
                        debug!("Observed Deepbook Rebate Event {:?}", data);
                        result.push(data);
                        Ok(result)
                    });
            })
    }
}

#[async_trait]
impl Handler for RebatesHandler {
    type Store = Db;

    async fn commit<'a>(
        values: &[Self::Value],
        conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        Ok(diesel::insert_into(rebates::table)
            .values(values)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?)
    }
}

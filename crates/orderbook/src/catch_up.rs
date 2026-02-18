use std::{net::SocketAddr, sync::Arc};

use tracing::info;
use url::Url;

use crate::{OrderbookManagerMap, replay};

/// Takes orderbook managers and quickly catches up to the latest checkpoint
/// using events already written by the indexer in Postgres.
pub async fn catch_up(
    database_url: Url,
    _metrics_address: SocketAddr,
    orderbook_managers: Arc<OrderbookManagerMap>,
    end: u64,
) -> Result<u64, anyhow::Error> {
    let mut conn = replay::establish_connection(&database_url)?;
    let pool_ids = replay::get_unique_pool_ids(&orderbook_managers);
    let start_checkpoint = replay::get_lowest_manager_checkpoint(&orderbook_managers)?;

    let replay_hi = replay::get_replay_upper_checkpoint(&mut conn)?;
    let target_checkpoint = match replay_hi {
        Some(v) => v.min(end as i64),
        None => start_checkpoint,
    };

    if target_checkpoint <= start_checkpoint {
        info!(
            "No catch_up work: start_checkpoint={}, replay_hi={:?}, requested_end={}",
            start_checkpoint, replay_hi, end
        );
        return Ok(start_checkpoint as u64);
    }

    let mut range_start = start_checkpoint + 1;
    while range_start <= target_checkpoint {
        let range_end = (range_start + replay::REPLAY_BATCH_SIZE - 1).min(target_checkpoint);
        replay::apply_range(
            &mut conn,
            &orderbook_managers,
            &pool_ids,
            range_start,
            range_end,
        )?;
        range_start = range_end + 1;
    }

    Ok(target_checkpoint as u64)
}

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use deeplook_indexer::{DeeplookEnv, MAINNET_REMOTE_STORE_URL};
use prometheus::Registry;
use sui_indexer_alt_framework::{
    Indexer, IndexerArgs,
    ingestion::ClientArgs,
    postgres::{Db, DbArgs},
};
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService, db::DbConnectionStatsCollector};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    OrderbookManagerMap, handlers::orderbook_order_update_handler::OrderbookOrderUpdateHandler,
};

/// Takes orderbook managers and quickly catches up to the latest checkpoint
/// using batch indexing, which is fast, but may be out of order.
pub async fn catch_up(
    database_url: Url,
    metrics_address: SocketAddr,
    orderbook_managers: Arc<OrderbookManagerMap>,
    end: u64,
) -> Result<(), anyhow::Error> {
    let registry = Registry::new_custom(Some("deeplook".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let cancel = CancellationToken::new();
    let metrics = MetricsService::new(
        MetricsArgs { metrics_address },
        registry.clone(),
        cancel.child_token(),
    );

    // Prepare the store for the indexer
    let store = Db::for_write(database_url, DbArgs::default())
        .await
        .context("Failed to connect to database")?;

    registry.register(Box::new(DbConnectionStatsCollector::new(
        Some("deepbook_indexer_db"),
        store.clone(),
    )))?;

    let lowest_checkpoint = orderbook_managers
        .values()
        .filter_map(|arc_mutex| {
            arc_mutex
                .lock()
                .ok()
                .map(|ob_mngr| ob_mngr.initial_checkpoint)
        })
        .min()
        .expect("failed getting starting checkpoint") as u64;

    let mut indexer = Indexer::new(
        store,
        IndexerArgs {
            first_checkpoint: Some(lowest_checkpoint + 1),
            last_checkpoint: Some(end),
            pipeline: vec![],
            skip_watermark: true,
        },
        ClientArgs {
            remote_store_url: Some(Url::parse(MAINNET_REMOTE_STORE_URL).unwrap()),
            local_ingestion_path: None,
            rpc_api_url: None,
            rpc_username: None,
            rpc_password: None,
        },
        Default::default(),
        metrics.registry(),
        cancel.clone(),
    )
    .await?;

    indexer
        .concurrent_pipeline(
            OrderbookOrderUpdateHandler::new(DeeplookEnv::Mainnet, orderbook_managers),
            Default::default(),
        )
        .await?;

    let h_indexer = indexer.run().await?;

    // TODO: metrics are disabled, since it makes more sense to only monitor long running keep_up
    // and mixing together two different metrics is a bad idea (?)
    // let h_metrics = metrics.run().await?;

    let _ = h_indexer.await;
    cancel.cancel();
    // let _ = h_metrics.await;

    Ok(())
}

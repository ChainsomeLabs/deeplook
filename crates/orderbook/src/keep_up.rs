use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use deeplook_indexer::{DeeplookEnv, MAINNET_REMOTE_STORE_URL};
use prometheus::Registry;
use sui_indexer_alt_framework::{
    Indexer, IndexerArgs,
    db::DbArgs,
    ingestion::{ClientArgs, IngestionConfig},
};
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

use crate::{
    OrderbookManagerMap, handlers::orderbook_order_update_handler::OrderbookOrderUpdateHandler,
};

/// Takes orderbook managers, that are caught up, and keeps them
/// up to date indexing checkpoints one at a time to make sure
/// orderbooks are always correct.
pub async fn keep_up(
    database_url: Url,
    metrics_address: SocketAddr,
    orderbook_managers: Arc<OrderbookManagerMap>,
    start: u64,
) -> Result<(), anyhow::Error> {
    let registry = Registry::new_custom(Some("deeplook".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let cancel = CancellationToken::new();
    let metrics = MetricsService::new(
        MetricsArgs { metrics_address },
        registry,
        cancel.child_token(),
    );

    let mut indexer = Indexer::new(
        database_url,
        DbArgs::default(),
        IndexerArgs {
            first_checkpoint: Some(start),
            last_checkpoint: None,
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
        IngestionConfig {
            checkpoint_buffer_size: 5000,
            ingest_concurrency: 1,
            retry_interval_ms: 200,
        },
        None,
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

    info!("keeping up from {}", start);

    let h_indexer = indexer.run().await?;
    let h_metrics = metrics.run().await?;

    let _ = h_indexer.await;
    cancel.cancel();
    let _ = h_metrics.await;

    Ok(())
}

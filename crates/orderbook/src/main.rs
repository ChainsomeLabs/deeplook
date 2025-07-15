use anyhow::Context;
use clap::Parser;
use deeplook_cache::Cache;
use deeplook_indexer::{DeeplookEnv, MAINNET_REMOTE_STORE_URL};
use deeplook_orderbook::OrderbookManagerMap;
use deeplook_orderbook::checkpoint::CheckpointDigest;
use deeplook_orderbook::handlers::orderbook_order_fill_handler::OrderbookOrderFillHandler;
use deeplook_orderbook::handlers::orderbook_order_update_handler::OrderbookOrderUpdateHandler;
use deeplook_orderbook::orderbook::OrderbookManager;
use diesel::{Connection, PgConnection, RunQueryDsl};

use prometheus::Registry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use sui_indexer_alt_framework::db::DbArgs;
use sui_indexer_alt_framework::ingestion::ClientArgs;
use sui_indexer_alt_framework::{Indexer, IndexerArgs};
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use sui_sdk::SuiClientBuilder;
use tokio_util::sync::CancellationToken;
use url::Url;

use deeplook_schema::models::Pool;
use deeplook_schema::schema::pools;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[clap(env, long, default_value = "0.0.0.0:9184")]
    metrics_address: SocketAddr,
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/deeplook"
    )]
    database_url: Url,
    #[clap(env, long, default_value = "redis://localhost:6379")]
    redis_url: Url,
    #[clap(env, long, default_value = "https://fullnode.mainnet.sui.io:443")]
    rpc_url: Url,
    /// Deeplook environment, defaulted to SUI mainnet.
    #[clap(env, long)]
    env: DeeplookEnv,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Args {
        metrics_address,
        database_url,
        redis_url,
        rpc_url,
        env,
    } = Args::parse();

    let mut db_connection =
        PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB");
    let sui_client = SuiClientBuilder::default()
        .build(rpc_url.as_str())
        .await
        .expect("Failed building sui client");

    let cache = Cache::new(redis_url);

    let pools = pools::table
        .load::<Pool>(&mut db_connection)
        .expect("Failed getting pools from db");

    let current_checkpoint = CheckpointDigest::get_sequence_number(sui_client.clone().into())
        .await
        .expect("Failed getting current checkpoint");

    let mut ob_manager_map: OrderbookManagerMap = HashMap::new();

    for pool in pools {
        let name = pool.pool_name.to_string();
        let id = pool.pool_id.to_string();
        let mut ob_manager =
            OrderbookManager::new(pool, sui_client.clone().into(), Mutex::new(cache.clone()));
        if ob_manager.sync().await.is_err() {
            println!("Failed syncing {}", name);
            continue;
        }
        let arc = Arc::new(Mutex::new(ob_manager));
        ob_manager_map.insert(name, arc.clone());
        ob_manager_map.insert(id, arc);
    }

    let cancel = CancellationToken::new();
    let registry = Registry::new_custom(Some("deeplook".into()), None)
        .context("Failed to create Prometheus registry.")?;
    let metrics = MetricsService::new(
        MetricsArgs { metrics_address },
        registry,
        cancel.child_token(),
    );

    let mut indexer = Indexer::new(
        database_url,
        DbArgs::default(),
        IndexerArgs {
            first_checkpoint: Some(current_checkpoint - 100),
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
        Default::default(),
        None,
        metrics.registry(),
        cancel.clone(),
    )
    .await?;

    let arc_manager_map = Arc::new(ob_manager_map);

    indexer
        .concurrent_pipeline(
            OrderbookOrderFillHandler::new(env, arc_manager_map.clone()),
            Default::default(),
        )
        .await?;
    indexer
        .concurrent_pipeline(
            OrderbookOrderUpdateHandler::new(env, arc_manager_map),
            Default::default(),
        )
        .await?;

    let h_indexer = indexer.run().await?;
    let h_metrics = metrics.run().await?;

    let _ = h_indexer.await;
    cancel.cancel();
    let _ = h_metrics.await;

    Ok(())
}

use clap::Parser;
use deeplook_indexer::DeepbookEnv;
use deeplook_orderbook::OrderbookManagerMap;
use deeplook_orderbook::catch_up::catch_up;
use deeplook_orderbook::keep_up::keep_up;
use deeplook_orderbook::orderbook::OrderbookManager;
use deeplook_utils::cache::Cache;
use deeplook_utils::logging::setup_logging;
use diesel::{Connection, ExpressionMethods, PgConnection, QueryDsl, RunQueryDsl};
use tracing::{error, info};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use sui_sdk::SuiClientBuilder;
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
    env: DeepbookEnv,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Args {
        metrics_address,
        database_url,
        redis_url,
        rpc_url,
        env: _,
    } = Args::parse();
    setup_logging();

    let mut db_connection =
        PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB");
    let sui_client = SuiClientBuilder::default()
        .build(rpc_url.as_str())
        .await
        .expect("Failed building sui client");

    let mut cache = Cache::new(redis_url);
    let deleted = cache
        .delete_by_prefixes(&["orderbook::", "latest_trades::"])
        .map_err(|e| anyhow::anyhow!("failed clearing redis startup keys: {:?}", e))?;
    info!("Cleared {} Redis keys on startup", deleted);

    // if None index all pools, if Some index only pool names in the list
    let whitelisted_pools: Option<Vec<&'static str>> = None;

    let pools = match whitelisted_pools {
        Some(white_list) => pools::table
            .filter(pools::pool_name.eq_any(&white_list))
            .load::<Pool>(&mut db_connection)
            .expect("Failed getting pools from db"),
        None => pools::table
            .load::<Pool>(&mut db_connection)
            .expect("Failed getting pools from db"),
    };

    let mut ob_manager_map: OrderbookManagerMap = HashMap::new();

    for pool in pools {
        let name = pool.pool_name.to_string();
        let id = pool.pool_id.to_string();
        let ob_manager = OrderbookManager::new(
            pool,
            sui_client.clone().into(),
            Mutex::new(cache.clone()),
            database_url.clone(),
        );
        let arc = Arc::new(Mutex::new(ob_manager));
        ob_manager_map.insert(name, arc.clone());
        ob_manager_map.insert(id, arc);
    }

    let orderbook_managers = Arc::new(ob_manager_map);

    let start = Instant::now();
    let catch_up_result = catch_up(
        database_url.clone(),
        metrics_address,
        orderbook_managers.clone(),
        u64::MAX,
    )
    .await;

    let duration = start.elapsed();

    let caught_up_checkpoint = match catch_up_result {
        Ok(checkpoint) => {
            info!(
                "Orderbooks caught up in {}s to checkpoint {}",
                duration.as_secs(),
                checkpoint
            );
            checkpoint
        }
        Err(e) => {
            error!("catching up failed: {:#?}", e);
            return Err(e);
        }
    };

    keep_up(
        database_url,
        metrics_address,
        orderbook_managers,
        caught_up_checkpoint + 1,
    )
    .await
}

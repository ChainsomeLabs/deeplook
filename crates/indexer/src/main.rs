use anyhow::Context;
use clap::Parser;
use deeplook_indexer::handlers::balances_handler::BalancesHandler;
use deeplook_indexer::handlers::flash_loan_handler::FlashLoanHandler;
use deeplook_indexer::handlers::order_fill_handler::OrderFillHandler;
use deeplook_indexer::handlers::order_update_handler::OrderUpdateHandler;
use deeplook_indexer::handlers::pool_price_handler::PoolPriceHandler;
use deeplook_indexer::handlers::proposals_handler::ProposalsHandler;
use deeplook_indexer::handlers::rebates_handler::RebatesHandler;
use deeplook_indexer::handlers::stakes_handler::StakesHandler;
use deeplook_indexer::handlers::trade_params_update_handler::TradeParamsUpdateHandler;
use deeplook_indexer::handlers::vote_handler::VotesHandler;
use deeplook_indexer::DeeplookEnv;
use deeplook_schema::MIGRATIONS;
use prometheus::Registry;
use std::net::SocketAddr;
use sui_indexer_alt_framework::ingestion::ClientArgs;
use sui_indexer_alt_framework::{Indexer, IndexerArgs};
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use sui_pg_db::DbArgs;
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[command(flatten)]
    db_args: DbArgs,
    #[command(flatten)]
    indexer_args: IndexerArgs,
    #[clap(env, long, default_value = "0.0.0.0:9184")]
    metrics_address: SocketAddr,
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/deeplook"
    )]
    database_url: Url,
    /// Deeplook environment, defaulted to SUI mainnet.
    #[clap(env, long)]
    env: DeeplookEnv,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let Args {
        db_args,
        indexer_args,
        metrics_address,
        database_url,
        env,
    } = Args::parse();

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
        db_args,
        indexer_args,
        ClientArgs {
            remote_store_url: Some(env.remote_store_url()),
            local_ingestion_path: None,
            rpc_api_url: None,
            rpc_username: None,
            rpc_password: None,
        },
        Default::default(),
        Some(&MIGRATIONS),
        metrics.registry(),
        cancel.clone(),
    )
    .await?;

    indexer
        .concurrent_pipeline(BalancesHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(FlashLoanHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(OrderFillHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(OrderUpdateHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(PoolPriceHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(ProposalsHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(RebatesHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(StakesHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(TradeParamsUpdateHandler::new(env), Default::default())
        .await?;
    indexer
        .concurrent_pipeline(VotesHandler::new(env), Default::default())
        .await?;

    let h_indexer = indexer.run().await?;
    let h_metrics = metrics.run().await?;

    let _ = h_indexer.await;
    cancel.cancel();
    let _ = h_metrics.await;

    Ok(())
}

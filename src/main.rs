use std::sync::Arc;
use teloxide::prelude::*;

mod bot;
mod config;
mod error;
mod es;
mod models;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("search_bot_rs=info".parse()?),
        )
        .init();

    tracing::info!("Starting search-bot-rs...");

    // Load configuration (env vars override TOML)
    let config = config::AppConfig::load()?;
    tracing::info!("Elasticsearch URL: {}", config.elasticsearch.url);

    if config.webhook.is_enabled() {
        tracing::info!(
            "Mode: webhook ({} -> {}:{})",
            config.webhook.url,
            config.webhook.listen_addr,
            config.webhook.port
        );
    } else {
        tracing::info!("Mode: long-polling (debug)");
    }

    // Initialize Elasticsearch client and ensure index exists
    let es_client = es::client::create_client(&config).await?;
    tracing::info!("Elasticsearch client initialized");

    // Create batch indexer (spawns background flush task)
    let indexer = Arc::new(es::indexer::BatchIndexer::new(
        es_client.clone(),
        config.elasticsearch.index_name.clone(),
        config.indexer.batch_size,
        config.indexer.flush_interval_ms,
    ));

    // Create search client
    let search_client = Arc::new(es::search::SearchClient::new(
        es_client,
        config.elasticsearch.index_name,
    ));

    // Create user cache (in-memory username<->user_id mapping)
    let user_cache = models::user_cache::UserCache::new();

    // Create bot and launch webhook dispatcher
    let bot = Bot::new(&config.telegram.bot_token);

    tracing::info!("Bot starting...");

    bot::handler::run_bot(
        bot,
        indexer,
        search_client,
        user_cache,
        config.search.default_page_size,
        config.webhook,
    )
    .await?;

    Ok(())
}

//! MongoDB to Elasticsearch migration tool.
//!
//! Migrates message data from MongoDB (BotLog format) to Elasticsearch,
//! processing only groups that already exist in ES and filling in older messages.

use anyhow::{Context, Result};
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{BulkParts, Elasticsearch, SearchParts};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Client as MongoClient,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use url::Url;

// ── Configuration ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Config {
    mongodb: MongoDbConfig,
    elasticsearch: EsConfig,
    migration: MigrationSettings,
}

#[derive(Debug, Deserialize)]
struct MongoDbConfig {
    uri: String,
    database: String,
    collection: String,
}

#[derive(Debug, Deserialize)]
struct EsConfig {
    url: String,
    index_name: String,
}

#[derive(Debug, Deserialize)]
struct MigrationSettings {
    batch_size: usize,
    #[serde(default)]
    dry_run: bool,
}

// ── Data models ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct EsMessage {
    message_id: i64,
    chat_id: i64,
    user_id: Option<i64>,
    text: String,
    date: i64,
    message_type: String,
}

#[derive(Debug, Clone)]
struct GroupInfo {
    chat_id: i64,
    earliest_message_id: i64,
}

// ── Main ───────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = load_config()?;
    if config.migration.dry_run {
        tracing::info!("DRY RUN mode enabled");
    }

    let es = create_es_client(&config.elasticsearch)?;
    let mongo = MongoClient::with_uri_str(&config.mongodb.uri)
        .await
        .context("Failed to connect to MongoDB")?;
    let collection = mongo
        .database(&config.mongodb.database)
        .collection::<Document>(&config.mongodb.collection);

    let groups = query_es_groups(&es, &config.elasticsearch.index_name).await?;
    if groups.is_empty() {
        tracing::info!("No groups found in ES — nothing to migrate");
        return Ok(());
    }

    tracing::info!("Found {} groups in ES", groups.len());
    for g in &groups {
        tracing::info!("  group {}: earliest message_id = {}", g.chat_id, g.earliest_message_id);
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;

    for (i, group) in groups.iter().enumerate() {
        tracing::info!("[{}/{}] Processing group {}", i + 1, groups.len(), group.chat_id);

        let filter = doc! {
            "group_id": group.chat_id,
            "msg_ctx.message_id": { "$lt": group.earliest_message_id },
            "msg_type": 1,
        };

        let count = collection.count_documents(filter.clone()).await?;
        if count == 0 {
            tracing::info!("  No messages to migrate");
            continue;
        }
        tracing::info!("  {count} messages to migrate");

        let options = mongodb::options::FindOptions::builder()
            .sort(doc! { "msg_ctx.message_id": 1 })
            .build();
        let mut cursor = collection.find(filter).with_options(options).await?;

        let mut batch: Vec<EsMessage> = Vec::with_capacity(config.migration.batch_size);
        let mut ok = 0usize;
        let mut err = 0usize;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => match parse_document(doc, group.chat_id) {
                    Ok(msg) => {
                        batch.push(msg);
                        if batch.len() >= config.migration.batch_size {
                            if config.migration.dry_run {
                                ok += batch.len();
                            } else {
                                match bulk_index(&es, &config.elasticsearch.index_name, &batch).await {
                                    Ok(n) => ok += n,
                                    Err(e) => {
                                        tracing::error!("  Bulk index error: {e}");
                                        err += batch.len();
                                    }
                                }
                            }
                            tracing::info!("  Progress: {ok}/{count}");
                            batch.clear();
                        }
                    }
                    Err(e) => {
                        tracing::warn!("  Parse error: {e}");
                        err += 1;
                    }
                },
                Err(e) => {
                    tracing::error!("  Cursor error: {e}");
                    err += 1;
                }
            }
        }

        // flush remainder
        if !batch.is_empty() {
            if config.migration.dry_run {
                ok += batch.len();
            } else {
                match bulk_index(&es, &config.elasticsearch.index_name, &batch).await {
                    Ok(n) => ok += n,
                    Err(e) => {
                        tracing::error!("  Bulk index error: {e}");
                        err += batch.len();
                    }
                }
            }
        }

        tracing::info!("  Done: {ok} migrated, {err} errors");
        total_ok += ok;
        total_err += err;
    }

    tracing::info!("Migration complete: {total_ok} migrated, {total_err} errors");
    Ok(())
}

// ── Config loading ─────────────────────────────────────────────

fn load_config() -> Result<Config> {
    let mut config: Config = if std::path::Path::new("migrate.toml").exists() {
        let content = std::fs::read_to_string("migrate.toml")?;
        toml::from_str(&content).context("Failed to parse migrate.toml")?
    } else {
        let _ = dotenvy::dotenv();
        Config {
            mongodb: MongoDbConfig {
                uri: std::env::var("MONGODB_URI").context("MONGODB_URI not set")?,
                database: std::env::var("MONGODB_DATABASE").context("MONGODB_DATABASE not set")?,
                collection: std::env::var("MONGODB_COLLECTION")
                    .unwrap_or_else(|_| "messages".into()),
            },
            elasticsearch: EsConfig {
                url: std::env::var("ELASTICSEARCH_URL").context("ELASTICSEARCH_URL not set")?,
                index_name: std::env::var("ELASTICSEARCH_INDEX")
                    .context("ELASTICSEARCH_INDEX not set")?,
            },
            migration: MigrationSettings {
                batch_size: std::env::var("MIGRATION_BATCH_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(500),
                dry_run: std::env::var("MIGRATION_DRY_RUN")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(false),
            },
        }
    };

    // Env vars always override file config
    if let Ok(v) = std::env::var("MIGRATION_DRY_RUN") {
        if let Ok(b) = v.parse::<bool>() {
            config.migration.dry_run = b;
        }
    }
    if let Ok(v) = std::env::var("MIGRATION_BATCH_SIZE") {
        if let Ok(n) = v.parse::<usize>() {
            config.migration.batch_size = n;
        }
    }

    Ok(config)
}

// ── ES helpers ─────────────────────────────────────────────────

fn create_es_client(config: &EsConfig) -> Result<Arc<Elasticsearch>> {
    let url = Url::parse(&config.url)?;
    let pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(pool).disable_proxy().build()?;
    Ok(Arc::new(Elasticsearch::new(transport)))
}

async fn query_es_groups(es: &Elasticsearch, index: &str) -> Result<Vec<GroupInfo>> {
    let response = es
        .search(SearchParts::Index(&[index]))
        .size(0)
        .body(json!({
            "aggs": {
                "groups": {
                    "terms": { "field": "chat_id", "size": 10000 },
                    "aggs": {
                        "earliest": { "min": { "field": "message_id" } }
                    }
                }
            }
        }))
        .send()
        .await?;

    if response.status_code().as_u16() == 404 {
        return Ok(vec![]);
    }
    if !response.status_code().is_success() {
        let body: serde_json::Value = response.json().await?;
        anyhow::bail!("ES aggregation failed: {body}");
    }

    let body: serde_json::Value = response.json().await?;
    let groups = body["aggregations"]["groups"]["buckets"]
        .as_array()
        .map(|buckets| {
            buckets
                .iter()
                .filter_map(|b| {
                    let chat_id = b["key"].as_i64()?;
                    let earliest = b["earliest"]["value"]
                        .as_f64()
                        .map(|f| f as i64)
                        .or_else(|| b["earliest"]["value"].as_i64())?;
                    Some(GroupInfo {
                        chat_id,
                        earliest_message_id: earliest,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(groups)
}

// ── Document parsing ───────────────────────────────────────────

fn parse_document(doc: Document, expected_chat_id: i64) -> Result<EsMessage> {
    let message_id = doc
        .get_document("msg_ctx")
        .and_then(|ctx| {
            ctx.get_i64("message_id")
                .or_else(|_| ctx.get_i32("message_id").map(i64::from))
        })
        .context("Missing msg_ctx.message_id")?;

    let chat_id = doc
        .get_i64("group_id")
        .or_else(|_| doc.get_i32("group_id").map(i64::from))
        .unwrap_or(expected_chat_id);

    let user_id = doc
        .get_i64("user_id")
        .or_else(|_| doc.get_i32("user_id").map(i64::from))
        .ok();

    let text = doc
        .get_document("msg_ctx")
        .and_then(|ctx| ctx.get_str("command").map(String::from))
        .unwrap_or_default();

    let date = doc
        .get_datetime("timestamp")
        .map(|dt| dt.timestamp_millis() / 1000)
        .or_else(|_| doc.get_i64("timestamp"))
        .context("Missing timestamp")?;

    Ok(EsMessage {
        message_id,
        chat_id,
        user_id,
        text,
        date,
        message_type: "text".into(),
    })
}

// ── Bulk indexing ──────────────────────────────────────────────

async fn bulk_index(es: &Elasticsearch, index: &str, messages: &[EsMessage]) -> Result<usize> {
    if messages.is_empty() {
        return Ok(0);
    }

    let mut body: Vec<JsonBody<serde_json::Value>> = Vec::with_capacity(messages.len() * 2);
    for msg in messages {
        let doc_id = format!("{}_{}", msg.chat_id, msg.message_id);
        body.push(json!({ "index": { "_id": doc_id } }).into());
        body.push(serde_json::to_value(msg)?.into());
    }

    let response = es
        .bulk(BulkParts::Index(index))
        .body(body)
        .send()
        .await?;

    if !response.status_code().is_success() {
        let body: serde_json::Value = response.json().await?;
        anyhow::bail!("Bulk index failed: {body}");
    }

    let body: serde_json::Value = response.json().await?;
    if body["errors"].as_bool().unwrap_or(false) {
        let errs = body["items"]
            .as_array()
            .map(|items| items.iter().filter(|i| i["index"]["error"].is_object()).count())
            .unwrap_or(0);
        tracing::warn!("Bulk index: {errs} errors out of {}", messages.len());
        return Ok(messages.len() - errs);
    }

    Ok(messages.len())
}

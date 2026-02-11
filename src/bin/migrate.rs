//! MongoDB to Elasticsearch Migration Tool
//! 
//! This tool migrates message data from MongoDB to Elasticsearch,
//! avoiding duplicates by querying the earliest message in ES first.

use anyhow::{Context, Result};
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{Elasticsearch, SearchParts, BulkParts};
use elasticsearch::http::request::JsonBody;
use futures::StreamExt;
use mongodb::{Client as MongoClient, bson::{doc, Document}};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::{info, warn, error};
use url::Url;

/// Configuration for the migration tool
#[derive(Debug, Deserialize)]
struct MigrationConfig {
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
    #[serde(default = "default_dry_run")]
    dry_run: bool,
}

fn default_dry_run() -> bool {
    false
}

/// MongoDB message document structure
/// This represents the message as stored in MongoDB
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MongoMessage {
    message_id: i64,
    #[serde(alias = "group_id")]
    chat_id: i64,
    user_id: Option<i64>,
    text: String,
    /// Unix timestamp in seconds or milliseconds
    #[serde(alias = "timestamp")]
    date: i64,
    #[serde(alias = "msg_type")]
    message_type: String,
}

/// Elasticsearch message document structure (same as ChatMessage)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EsMessage {
    message_id: i64,
    chat_id: i64,
    user_id: Option<i64>,
    text: String,
    /// Unix timestamp in seconds
    date: i64,
    message_type: String,
}

impl From<MongoMessage> for EsMessage {
    fn from(mongo_msg: MongoMessage) -> Self {
        // Convert timestamp to seconds if it's in milliseconds (> year 2100 in seconds)
        let date = if mongo_msg.date > 4_000_000_000 {
            mongo_msg.date / 1000
        } else {
            mongo_msg.date
        };

        Self {
            message_id: mongo_msg.message_id,
            chat_id: mongo_msg.chat_id,
            user_id: mongo_msg.user_id,
            text: mongo_msg.text,
            date,
            // Always use "text" as message_type regardless of MongoDB msg_type
            message_type: "text".to_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting MongoDB to Elasticsearch migration");

    // Load configuration
    let config = load_config()?;
    
    if config.migration.dry_run {
        info!("Running in DRY RUN mode - no data will be written to ES");
    }

    // Connect to ES
    let es_client = create_es_client(&config.elasticsearch).await?;
    
    // Connect to MongoDB
    let mongo_client = MongoClient::with_uri_str(&config.mongodb.uri)
        .await
        .context("Failed to connect to MongoDB")?;
    
    let db = mongo_client.database(&config.mongodb.database);
    let collection = db.collection::<Document>(&config.mongodb.collection);

    // Step 1: Query all groups in ES and their earliest messages
    info!("Querying groups and their earliest messages in Elasticsearch...");
    let groups = get_groups_with_earliest_messages(&es_client, &config.elasticsearch.index_name).await?;
    
    if groups.is_empty() {
        info!("No groups found in ES with existing messages");
        info!("Migration complete - nothing to migrate!");
        return Ok(());
    }
    
    info!("Found {} groups in ES with existing messages:", groups.len());
    for group in &groups {
        info!("  - Group {}: earliest message_id = {}",
            group.chat_id,
            group.earliest_message_id
        );
    }

    // Step 2: Migrate each group separately
    let mut total_migrated = 0;
    let mut total_errors = 0;

    for (idx, group) in groups.iter().enumerate() {
        info!("\n[{}/{}] Processing group {}...", idx + 1, groups.len(), group.chat_id);
        
        // Query messages for this specific group with message_id less than the earliest in ES
        // Note: message_id is inside msg_ctx in MongoDB BotLog structure
        // Only migrate msg_type = 1 (photo messages)
        let filter = doc! {
            "$and": [
                {
                    "group_id": group.chat_id
                },
                {
                    "msg_ctx.message_id": { "$lt": group.earliest_message_id }
                },
                {
                    "msg_type": 1
                }
            ]
        };

        let group_count = collection.count_documents(filter.clone()).await?;
        
        if group_count == 0 {
            info!("  No messages to migrate for group {}", group.chat_id);
            continue;
        }
        
        info!("  Found {} messages to migrate for group {}", group_count, group.chat_id);

        // Sort by message_id ascending to migrate oldest first
        let find_options = mongodb::options::FindOptions::builder()
            .sort(doc! { "msg_ctx.message_id": 1 })
            .build();

        let mut cursor = collection.find(filter).with_options(find_options).await?;
        
        let mut batch: Vec<EsMessage> = Vec::with_capacity(config.migration.batch_size);
        let mut group_migrated = 0;
        let mut group_errors = 0;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => {
                    match parse_mongo_document(doc) {
                        Ok(mongo_msg) => {
                            let es_msg = EsMessage::from(mongo_msg);
                            batch.push(es_msg);

                            if batch.len() >= config.migration.batch_size {
                                if !config.migration.dry_run {
                                    match bulk_index(&es_client, &config.elasticsearch.index_name, &batch).await {
                                        Ok(count) => {
                                            group_migrated += count;
                                            info!("    Migrated {} messages (group progress: {}/{})", 
                                                count, group_migrated, group_count);
                                        }
                                        Err(e) => {
                                            error!("    Failed to bulk index: {}", e);
                                            group_errors += batch.len();
                                        }
                                    }
                                } else {
                                    group_migrated += batch.len();
                                    info!("    DRY RUN: Would migrate {} messages (group progress: {}/{})", 
                                        batch.len(), group_migrated, group_count);
                                }
                                batch.clear();
                            }
                        }
                        Err(e) => {
                            warn!("    Failed to parse document: {}", e);
                            group_errors += 1;
                        }
                    }
                }
                Err(e) => {
                    error!("    Failed to fetch document: {}", e);
                    group_errors += 1;
                }
            }
        }

        // Flush remaining batch for this group
        if !batch.is_empty() {
            if !config.migration.dry_run {
                match bulk_index(&es_client, &config.elasticsearch.index_name, &batch).await {
                    Ok(count) => {
                        group_migrated += count;
                        info!("    Migrated final batch of {} messages for group {}", count, group.chat_id);
                    }
                    Err(e) => {
                        error!("    Failed to bulk index final batch: {}", e);
                        group_errors += batch.len();
                    }
                }
            } else {
                group_migrated += batch.len();
                info!("    DRY RUN: Would migrate final batch of {} messages for group {}", batch.len(), group.chat_id);
            }
        }

        info!("  ✓ Group {} complete: {} migrated, {} errors", 
            group.chat_id, group_migrated, group_errors);
        
        total_migrated += group_migrated;
        total_errors += group_errors;
    }

    info!("\n=== Migration Complete! ===");
    info!("Total groups processed: {}", groups.len());
    info!("Successfully migrated: {} messages", total_migrated);
    if total_errors > 0 {
        warn!("Errors encountered: {} documents", total_errors);
    }

    Ok(())
}

fn load_config() -> Result<MigrationConfig> {
    // Try loading from migrate.toml
    let mut config = if std::path::Path::new("migrate.toml").exists() {
        let content = std::fs::read_to_string("migrate.toml")?;
        toml::from_str(&content)
            .context("Failed to parse migrate.toml")?
    } else {
        // Fallback to environment variables
        let _ = dotenvy::dotenv();
        
        MigrationConfig {
            mongodb: MongoDbConfig {
                uri: std::env::var("MONGODB_URI")
                    .context("MONGODB_URI not set")?,
                database: std::env::var("MONGODB_DATABASE")
                    .context("MONGODB_DATABASE not set")?,
                collection: std::env::var("MONGODB_COLLECTION")
                    .unwrap_or_else(|_| "messages".to_string()),
            },
            elasticsearch: EsConfig {
                url: std::env::var("ELASTICSEARCH_URL")
                    .context("ELASTICSEARCH_URL not set")?,
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

    // Environment variables override config file settings
    if let Ok(dry_run_str) = std::env::var("MIGRATION_DRY_RUN") {
        if let Ok(dry_run) = dry_run_str.parse::<bool>() {
            config.migration.dry_run = dry_run;
        }
    }

    if let Ok(batch_size_str) = std::env::var("MIGRATION_BATCH_SIZE") {
        if let Ok(batch_size) = batch_size_str.parse::<usize>() {
            config.migration.batch_size = batch_size;
        }
    }

    Ok(config)
}

async fn create_es_client(config: &EsConfig) -> Result<Arc<Elasticsearch>> {
    let url = Url::parse(&config.url)?;
    let pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);
    Ok(Arc::new(client))
}

/// Group information with earliest message ID
#[derive(Debug, Clone)]
struct GroupEarliestMessage {
    chat_id: i64,
    earliest_message_id: i64,
}

/// Query all groups in ES and their earliest message IDs
async fn get_groups_with_earliest_messages(
    es: &Elasticsearch,
    index_name: &str,
) -> Result<Vec<GroupEarliestMessage>> {
    let response = es
        .search(SearchParts::Index(&[index_name]))
        .size(0)
        .body(json!({
            "aggs": {
                "groups": {
                    "terms": {
                        "field": "chat_id",
                        "size": 10000  // Maximum groups to process
                    },
                    "aggs": {
                        "earliest_message": {
                            "min": {
                                "field": "message_id"
                            }
                        }
                    }
                }
            }
        }))
        .send()
        .await?;

    let status = response.status_code();
    if status.as_u16() == 404 {
        // Index doesn't exist yet
        info!("ES index does not exist, no groups to migrate");
        return Ok(Vec::new());
    }

    if !status.is_success() {
        let body: serde_json::Value = response.json().await?;
        anyhow::bail!("Failed to query ES groups (status {}): {}", status, body);
    }

    let body: serde_json::Value = response.json().await?;
    
    // Debug: log the response to understand what ES returns
    tracing::debug!("ES aggregation response: {}", serde_json::to_string_pretty(&body).unwrap_or_default());
    
    let mut groups = Vec::new();
    
    if let Some(buckets) = body["aggregations"]["groups"]["buckets"].as_array() {
        for bucket in buckets {
            let chat_id = bucket["key"].as_i64();
            
            // ES may return the value as float or int, so we need to handle both
            let earliest_message_id = bucket["earliest_message"]["value"].as_f64()
                .map(|f| f as i64)
                .or_else(|| bucket["earliest_message"]["value"].as_i64());
            
            if let (Some(chat_id), Some(earliest_message_id)) = (chat_id, earliest_message_id) {
                groups.push(GroupEarliestMessage {
                    chat_id,
                    earliest_message_id,
                });
            } else {
                warn!("Failed to parse bucket: chat_id={:?}, earliest_message_id={:?}, bucket={}", 
                    chat_id, earliest_message_id, bucket);
            }
        }
    } else {
        warn!("No aggregations found in ES response. Response body: {}", 
            serde_json::to_string_pretty(&body).unwrap_or_default());
    }

    Ok(groups)
}

/// Parse MongoDB document to MongoMessage
fn parse_mongo_document(doc: Document) -> Result<MongoMessage> {
    // message_id is inside msg_ctx in BotLog structure
    let message_id = doc.get_document("msg_ctx")
        .and_then(|ctx| ctx.get_i64("message_id")
            .or_else(|_| ctx.get_i32("message_id").map(|v| v as i64)))
        .or_else(|_| doc.get_i64("message_id"))
        .or_else(|_| doc.get_i32("message_id").map(|v| v as i64))
        .context("Missing message_id or msg_ctx.message_id")?;

    // BotLog uses group_id, not chat_id
    let chat_id = doc.get_i64("group_id")
        .or_else(|_| doc.get_i64("chat_id"))
        .or_else(|_| doc.get_i32("group_id").map(|v| v as i64))
        .or_else(|_| doc.get_i32("chat_id").map(|v| v as i64))
        .context("Missing group_id/chat_id")?;

    // user_id in BotLog is u64 but we store as i64
    let user_id = doc.get_i64("user_id")
        .or_else(|_| doc.get_i32("user_id").map(|v| v as i64))
        .ok();

    // text is in msg_ctx.command in BotLog structure
    let text = doc.get_document("msg_ctx")
        .and_then(|ctx| ctx.get_str("command").map(|s| s.to_string()))
        .or_else(|_| doc.get_str("text").map(|s| s.to_string()))
        .or_else(|_| doc.get_str("content").map(|s| s.to_string()))
        .unwrap_or_default();

    // timestamp is ISODate in BotLog
    let date = doc.get_datetime("timestamp")
        .or_else(|_| doc.get_datetime("date"))
        .map(|dt| dt.timestamp_millis() / 1000) // Convert to seconds
        .or_else(|_| doc.get_i64("date"))
        .or_else(|_| doc.get_i64("timestamp"))
        .or_else(|_| doc.get_i32("date").map(|v| v as i64))
        .or_else(|_| doc.get_i32("timestamp").map(|v| v as i64))
        .context("Missing date/timestamp")?;

    // msg_type is a number in BotLog: 0=text, 1=photo, 2=video, etc.
    let message_type = doc.get_i32("msg_type")
        .or_else(|_| doc.get_i64("msg_type").map(|v| v as i32))
        .map(|v| v.to_string())
        .or_else(|_| doc.get_str("message_type").map(|s| s.to_string()))
        .or_else(|_| doc.get_str("msg_type").map(|s| s.to_string()))
        .or_else(|_| doc.get_str("type").map(|s| s.to_string()))
        .unwrap_or_else(|_| "0".to_string());

    Ok(MongoMessage {
        message_id,
        chat_id,
        user_id,
        text,
        date,
        message_type,
    })
}

/// Bulk index messages to Elasticsearch
async fn bulk_index(
    es: &Elasticsearch,
    index_name: &str,
    messages: &[EsMessage],
) -> Result<usize> {
    if messages.is_empty() {
        return Ok(0);
    }

    let mut body: Vec<JsonBody<serde_json::Value>> = Vec::with_capacity(messages.len() * 2);

    for msg in messages {
        let doc_id = format!("{}_{}", msg.chat_id, msg.message_id);
        
        // Action line
        body.push(json!({ "index": { "_id": doc_id } }).into());
        // Document line
        body.push(serde_json::to_value(msg)?.into());
    }

    let response = es
        .bulk(BulkParts::Index(index_name))
        .body(body)
        .send()
        .await?;

    let status = response.status_code();
    if !status.is_success() {
        let body: serde_json::Value = response.json().await?;
        anyhow::bail!("Bulk index failed (status {}): {}", status, body);
    }

    let body: serde_json::Value = response.json().await?;
    
    if body["errors"].as_bool().unwrap_or(false) {
        let error_items: Vec<&serde_json::Value> = body["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter(|item| item["index"]["error"].is_object())
                    .collect()
            })
            .unwrap_or_default();
        
        warn!("Bulk index had {} errors out of {}", error_items.len(), messages.len());
        return Ok(messages.len() - error_items.len());
    }

    Ok(messages.len())
}

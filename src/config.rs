use anyhow::bail;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub telegram: TelegramConfig,
    pub elasticsearch: EsConfig,
    pub indexer: IndexerConfig,
    pub search: SearchConfig,
    #[serde(default)]
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EsConfig {
    pub url: String,
    pub index_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerConfig {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchConfig {
    pub default_page_size: usize,
    pub max_page_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    /// Public URL that Telegram sends updates to, e.g. https://example.com
    pub url: String,
    /// Address to bind the webhook listener, e.g. 0.0.0.0
    pub listen_addr: String,
    /// Port for the webhook listener
    pub port: u16,
}

impl WebhookConfig {
    pub fn is_enabled(&self) -> bool {
        !self.url.is_empty()
    }
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            listen_addr: "0.0.0.0".into(),
            port: 8443,
        }
    }
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        // Step 1: Try loading .env file (silently ignore if not found)
        let _ = dotenvy::dotenv();

        // Step 2: Try loading TOML config as base
        let mut config = if Path::new("config.toml").exists() {
            let content = std::fs::read_to_string("config.toml")?;
            toml::from_str::<AppConfig>(&content)?
        } else {
            AppConfig::defaults()
        };

        // Step 3: Override with environment variables where present
        if let Ok(token) = std::env::var("TELOXIDE_TOKEN") {
            config.telegram.bot_token = token;
        }
        if let Ok(url) = std::env::var("ELASTICSEARCH_URL") {
            config.elasticsearch.url = url;
        }
        if let Ok(index) = std::env::var("ELASTICSEARCH_INDEX") {
            config.elasticsearch.index_name = index;
        }
        if let Ok(val) = std::env::var("INDEXER_BATCH_SIZE") {
            config.indexer.batch_size = val.parse()?;
        }
        if let Ok(val) = std::env::var("INDEXER_FLUSH_INTERVAL_MS") {
            config.indexer.flush_interval_ms = val.parse()?;
        }
        if let Ok(val) = std::env::var("SEARCH_DEFAULT_PAGE_SIZE") {
            config.search.default_page_size = val.parse()?;
        }
        if let Ok(val) = std::env::var("SEARCH_MAX_PAGE_SIZE") {
            config.search.max_page_size = val.parse()?;
        }
        if let Ok(val) = std::env::var("WEBHOOK_URL") {
            config.webhook.url = val;
        }
        if let Ok(val) = std::env::var("WEBHOOK_LISTEN_ADDR") {
            config.webhook.listen_addr = val;
        }
        if let Ok(val) = std::env::var("WEBHOOK_PORT") {
            config.webhook.port = val.parse()?;
        }

        // Validate
        if config.telegram.bot_token.is_empty()
            || config.telegram.bot_token == "YOUR_BOT_TOKEN_HERE"
        {
            bail!(
                "Bot token not configured. Set TELOXIDE_TOKEN env var or telegram.bot_token in config.toml"
            );
        }
        Ok(config)
    }

    fn defaults() -> Self {
        Self {
            telegram: TelegramConfig {
                bot_token: String::new(),
            },
            elasticsearch: EsConfig {
                url: "http://localhost:9200".into(),
                index_name: "telegram_messages".into(),
            },
            indexer: IndexerConfig {
                batch_size: 50,
                flush_interval_ms: 5000,
            },
            search: SearchConfig {
                default_page_size: 5,
                max_page_size: 20,
            },
            webhook: WebhookConfig::default(),
        }
    }
}

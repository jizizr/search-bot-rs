use elasticsearch::http::request::JsonBody;
use elasticsearch::{BulkParts, Elasticsearch};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

use crate::models::message::ChatMessage;

pub struct BatchIndexer {
    sender: mpsc::Sender<ChatMessage>,
}

impl BatchIndexer {
    pub fn new(
        es_client: Arc<Elasticsearch>,
        index_name: String,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<ChatMessage>(batch_size * 4);
        tokio::spawn(flush_loop(rx, es_client, index_name, batch_size, flush_interval_ms));
        Self { sender: tx }
    }

    pub async fn index(&self, msg: ChatMessage) {
        if let Err(e) = self.sender.send(msg).await {
            tracing::warn!("Failed to queue message for indexing: {e}");
        }
    }
}

async fn flush_loop(
    mut rx: mpsc::Receiver<ChatMessage>,
    es: Arc<Elasticsearch>,
    index_name: String,
    batch_size: usize,
    flush_interval_ms: u64,
) {
    let mut buffer: Vec<ChatMessage> = Vec::with_capacity(batch_size);
    let mut tick = interval(Duration::from_millis(flush_interval_ms));
    tick.tick().await; // consume first immediate tick

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(m) => {
                        buffer.push(m);
                        if buffer.len() >= batch_size {
                            flush_buffer(&es, &index_name, &mut buffer).await;
                        }
                    }
                    None => {
                        if !buffer.is_empty() {
                            flush_buffer(&es, &index_name, &mut buffer).await;
                        }
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                if !buffer.is_empty() {
                    flush_buffer(&es, &index_name, &mut buffer).await;
                }
            }
        }
    }
}

async fn flush_buffer(es: &Elasticsearch, index_name: &str, buffer: &mut Vec<ChatMessage>) {
    let count = buffer.len();
    let mut body: Vec<JsonBody<serde_json::Value>> = Vec::with_capacity(count * 2);

    for msg in buffer.drain(..) {
        let doc_id = format!("{}_{}", msg.chat_id, msg.message_id);
        body.push(json!({"index": {"_id": doc_id}}).into());
        match serde_json::to_value(&msg) {
            Ok(val) => body.push(val.into()),
            Err(e) => {
                tracing::error!("Failed to serialize message: {e}");
                continue;
            }
        }
    }

    if body.is_empty() {
        return;
    }

    match es.bulk(BulkParts::Index(index_name)).body(body).send().await {
        Ok(response) if response.status_code().is_success() => {
            match response.json::<serde_json::Value>().await {
                Ok(body) if body["errors"].as_bool().unwrap_or(false) => {
                    let errs = body["items"]
                        .as_array()
                        .map(|items| {
                            items.iter().filter(|i| i["index"]["error"].is_object()).count()
                        })
                        .unwrap_or(0);
                    tracing::error!("Bulk index had {errs} errors out of {count}");
                }
                Ok(_) => tracing::debug!("Indexed {count} messages"),
                Err(e) => tracing::error!("Failed to read bulk response: {e}"),
            }
        }
        Ok(response) => tracing::error!("Bulk index returned status {}", response.status_code()),
        Err(e) => tracing::error!("Bulk index request failed: {e}"),
    }
}

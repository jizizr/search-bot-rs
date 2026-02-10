use elasticsearch::{Elasticsearch, SearchParts};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::models::message::ChatMessage;

pub struct SearchClient {
    es: Arc<Elasticsearch>,
    index_name: String,
}

#[derive(Debug, Clone)]
pub struct SearchParams {
    pub chat_id: i64,
    pub keyword: Option<String>,
    pub user_id: Option<i64>,
    pub date_from: Option<i64>,
    pub date_to: Option<i64>,
    pub message_type: Option<String>,
    pub page: usize,
    pub page_size: usize,
}

impl Default for SearchParams {
    fn default() -> Self {
        Self {
            chat_id: 0,
            keyword: None,
            user_id: None,
            date_from: None,
            date_to: None,
            message_type: None,
            page: 0,
            page_size: 5,
        }
    }
}

#[derive(Debug)]
pub struct SearchResult {
    pub total: u64,
    pub messages: Vec<SearchHit>,
    pub page: usize,
    pub total_pages: usize,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct SearchHit {
    pub message: ChatMessage,
    pub highlight: Option<String>,
    pub score: f64,
}

impl SearchClient {
    pub fn new(es: Arc<Elasticsearch>, index_name: String) -> Self {
        Self { es, index_name }
    }

    pub async fn search(&self, params: &SearchParams) -> anyhow::Result<SearchResult> {
        let query = self.build_query(params);
        let from = params.page * params.page_size;

        let response = self
            .es
            .search(SearchParts::Index(&[&self.index_name]))
            .from(from as i64)
            .size(params.page_size as i64)
            .body(query)
            .send()
            .await?;

        let status = response.status_code();
        if !status.is_success() {
            let body: Value = response.json().await?;
            anyhow::bail!("Search failed (status {status}): {body}");
        }

        let body: Value = response.json().await?;
        self.parse_response(&body, params.page, params.page_size)
    }

    fn build_query(&self, params: &SearchParams) -> Value {
        let mut must_clauses: Vec<Value> = vec![];
        let mut filter_clauses: Vec<Value> = vec![];

        // Always filter by chat_id (security: only search within the requesting group)
        filter_clauses.push(json!({ "term": { "chat_id": params.chat_id } }));

        // Full-text keyword search with IK smart analyzer
        if let Some(ref keyword) = params.keyword
            && !keyword.is_empty() {
                must_clauses.push(json!({
                    "match": {
                        "text": {
                            "query": keyword,
                            "analyzer": "ik_smart"
                        }
                    }
                }));
            }

        // Filter by user_id (resolved from username before search)
        if let Some(uid) = params.user_id {
            filter_clauses.push(json!({ "term": { "user_id": uid } }));
        }

        // Date range filter
        let mut range_obj = serde_json::Map::new();
        if let Some(from) = params.date_from {
            range_obj.insert("gte".to_string(), json!(from));
        }
        if let Some(to) = params.date_to {
            range_obj.insert("lte".to_string(), json!(to));
        }
        if !range_obj.is_empty() {
            filter_clauses.push(json!({ "range": { "date": range_obj } }));
        }

        // Message type filter
        if let Some(ref msg_type) = params.message_type {
            filter_clauses.push(json!({ "term": { "message_type": msg_type } }));
        }

        // If no keyword, use match_all in must
        if must_clauses.is_empty() {
            must_clauses.push(json!({ "match_all": {} }));
        }

        json!({
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            },
            "sort": [
                { "_score": { "order": "desc" } },
                { "date": { "order": "desc" } }
            ],
            "highlight": {
                "fields": {
                    "text": {
                        "pre_tags": ["<b>"],
                        "post_tags": ["</b>"],
                        "fragment_size": 100,
                        "number_of_fragments": 1
                    }
                }
            }
        })
    }

    fn parse_response(
        &self,
        body: &Value,
        page: usize,
        page_size: usize,
    ) -> anyhow::Result<SearchResult> {
        let total = body["hits"]["total"]["value"]
            .as_u64()
            .unwrap_or(0);

        let total_pages = if total == 0 {
            0
        } else {
            (total as usize).div_ceil(page_size)
        };

        let hits = body["hits"]["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut messages = Vec::with_capacity(hits.len());
        for hit in &hits {
            let source = &hit["_source"];
            let message: ChatMessage = match serde_json::from_value(source.clone()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to parse search hit: {e}");
                    continue;
                }
            };

            let highlight = hit["highlight"]["text"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .map(String::from);

            let score = hit["_score"].as_f64().unwrap_or(0.0);

            messages.push(SearchHit {
                message,
                highlight,
                score,
            });
        }

        Ok(SearchResult {
            total,
            messages,
            page,
            total_pages,
        })
    }
}

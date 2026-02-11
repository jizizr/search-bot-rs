use elasticsearch::{Elasticsearch, SearchParts};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::models::message::ChatMessage;

pub struct SearchClient {
    es: Arc<Elasticsearch>,
    index_name: String,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug)]
pub struct SearchResult {
    pub total: u64,
    pub messages: Vec<SearchHit>,
    pub page: usize,
    pub total_pages: usize,
}

#[derive(Debug)]
pub struct SearchHit {
    pub message: ChatMessage,
    pub highlight: Option<String>,
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
        let mut must = vec![];
        let mut filter = vec![json!({ "term": { "chat_id": params.chat_id } })];

        if let Some(ref kw) = params.keyword
            && !kw.is_empty()
        {
            must.push(json!({
                "match": { "text": { "query": kw, "analyzer": "ik_smart" } }
            }));
        }

        if must.is_empty() {
            must.push(json!({ "match_all": {} }));
        }

        if let Some(uid) = params.user_id {
            filter.push(json!({ "term": { "user_id": uid } }));
        }

        let mut range = serde_json::Map::new();
        if let Some(from) = params.date_from {
            range.insert("gte".into(), json!(from));
        }
        if let Some(to) = params.date_to {
            range.insert("lte".into(), json!(to));
        }
        if !range.is_empty() {
            filter.push(json!({ "range": { "date": range } }));
        }

        if let Some(ref mt) = params.message_type {
            filter.push(json!({ "term": { "message_type": mt } }));
        }

        json!({
            "query": {
                "bool": { "must": must, "filter": filter }
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
        let total = body["hits"]["total"]["value"].as_u64().unwrap_or(0);
        let total_pages = if total == 0 {
            0
        } else {
            (total as usize).div_ceil(page_size)
        };

        let messages = body["hits"]["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|hit| {
                let message: ChatMessage =
                    serde_json::from_value(hit["_source"].clone()).ok()?;
                let highlight = hit["highlight"]["text"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.as_str())
                    .map(String::from);
                Some(SearchHit {
                    message,
                    highlight,
                })
            })
            .collect();

        Ok(SearchResult {
            total,
            messages,
            page,
            total_pages,
        })
    }
}

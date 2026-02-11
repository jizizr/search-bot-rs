use serde_json::{json, Value};

pub fn index_settings_and_mappings() -> Value {
    json!({
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "message_id":   { "type": "long" },
                "chat_id":      { "type": "long" },
                "user_id":      { "type": "long" },
                "text": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "date":         { "type": "long" },
                "message_type": { "type": "keyword" }
            }
        }
    })
}

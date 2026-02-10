use serde_json::{json, Value};

pub fn index_settings_and_mappings() -> Value {
    json!({
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "ik_max": {
                        "type": "custom",
                        "tokenizer": "ik_max_word"
                    },
                    "ik_smart_search": {
                        "type": "custom",
                        "tokenizer": "ik_smart"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "message_id":   { "type": "long" },
                "chat_id":      { "type": "long" },
                "user_id":      { "type": "long" },
                "username":     { "type": "keyword" },
                "display_name": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {
                        "keyword": { "type": "keyword" }
                    }
                },
                "text": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "date": {
                    "type": "date",
                    "format": "epoch_second"
                },
                "reply_to_message_id": { "type": "long" },
                "message_type":        { "type": "keyword" },
                "chat_title":          { "type": "keyword" }
            }
        }
    })
}

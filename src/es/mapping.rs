use serde_json::{json, Value};

pub fn index_settings_and_mappings() -> Value {
    // IK plugin registers "ik_max_word" and "ik_smart" as built-in analyzers,
    // so they can be referenced directly in field mappings without custom definitions.
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
                "date": {
                    "type": "date",
                    "format": "epoch_second"
                },
                "message_type": { "type": "keyword" }
            }
        }
    })
}

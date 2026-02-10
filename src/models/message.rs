use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub message_id: i64,
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub username: Option<String>,
    pub display_name: String,
    pub text: String,
    /// Unix epoch seconds
    pub date: i64,
    pub reply_to_message_id: Option<i64>,
    pub message_type: MessageType,
    pub chat_title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Text,
    Photo,
    Video,
    Document,
    Sticker,
    Voice,
    Animation,
    Other,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageType::Text => write!(f, "text"),
            MessageType::Photo => write!(f, "photo"),
            MessageType::Video => write!(f, "video"),
            MessageType::Document => write!(f, "document"),
            MessageType::Sticker => write!(f, "sticker"),
            MessageType::Voice => write!(f, "voice"),
            MessageType::Animation => write!(f, "animation"),
            MessageType::Other => write!(f, "other"),
        }
    }
}

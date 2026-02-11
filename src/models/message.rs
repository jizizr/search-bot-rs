use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub message_id: i64,
    pub chat_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<i64>,
    pub text: String,
    /// Unix epoch seconds
    pub date: i64,
    pub message_type: MessageType,
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
            Self::Text => write!(f, "text"),
            Self::Photo => write!(f, "photo"),
            Self::Video => write!(f, "video"),
            Self::Document => write!(f, "document"),
            Self::Sticker => write!(f, "sticker"),
            Self::Voice => write!(f, "voice"),
            Self::Animation => write!(f, "animation"),
            Self::Other => write!(f, "other"),
        }
    }
}

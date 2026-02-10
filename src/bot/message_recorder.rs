use std::sync::Arc;
use teloxide::prelude::*;

use crate::es::indexer::BatchIndexer;
use crate::models::message::{ChatMessage, MessageType};
use crate::models::user_cache::UserCache;

pub async fn record_message(
    msg: Message,
    indexer: Arc<BatchIndexer>,
    user_cache: UserCache,
) -> anyhow::Result<()> {
    // Only record from groups and supergroups
    if !msg.chat.is_group() && !msg.chat.is_supergroup() {
        return Ok(());
    }

    // Always update user cache (even for media-only messages without text)
    if let Some(user) = msg.from.as_ref() {
        let display_name = match &user.last_name {
            Some(last) => format!("{} {last}", user.first_name),
            None => user.first_name.clone(),
        };
        user_cache.update(
            user.id.0 as i64,
            user.username.as_deref(),
            display_name,
        );
    }

    let text = extract_text(&msg);
    if text.is_empty() {
        return Ok(());
    }

    let chat_message = ChatMessage {
        message_id: msg.id.0 as i64,
        chat_id: msg.chat.id.0,
        user_id: msg.from.as_ref().map(|u| u.id.0 as i64),
        text,
        date: msg.date.timestamp(),
        message_type: classify_message(&msg),
    };

    indexer.index(chat_message).await;
    Ok(())
}

fn extract_text(msg: &Message) -> String {
    msg.text()
        .or_else(|| msg.caption())
        .unwrap_or_default()
        .to_string()
}

fn classify_message(msg: &Message) -> MessageType {
    if msg.text().is_some() {
        MessageType::Text
    } else if msg.photo().is_some() {
        MessageType::Photo
    } else if msg.video().is_some() {
        MessageType::Video
    } else if msg.document().is_some() {
        MessageType::Document
    } else if msg.sticker().is_some() {
        MessageType::Sticker
    } else if msg.voice().is_some() {
        MessageType::Voice
    } else if msg.animation().is_some() {
        MessageType::Animation
    } else {
        MessageType::Other
    }
}

use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{
    InlineKeyboardButton, InlineKeyboardMarkup, MaybeInaccessibleMessage, ParseMode,
    ReplyParameters,
};

use crate::es::search::{SearchClient, SearchParams, SearchResult};

/// Compact search state for encoding in callback data
#[derive(Debug, Clone)]
struct SearchState {
    page: usize,
    message_type: Option<String>,
    date_range: Option<&'static str>, // "7d", "30d", "90d"
    user_id: Option<i64>,
}

impl SearchState {
    /// Encode state as a compact string: {page}|{type}|{date}|{user_id}
    fn encode(&self) -> String {
        let type_char = match self.message_type.as_deref() {
            Some("text") => "t",
            Some("photo") => "p",
            Some("video") => "v",
            Some("document") => "d",
            _ => "-",
        };
        let date_char = match self.date_range {
            Some("7d") => "7",
            Some("30d") => "3",
            Some("90d") => "9",
            _ => "-",
        };
        let user_str = self.user_id.map_or("-".to_string(), |id| id.to_string());
        format!("{}|{}|{}|{}", self.page, type_char, date_char, user_str)
    }

    /// Decode state from compact string
    fn decode(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() != 4 {
            anyhow::bail!("Invalid state format: {}", s);
        }

        let page = parts[0].parse::<usize>()?;

        let message_type = match parts[1] {
            "t" => Some("text".to_string()),
            "p" => Some("photo".to_string()),
            "v" => Some("video".to_string()),
            "d" => Some("document".to_string()),
            "-" => None,
            _ => anyhow::bail!("Invalid message type: {}", parts[1]),
        };

        let date_range = match parts[2] {
            "7" => Some("7d"),
            "3" => Some("30d"),
            "9" => Some("90d"),
            "-" => None,
            _ => anyhow::bail!("Invalid date range: {}", parts[2]),
        };

        let user_id = if parts[3] == "-" {
            None
        } else {
            Some(parts[3].parse::<i64>()?)
        };

        Ok(Self {
            page,
            message_type,
            date_range,
            user_id,
        })
    }

    fn to_date_from(&self) -> Option<i64> {
        let now = chrono::Utc::now().timestamp();
        match self.date_range {
            Some("7d") => Some(now - 7 * 86400),
            Some("30d") => Some(now - 30 * 86400),
            Some("90d") => Some(now - 90 * 86400),
            _ => None,
        }
    }
}

/// Handle the /search command: perform initial search and show results with keyboard.
pub async fn handle_search(
    bot: Bot,
    msg: Message,
    query: String,
    search_client: Arc<SearchClient>,
    default_page_size: usize,
) -> anyhow::Result<()> {
    let chat_id = msg.chat.id;

    if query.trim().is_empty() {
        bot.send_message(
            chat_id,
            "用法: /s <关键词>\n\n\
             示例:\n\
             /s 你好\n\
             /s id:123456 关键词\n\n\
             也可以回复某人的消息后发送 /s 关键词，自动过滤该用户",
        )
        .await?;
        return Ok(());
    }

    let reply_user_id = msg
        .reply_to_message()
        .and_then(|r| r.from.as_ref())
        .map(|u| u.id.0 as i64);

    let (keyword, user_id_filter) = parse_search_query(&query, reply_user_id);

    let params = SearchParams {
        chat_id: chat_id.0,
        keyword: Some(keyword.clone()),
        user_id: user_id_filter,
        page_size: default_page_size,
        ..Default::default()
    };

    let result = search_client.search(&params).await?;

    let state = SearchState {
        page: 0,
        message_type: None,
        date_range: None,
        user_id: user_id_filter,
    };

    let text = format_results(&result, chat_id.0);
    let keyboard = build_keyboard(&result, &state, user_id_filter.is_some());

    bot.send_message(chat_id, text)
        .parse_mode(ParseMode::Html)
        .reply_markup(keyboard)
        .reply_parameters(ReplyParameters::new(msg.id))
        .await?;

    Ok(())
}

/// Handle inline keyboard callback queries for pagination and filters.
pub async fn handle_callback(
    bot: Bot,
    q: CallbackQuery,
    search_client: Arc<SearchClient>,
    default_page_size: usize,
) -> anyhow::Result<()> {
    let data = match q.data {
        Some(ref d) => d.clone(),
        None => return Ok(()),
    };

    // Ignore noop callbacks
    if data == "noop" {
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    }

    bot.answer_callback_query(q.id.clone()).await?;

    let msg = match q.message {
        Some(MaybeInaccessibleMessage::Regular(ref m)) => m.clone(),
        _ => return Ok(()),
    };

    // Decode the state from callback data
    let state = SearchState::decode(&data)?;

    // Get the original search command from reply_to_message
    let original_msg = msg
        .reply_to_message()
        .ok_or_else(|| anyhow::anyhow!("No reply_to_message found"))?;

    let query = extract_search_query(&original_msg)?;

    // user_id_filter is now stored in state, no need to get from reply_to_message
    let (keyword, _) = parse_search_query(&query, None);

    // Build search params from state and original query
    let params = SearchParams {
        chat_id: msg.chat.id.0,
        keyword: Some(keyword),
        user_id: state.user_id,
        page: state.page,
        page_size: default_page_size,
        message_type: state.message_type.clone(),
        date_from: state.to_date_from(),
        date_to: None,
    };

    // Perform search
    let result = search_client.search(&params).await?;
    let text = format_results(&result, msg.chat.id.0);
    let keyboard = build_keyboard(&result, &state, state.user_id.is_some());

    // Update message
    match bot
        .edit_message_text(msg.chat.id, msg.id, text)
        .parse_mode(ParseMode::Html)
        .reply_markup(keyboard)
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("message is not modified") => {}
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

/// Extract search query from a message (either from /s command or message text)
fn extract_search_query(msg: &Message) -> anyhow::Result<String> {
    let text = msg
        .text()
        .ok_or_else(|| anyhow::anyhow!("Message has no text"))?;

    // Check if it starts with /s or /search command
    if let Some(query) = text.strip_prefix("/s ") {
        return Ok(query.to_string());
    }
    if let Some(query) = text.strip_prefix("/search ") {
        return Ok(query.to_string());
    }

    // If no command prefix, return the whole text
    Ok(text.to_string())
}

// ── Helpers ────────────────────────────────────────────────────

fn parse_search_query(query: &str, reply_user_id: Option<i64>) -> (String, Option<i64>) {
    let parts: Vec<&str> = query.splitn(2, ' ').collect();
    if parts.len() == 2 {
        if let Some(uid) = try_parse_id_prefix(parts[0]) {
            return (parts[1].to_string(), Some(uid));
        }
        if let Some(uid) = try_parse_id_prefix(parts[1]) {
            return (parts[0].to_string(), Some(uid));
        }
    }
    (query.to_string(), reply_user_id)
}

fn try_parse_id_prefix(token: &str) -> Option<i64> {
    token.strip_prefix("id:").and_then(|s| s.parse().ok())
}

fn format_results(result: &SearchResult, chat_id: i64) -> String {
    if result.total == 0 {
        return "未找到相关消息。".to_string();
    }

    let mut text = format!(
        "共找到 <b>{}</b> 条结果（第 {}/{} 页）：\n\n",
        result.total,
        result.page + 1,
        result.total_pages
    );

    for (i, hit) in result.messages.iter().enumerate() {
        let num = result.page * 5 + i + 1;
        let date = chrono::DateTime::from_timestamp(hit.message.date, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_default();

        // Format user info with tg://user?id=xxx link
        let user_info = if let Some(user_id) = hit.message.user_id {
            format!(" | <a href=\"tg://user?id={}\">User {}</a>", user_id, user_id)
        } else {
            String::new()
        };

        let snippet = hit
            .highlight
            .as_deref()
            .map(String::from)
            .unwrap_or_else(|| truncate_html(&hit.message.text, 80));

        let link = format_message_link(chat_id, hit.message.message_id);
        text.push_str(&format!(
            "{num}. <i>{date}</i>{user_info}\n{snippet}\n<a href=\"{link}\">跳转到消息</a>\n\n"
        ));
    }
    text
}

fn truncate_html(s: &str, max_chars: usize) -> String {
    if s.chars().count() > max_chars {
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{}...", html_escape(&truncated))
    } else {
        html_escape(s)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn format_message_link(chat_id: i64, message_id: i64) -> String {
    let abs_id = chat_id.unsigned_abs();
    let channel_id = if abs_id > 1_000_000_000_000 {
        abs_id - 1_000_000_000_000
    } else {
        abs_id
    };
    format!("https://t.me/c/{channel_id}/{message_id}")
}

fn build_keyboard(
    result: &SearchResult,
    state: &SearchState,
    has_user_filter: bool,
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = vec![];

    // Navigation
    if result.total_pages > 1 {
        let mut nav = vec![];
        if result.page > 0 {
            let prev_state = SearchState {
                page: result.page - 1,
                ..state.clone()
            };
            nav.push(InlineKeyboardButton::callback(
                "⬅ 上一页",
                prev_state.encode(),
            ));
        }
        nav.push(InlineKeyboardButton::callback(
            format!("{}/{}", result.page + 1, result.total_pages),
            "noop".to_string(),
        ));
        if result.page + 1 < result.total_pages {
            let next_state = SearchState {
                page: result.page + 1,
                ..state.clone()
            };
            nav.push(InlineKeyboardButton::callback(
                "下一页 ➡",
                next_state.encode(),
            ));
        }
        rows.push(nav);
    }

    // Date filter
    rows.push(
        [("7d", "7天内"), ("30d", "30天内"), ("90d", "90天内"), (
            "all", "全部",
        )]
            .map(|(key, label)| {
                let active = state.date_range == Some(key) || (key == "all" && state.date_range.is_none());
                let text = if active {
                    format!("✓ {label}")
                } else {
                    label.to_string()
                };
                let new_state = SearchState {
                    page: 0,
                    message_type: state.message_type.clone(),
                    date_range: if key == "all" { None } else { Some(key) },
                    user_id: state.user_id,
                };
                InlineKeyboardButton::callback(text, new_state.encode())
            })
            .to_vec(),
    );

    // Message type filter (only show if not filtered by user)
    if !has_user_filter {
        rows.push(
            [
                ("text", "文字"),
                ("photo", "图片"),
                ("video", "视频"),
                ("document", "文件"),
            ]
            .map(|(key, label)| {
                let active = state.message_type.as_deref() == Some(key);
                let text = if active {
                    format!("✓ {label}")
                } else {
                    label.to_string()
                };
                let new_state = SearchState {
                    page: 0,
                    message_type: if active { None } else { Some(key.to_string()) },
                    date_range: state.date_range,
                    user_id: state.user_id,
                };
                InlineKeyboardButton::callback(text, new_state.encode())
            })
            .to_vec(),
        );
    }

    InlineKeyboardMarkup::new(rows)
}

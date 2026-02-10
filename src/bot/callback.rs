use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, ParseMode};

use crate::es::search::{SearchClient, SearchParams, SearchResult};

/// In-memory store for active search sessions.
/// Key: short session hash, Value: SearchParams.
pub type SearchSessions = Arc<DashMap<String, SearchParams>>;

pub fn create_sessions() -> SearchSessions {
    Arc::new(DashMap::new())
}

/// Generate a short session ID from search parameters.
fn session_id(chat_id: i64, keyword: &str) -> String {
    let mut hasher = DefaultHasher::new();
    chat_id.hash(&mut hasher);
    keyword.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .hash(&mut hasher);
    format!("{:x}", hasher.finish())[..8].to_string()
}

/// Handle the /search command: perform initial search and show results with keyboard.
pub async fn handle_search(
    bot: Bot,
    msg: Message,
    query: String,
    search_client: Arc<SearchClient>,
    sessions: SearchSessions,
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

    // Extract user_id from replied-to message (if any)
    let reply_user_id = msg
        .reply_to_message()
        .and_then(|r| r.from.as_ref())
        .map(|u| u.id.0 as i64);

    let (keyword, user_id_filter) = parse_search_query(&query, reply_user_id);

    let params = SearchParams {
        chat_id: chat_id.0,
        keyword: Some(keyword.clone()),
        user_id: user_id_filter,
        page: 0,
        page_size: default_page_size,
        ..Default::default()
    };

    let result = search_client.search(&params).await?;
    let sid = session_id(chat_id.0, &keyword);
    sessions.insert(sid.clone(), params);

    let text = format_results(&result, chat_id.0);
    let keyboard = build_results_keyboard(&result, &sid);

    bot.send_message(chat_id, text)
        .parse_mode(ParseMode::Html)
        .reply_markup(keyboard)
        .await?;

    Ok(())
}

/// Handle inline keyboard callback queries for pagination and filters.
pub async fn handle_callback(
    bot: Bot,
    q: CallbackQuery,
    search_client: Arc<SearchClient>,
    sessions: SearchSessions,
) -> anyhow::Result<()> {
    let data = match q.data {
        Some(ref d) => d.clone(),
        None => return Ok(()),
    };

    bot.answer_callback_query(q.id.clone()).await?;

    let msg = match q.message {
        Some(ref m) => m.clone(),
        None => return Ok(()),
    };

    let parts: Vec<&str> = data.split(':').collect();
    match parts.first() {
        Some(&"p") => {
            if parts.len() >= 3 {
                let page: usize = parts[1].parse().unwrap_or(0);
                let sid = parts[2];

                if let Some(mut params) = sessions.get_mut(sid) {
                    params.page = page;
                    let params_clone = params.clone();
                    drop(params);

                    let result = search_client.search(&params_clone).await?;
                    let text = format_results(&result, params_clone.chat_id);
                    let keyboard = build_results_keyboard(&result, sid);

                    if let Some(id) = msg.regular_message().map(|m| m.id) {
                        bot.edit_message_text(msg.chat().id, id, text)
                            .parse_mode(ParseMode::Html)
                            .reply_markup(keyboard)
                            .await?;
                    }
                }
            }
        }
        Some(&"ft") => {
            if parts.len() >= 3 {
                let msg_type = parts[1];
                let sid = parts[2];

                if let Some(mut params) = sessions.get_mut(sid) {
                    if params.message_type.as_deref() == Some(msg_type) {
                        params.message_type = None;
                    } else {
                        params.message_type = Some(msg_type.to_string());
                    }
                    params.page = 0;
                    let params_clone = params.clone();
                    drop(params);

                    let result = search_client.search(&params_clone).await?;
                    let text = format_results(&result, params_clone.chat_id);
                    let keyboard = build_results_keyboard(&result, sid);

                    if let Some(id) = msg.regular_message().map(|m| m.id) {
                        bot.edit_message_text(msg.chat().id, id, text)
                            .parse_mode(ParseMode::Html)
                            .reply_markup(keyboard)
                            .await?;
                    }
                }
            }
        }
        Some(&"fd") => {
            if parts.len() >= 3 {
                let range = parts[1];
                let sid = parts[2];

                if let Some(mut params) = sessions.get_mut(sid) {
                    let now = chrono::Utc::now().timestamp();
                    match range {
                        "7d" => {
                            params.date_from = Some(now - 7 * 86400);
                            params.date_to = None;
                        }
                        "30d" => {
                            params.date_from = Some(now - 30 * 86400);
                            params.date_to = None;
                        }
                        "90d" => {
                            params.date_from = Some(now - 90 * 86400);
                            params.date_to = None;
                        }
                        _ => {
                            params.date_from = None;
                            params.date_to = None;
                        }
                    }
                    params.page = 0;
                    let params_clone = params.clone();
                    drop(params);

                    let result = search_client.search(&params_clone).await?;
                    let text = format_results(&result, params_clone.chat_id);
                    let keyboard = build_results_keyboard(&result, sid);

                    if let Some(id) = msg.regular_message().map(|m| m.id) {
                        bot.edit_message_text(msg.chat().id, id, text)
                            .parse_mode(ParseMode::Html)
                            .reply_markup(keyboard)
                            .await?;
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

/// Parse search query: extract `id:<user_id>` prefix, or fall back to reply-to user_id.
fn parse_search_query(query: &str, reply_user_id: Option<i64>) -> (String, Option<i64>) {
    let parts: Vec<&str> = query.splitn(2, ' ').collect();

    // Check id:123456 as first token
    if parts.len() == 2 {
        if let Some(uid) = try_parse_id_prefix(parts[0]) {
            return (parts[1].to_string(), Some(uid));
        }
        if let Some(uid) = try_parse_id_prefix(parts[1]) {
            return (parts[0].to_string(), Some(uid));
        }
    }

    // Fall back to reply-to user
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

        let snippet = if let Some(ref hl) = hit.highlight {
            hl.clone()
        } else {
            let t = &hit.message.text;
            if t.chars().count() > 80 {
                let truncated: String = t.chars().take(80).collect();
                format!("{truncated}...")
            } else {
                html_escape(t)
            }
        };

        let link = format_message_link(chat_id, hit.message.message_id);

        text.push_str(&format!(
            "{num}. <i>{date}</i>\n{snippet}\n<a href=\"{link}\">跳转到消息</a>\n\n"
        ));
    }

    text
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

fn build_results_keyboard(result: &SearchResult, session_id: &str) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = vec![];

    let mut nav_row = vec![];
    if result.page > 0 {
        nav_row.push(InlineKeyboardButton::callback(
            "⬅ 上一页",
            format!("p:{}:{session_id}", result.page - 1),
        ));
    }
    nav_row.push(InlineKeyboardButton::callback(
        format!("{}/{}", result.page + 1, result.total_pages),
        "noop".to_string(),
    ));
    if result.page + 1 < result.total_pages {
        nav_row.push(InlineKeyboardButton::callback(
            "下一页 ➡",
            format!("p:{}:{session_id}", result.page + 1),
        ));
    }
    if nav_row.len() > 1 || result.total_pages > 1 {
        rows.push(nav_row);
    }

    rows.push(vec![
        InlineKeyboardButton::callback("7天内", format!("fd:7d:{session_id}")),
        InlineKeyboardButton::callback("30天内", format!("fd:30d:{session_id}")),
        InlineKeyboardButton::callback("90天内", format!("fd:90d:{session_id}")),
        InlineKeyboardButton::callback("全部", format!("fd:all:{session_id}")),
    ]);

    rows.push(vec![
        InlineKeyboardButton::callback("文字", format!("ft:text:{session_id}")),
        InlineKeyboardButton::callback("图片", format!("ft:photo:{session_id}")),
        InlineKeyboardButton::callback("视频", format!("ft:video:{session_id}")),
        InlineKeyboardButton::callback("文件", format!("ft:document:{session_id}")),
    ]);

    InlineKeyboardMarkup::new(rows)
}

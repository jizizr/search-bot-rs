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
    // Use current time to allow multiple searches with same keyword
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
        bot.send_message(chat_id, "用法: /search <关键词>\n\n示例:\n/search 你好\n/search @username 关键词")
            .await?;
        return Ok(());
    }

    // Parse query: detect @username prefix
    let (keyword, username) = parse_search_query(&query);

    let params = SearchParams {
        chat_id: chat_id.0,
        keyword: Some(keyword.clone()),
        username,
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

    // Answer callback to dismiss loading indicator
    bot.answer_callback_query(&q.id).await?;

    let msg = match q.message {
        Some(ref m) => m.clone(),
        None => return Ok(()),
    };

    let parts: Vec<&str> = data.split(':').collect();
    match parts.first() {
        Some(&"p") => {
            // Pagination: p:{page}:{session_id}
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
            // Filter by message type: ft:{type}:{session_id}
            if parts.len() >= 3 {
                let msg_type = parts[1];
                let sid = parts[2];

                if let Some(mut params) = sessions.get_mut(sid) {
                    // Toggle: if same type is set, remove it; otherwise set it
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
            // Filter by date range: fd:{range}:{session_id}
            // range: "7d", "30d", "all"
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
                            // "all" - remove date filter
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
        _ => {
            // noop or unknown callback
        }
    }

    Ok(())
}

fn parse_search_query(query: &str) -> (String, Option<String>) {
    let parts: Vec<&str> = query.splitn(2, ' ').collect();
    if parts.len() == 2 && parts[0].starts_with('@') {
        let username = parts[0].trim_start_matches('@').to_string();
        let keyword = parts[1].to_string();
        (keyword, Some(username))
    } else if parts.len() == 2 && parts[1].starts_with('@') {
        let keyword = parts[0].to_string();
        let username = parts[1].trim_start_matches('@').to_string();
        (keyword, Some(username))
    } else {
        (query.to_string(), None)
    }
}

fn format_results(result: &SearchResult, chat_id: i64) -> String {
    if result.total == 0 {
        return "未找到相关消息。".to_string();
    }

    let mut text = format!(
        "共找到 <b>{}</b> 条结果（第 {}/{}  页）：\n\n",
        result.total,
        result.page + 1,
        result.total_pages
    );

    for (i, hit) in result.messages.iter().enumerate() {
        let num = result.page * 5 + i + 1;
        let name = html_escape(&hit.message.display_name);
        let date = chrono::DateTime::from_timestamp(hit.message.date, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_default();

        // Use highlight if available, otherwise truncate text
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
            "{num}. <b>{name}</b>  <i>{date}</i>\n{snippet}\n<a href=\"{link}\">跳转到消息</a>\n\n"
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
    // Telegram supergroup IDs: -100{channel_id}
    // Link format: https://t.me/c/{channel_id}/{message_id}
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

    // Row 1: Pagination
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

    // Row 2: Date range filters
    rows.push(vec![
        InlineKeyboardButton::callback("7天内", format!("fd:7d:{session_id}")),
        InlineKeyboardButton::callback("30天内", format!("fd:30d:{session_id}")),
        InlineKeyboardButton::callback("90天内", format!("fd:90d:{session_id}")),
        InlineKeyboardButton::callback("全部", format!("fd:all:{session_id}")),
    ]);

    // Row 3: Message type filters
    rows.push(vec![
        InlineKeyboardButton::callback("文字", format!("ft:text:{session_id}")),
        InlineKeyboardButton::callback("图片", format!("ft:photo:{session_id}")),
        InlineKeyboardButton::callback("视频", format!("ft:video:{session_id}")),
        InlineKeyboardButton::callback("文件", format!("ft:document:{session_id}")),
    ]);

    InlineKeyboardMarkup::new(rows)
}

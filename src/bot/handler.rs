use std::net::SocketAddr;
use std::sync::Arc;
use teloxide::dispatching::UpdateFilterExt;
use teloxide::prelude::*;
use teloxide::update_listeners::webhooks;
use teloxide::utils::command::BotCommands;

use crate::bot::callback::{
    create_sessions, handle_callback, handle_search, SearchSessions,
};
use crate::bot::commands::Command;
use crate::bot::message_recorder::record_message;
use crate::config::WebhookConfig;
use crate::es::indexer::BatchIndexer;
use crate::es::search::SearchClient;
use crate::models::user_cache::UserCache;

pub async fn run_bot(
    bot: Bot,
    indexer: Arc<BatchIndexer>,
    search_client: Arc<SearchClient>,
    user_cache: UserCache,
    default_page_size: usize,
    webhook_config: WebhookConfig,
) -> anyhow::Result<()> {
    let sessions = create_sessions();

    let handler = dptree::entry()
        // Branch 1: Handle callback queries (inline keyboard presses)
        .branch(Update::filter_callback_query().endpoint(
            |bot: Bot,
             q: CallbackQuery,
             search_client: Arc<SearchClient>,
             sessions: SearchSessions,
             user_cache: UserCache| async move {
                handle_callback(bot, q, search_client, sessions, user_cache).await?;
                Ok::<(), anyhow::Error>(())
            },
        ))
        // Branch 2: Handle commands
        .branch(
            Update::filter_message()
                .filter_command::<Command>()
                .endpoint(
                    |bot: Bot,
                     msg: Message,
                     cmd: Command,
                     search_client: Arc<SearchClient>,
                     sessions: SearchSessions,
                     user_cache: UserCache,
                     _indexer: Arc<BatchIndexer>,
                     default_page_size: usize| async move {
                        // Update user cache from command senders too
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
                        match cmd {
                            Command::Search(query) => {
                                handle_search(
                                    bot,
                                    msg,
                                    query,
                                    search_client,
                                    sessions,
                                    user_cache,
                                    default_page_size,
                                )
                                .await?;
                            }
                            Command::Help => {
                                bot.send_message(msg.chat.id, Command::descriptions().to_string())
                                    .await?;
                            }
                        }
                        Ok::<(), anyhow::Error>(())
                    },
                ),
        )
        // Branch 3: Record all other messages (catch-all, must be last)
        .branch(Update::filter_message().endpoint(
            |msg: Message, indexer: Arc<BatchIndexer>, user_cache: UserCache| async move {
                record_message(msg, indexer, user_cache).await?;
                Ok::<(), anyhow::Error>(())
            },
        ));

    let mut dispatcher = Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![
            indexer,
            search_client,
            sessions,
            user_cache,
            default_page_size
        ])
        .default_handler(|_| async {})
        .error_handler(LoggingErrorHandler::new())
        .enable_ctrlc_handler()
        .build();

    if webhook_config.is_enabled() {
        // Production: webhook mode
        let addr: SocketAddr =
            format!("{}:{}", webhook_config.listen_addr, webhook_config.port).parse()?;
        let webhook_url: url::Url = webhook_config.url.parse()?;

        let listener = webhooks::axum(bot, webhooks::Options::new(addr, webhook_url))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create webhook listener: {e}"))?;

        tracing::info!("Webhook listener bound to {addr}");

        dispatcher
            .dispatch_with_listener(
                listener,
                LoggingErrorHandler::with_custom_text("Webhook listener error"),
            )
            .await;
    } else {
        // Debug: long polling mode
        tracing::info!("Running in long-polling mode (no WEBHOOK_URL set)");
        dispatcher.dispatch().await;
    }

    Ok(())
}

use std::net::SocketAddr;
use std::sync::Arc;
use teloxide::dispatching::UpdateFilterExt;
use teloxide::prelude::*;
use teloxide::update_listeners::webhooks;
use teloxide::utils::command::BotCommands;

use crate::bot::callback::{handle_callback, handle_search};
use crate::bot::commands::Command;
use crate::bot::message_recorder::record_message;
use crate::config::WebhookConfig;
use crate::es::indexer::BatchIndexer;
use crate::es::search::SearchClient;

pub async fn run_bot(
    bot: Bot,
    indexer: Arc<BatchIndexer>,
    search_client: Arc<SearchClient>,
    default_page_size: usize,
    webhook_config: WebhookConfig,
) -> anyhow::Result<()> {
    let handler = dptree::entry()
        .branch(Update::filter_callback_query().endpoint(
            |bot: Bot,
             q: CallbackQuery,
             search_client: Arc<SearchClient>,
             default_page_size: usize| async move {
                handle_callback(bot, q, search_client, default_page_size).await
            },
        ))
        .branch(
            Update::filter_message()
                .filter_command::<Command>()
                .endpoint(
                    |bot: Bot,
                     msg: Message,
                     cmd: Command,
                     search_client: Arc<SearchClient>,
                     _indexer: Arc<BatchIndexer>,
                     default_page_size: usize| async move {
                        match cmd {
                            Command::Search(query) => {
                                handle_search(bot, msg, query, search_client, default_page_size)
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
        .branch(Update::filter_message().endpoint(
            |msg: Message, indexer: Arc<BatchIndexer>| async move {
                record_message(msg, indexer).await
            },
        ));

    let mut dispatcher = Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![indexer, search_client, default_page_size])
        .default_handler(|_| async {})
        .error_handler(LoggingErrorHandler::new())
        .enable_ctrlc_handler()
        .build();

    if webhook_config.is_enabled() {
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
        dispatcher.dispatch().await;
    }

    Ok(())
}

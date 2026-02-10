use teloxide::utils::command::BotCommands;

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "可用命令：")]
pub enum Command {
    #[command(description = "搜索群组消息：/s <关键词>", aliases = ["s"])]
    Search(String),

    #[command(description = "显示帮助信息", aliases = ["h"])]
    Help,
}

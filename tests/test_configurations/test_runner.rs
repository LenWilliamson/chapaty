use chapaty::{error::ChapatyErrorKind, Bot, BotBuilder, DataFrame};

use super::{bot_config::BotConfig, test_result::TestResult};

pub struct TestRunner {
    config: BotConfig,
}

impl TestRunner {
    pub fn new(config: BotConfig) -> Self {
        Self { config }
    }

    pub fn setup(&self) -> Result<Bot, ChapatyErrorKind> {
        let bot_builder: BotBuilder = self.config.clone().into();
        bot_builder.build()
    }

    pub async fn run(&self, bot: Bot) -> TestResult {
        let backtest_result = bot.backtest().await;

        TestResult {
            market: self.config.market,
            year: self.config.year,
            backtest_result,
        }
    }
}

pub fn assert(result: TestResult, expected_pnl: DataFrame) {
    let result_pnl = result
        .backtest_result
        .market_and_year
        .pnl_statement
        .pnl_data
        .get(&result.market)
        .unwrap()
        .reports
        .get(&result.year)
        .unwrap()
        .clone();

    assert_eq!(
        expected_pnl.equals(&result_pnl),
        true,
        "Test failed: expected {expected_pnl:?}, got {result_pnl:?}"
    );
}

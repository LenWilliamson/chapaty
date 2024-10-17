#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum StrategyKind {
    NewsCounter,
    NewsRasslerWithConfirmation,
    NewsRassler,
    Ppp,
}

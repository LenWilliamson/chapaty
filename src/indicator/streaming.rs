pub mod fair_value_gap;
pub mod momentum;
pub mod moving_averages;
pub mod oscillators;
pub mod session;
pub mod swing;
pub mod timing;
pub mod volatility;

pub trait StreamingIndicator: std::fmt::Debug + Send + Sync {
    type Input;
    type Output<'a>
    where
        Self: 'a;

    /// Update the indicator with the latest data point.
    fn update(&mut self, input: Self::Input) -> Self::Output<'_>;

    /// Reset the internal state to clear history (e.g., for a new trading
    /// session).
    fn reset(&mut self);
}

/// A generic trait for incremental indicators.
/// Designed to be object-safe so agents can hold `Box<dyn StreamingIndicator<Input=I, Output=O>>`.
pub trait StreamingIndicator: std::fmt::Debug + Send + Sync {
    type Input;
    type Output;

    /// Update the indicator with the latest data point.
    fn update(&mut self, input: Self::Input) -> Self::Output;

    /// Reset the internal state to clear history (e.g., for a new trading session).
    fn reset(&mut self);
}

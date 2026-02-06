use rand::Rng;
use crate::gym::flow::domain::{BasisPoints, ClientTier, Side};

/// Configuration for the probabilistic fill model (Logistic Function).
#[derive(Debug, Clone, Copy)]
pub struct FillModelConfig {
    /// A global scaling factor for market volatility or regime.
    ///
    /// - `1.0`: Standard market conditions.
    /// - `>1.0`: High competition/transparency (Harder to fill).
    /// - `<1.0`: Low competition/opaque market (Easier to fill).
    pub global_sensitivity_scaler: f64,

    /// The "Market Maker Bias" in basis points.
    ///
    /// Represents the spread a client is willing to pay for liquidity even at Fair Value.
    /// - `Negative (-2.0)`: Client accepts a price 2bps worse than Mid.
    /// - `Zero (0.0)`: Client only trades at strictly Fair Value.
    pub bias_bps: f64,
}

impl Default for FillModelConfig {
    fn default() -> Self {
        Self {
            global_sensitivity_scaler: 1.0,
            bias_bps: -2.0, 
        }
    }
}

#[derive(Clone, Debug)]
pub struct FillSimulator {
    config: FillModelConfig,
}

impl FillSimulator {
    pub fn new(config: FillModelConfig) -> Self {
        Self { config }
    }

    /// Determines whether the client accepts the quoted price.
    ///
    /// Uses a sigmoid (logistic) function:
    /// $$ P(\text{Accept}) = \frac{1}{1 + e^{-\alpha \cdot (\text{Advantage} + \text{Bias})}} $$
    pub fn decide(
        &self,
        rng: &mut impl Rng,
        side: Side,         // Assuming Side is defined in domain
        my_quote: f64,
        mid_price: f64,
        tier: ClientTier,
    ) -> bool {
        // 1. Calculate Advantage in Basis Points
        // We use f64 logic here to capture fractional bps precision.
        let price_delta = match side {
            Side::Buy => mid_price - my_quote, // Client buys: Lower quote is better
            Side::Sell => my_quote - mid_price, // Client sells: Higher quote is better
        };

        let advantage = BasisPoints::from_price_diff(price_delta, mid_price);

        // 2. Determine Alpha (Steepness)
        // Combine the Tier's intrinsic nature with the global market scaler.
        let alpha = tier.intrinsic_sensitivity() * self.config.global_sensitivity_scaler;

        // 3. Compute Logistic Probability
        // x = -alpha * (advantage + bias)
        let exponent = -alpha * (advantage.value() + self.config.bias_bps);
        let probability = 1.0 / (1.0 + exponent.exp());

        // 4. Roll the dice
        rng.random_bool(probability)
    }
}
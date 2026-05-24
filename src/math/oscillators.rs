use serde::{Deserialize, Serialize};

use crate::math::{StreamingIndicator, moving_averages::StreamingEwm};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRsi {
    prev_price: Option<f64>,
    avg_gain: StreamingEwm,
    avg_loss: StreamingEwm,
}

impl StreamingRsi {
    pub fn new(window_size: u16) -> Self {
        // Wilder's Smoothing Alpha = 1 / N
        // This differs from standard EMA!
        let alpha = 1.0 / (window_size as f64);
        let win = window_size as usize;

        Self {
            prev_price: None,
            avg_gain: StreamingEwm::new(alpha, win),
            avg_loss: StreamingEwm::new(alpha, win),
        }
    }
}

impl StreamingIndicator for StreamingRsi {
    type Input = f64;
    type Output = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output {
        let prev = match self.prev_price {
            Some(p) => p,
            None => {
                // First trade: just store the price, we cannot calculate delta yet
                self.prev_price = Some(value);
                return None;
            }
        };

        // 1. Calculate Delta
        let delta = value - prev;
        self.prev_price = Some(value);

        // 2. Separate Gain/Loss
        let (gain, loss) = if delta > 0.0 {
            (delta, 0.0)
        } else {
            (0.0, delta.abs())
        };

        // 3. Update Wilder's Smoothers
        // We capture the Option from both. If both are Some, we have enough data.
        let g_val = self.avg_gain.update(gain);
        let l_val = self.avg_loss.update(loss);

        match (g_val, l_val) {
            (Some(avg_gain), Some(avg_loss)) => {
                // 4. Calculate RSI
                // Prevent division by zero if avg_loss is 0 (Monotonic Up-trend)
                if avg_loss == 0.0 {
                    if avg_gain == 0.0 {
                        // Flat line
                        Some(50.0)
                    } else {
                        // Pure gain
                        Some(100.0)
                    }
                } else {
                    let rs = avg_gain / avg_loss;
                    Some(100.0 - (100.0 / (1.0 + rs)))
                }
            }
            _ => None,
        }
    }

    fn reset(&mut self) {
        self.prev_price = None;
        self.avg_gain.reset();
        self.avg_loss.reset();
    }
}

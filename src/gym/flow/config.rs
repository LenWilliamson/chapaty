use rand::Rng;
use rand_distr::{Distribution, LogNormal, Normal, Uniform};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Wahrscheinlichkeit, dass der Kunde auf ein Quote antwortet (0.0 - 1.0).
    pub response_rate: f64,

    /// Latenz-Modellierung.
    pub latency_model: LatencyModel,

    /// Fill-Modell Parameter (Logistic Function).
    pub fill_model: FillModelConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            response_rate: 0.95,
            latency_model: LatencyModel::LogNormal {
                mu: 4.6,    // exp(4.6) ≈ 100ms
                sigma: 0.5, // Shape parameter
            },
            fill_model: FillModelConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillModelConfig {
    /// Globaler Skalierer für Volatilität/Wettbewerb.
    /// > 1.0 = Markt ist härter (Kunden sind preissensibler).
    pub global_sensitivity_scaler: f64,

    /// Bias in Basispunkten.
    /// Wie viel Spread zahlt der Kunde "freiwillig" (Market Taker Fee).
    /// -2.0 bps bedeutet, der Kunde akzeptiert Preise 2bps schlechter als Mid.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LatencyModel {
    /// Feste Latenz (Determinismus für Tests).
    Fixed { ms: u64 },
    /// Gleichverteilung.
    Uniform { min: u64, max: u64 },
    /// Gauß-Verteilung (Achtung: Kann theoretisch negativ werden, wir cappen bei 1ms).
    Normal { mean: f64, std: f64 },
    /// Log-Normalverteilung (Realistisch für Netzwerke).
    /// Parameter sind für den zugrundeliegenden Logarithmus (mu, sigma).
    LogNormal { mu: f64, sigma: f64 },
}

// TODO replace all .unwrap()
impl ClientConfig {
    pub fn sample_latency<R: Rng + ?Sized>(&self, rng: &mut R) -> i64 {
        let val = match self.latency_model {
            LatencyModel::Fixed { ms } => ms as f64,
            LatencyModel::Uniform { min, max } => Uniform::new_inclusive(min as f64, max as f64)
                .unwrap()
                .sample(rng),
            LatencyModel::Normal { mean, std } => {
                let dist = Normal::new(mean, std).unwrap();
                dist.sample(rng)
            }
            LatencyModel::LogNormal { mu, sigma } => {
                let dist = LogNormal::new(mu, sigma).unwrap();
                dist.sample(rng)
            }
        };
        // Latenz darf nicht negativ sein (Physik!) und sollte mind. 1ms betragen.
        val.max(1.0) as i64
    }
}

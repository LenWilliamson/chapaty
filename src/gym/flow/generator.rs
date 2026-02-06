use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use chrono::Duration;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rand_distr::{Distribution, Pareto};
use serde::{Deserialize, Serialize};

use crate::{
    agent::AgentIdentifier,
    data::{
        domain::{Currency, Symbol},
        event::Trade,
    },
    gym::flow::{
        domain::{ClientTier, QuoteMode, RfqId, SettlementType, Side},
        state::{Open, Rfq},
    },
};

/// Defines the strategy used to determine the asset class and instrument specifications of the generated RFQ.
///
/// This setting allows the engine to operate in two distinct modes:
/// 1. **Data-Driven (MVP):** Using the actual symbols from the feed (e.g., Crypto Spot).
/// 2. **Behavior-Driven (Pitch):** Using the feed only as a clock/signal, while simulating a different asset class (e.g., Bonds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AssetGenerationMode {
    /// **Pass-Through Mode (Crypto / Spot).**
    ///
    /// The generator utilizes the exact symbol found in the underlying market data feed (e.g., `BTC-USDT`).
    /// - **Instrument:** Spot Pair or Future (as defined in data).
    /// - **Settlement:** T+0 (Standard for Crypto).
    /// - **Quoting:** Price (Percentage or Absolute).
    ///
    /// *Use Case:* Algo development, MVP backtesting on real crypto data.
    #[default]
    UseUnderlyingSymbol,

    /// **Synthetic Fixed Income Mode (Bond Simulation).**
    ///
    /// The generator uses the underlying trade timestamp as a **volatility signal**, but hallucinates
    /// a fictional Government or Corporate Bond (ISIN) as the traded instrument.
    ///
    /// - **Instrument:** Synthetic Bond ISIN (e.g., `US123...`).
    /// - **Settlement:** T+1 or T+2 (Standard for Bonds).
    /// - **Quoting:** Mixed (Price or Yield).
    ///
    /// *Use Case:* Demonstrating platform capabilities for institutional Fixed Income desks using high-frequency crypto data as a proxy.
    SyntheticFixedIncome,
}

pub struct GeneratorConfig {
    pub global_seed: u64,

    /// **Base Arrival Rate.**
    /// The baseline probability that any given market trade triggers an RFQ,
    /// regardless of size. Represents "background noise".
    pub base_probability: f64, // z.B. 0.005 (0.5%)

    /// **Volume Sensitivity Factor.**
    /// How much the trade size increases the probability of an RFQ.
    /// Formula: `P = base + (trade_qty * sensitivity)`.
    /// High values mean the desk reacts strongly to large market prints.
    pub volume_sensitivity: f64, // z.B. 0.01 per Unit

    /// Multiplier to scale market trade size to OTC block size.
    pub quantity_multiplier: f64,

    /// Determines the asset class strategy for the simulation.
    /// Replaces the boolean flag for better readability and extensibility.
    pub asset_mode: AssetGenerationMode,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            global_seed: 0,
            base_probability: 0.005,
            volume_sensitivity: 0.001,
            quantity_multiplier: 10.0,
            asset_mode: AssetGenerationMode::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RfqGenerator {
    config: GeneratorConfig,
}

impl RfqGenerator {
    pub fn new(config: GeneratorConfig) -> Self {
        Self { config }
    }

    /// Transforms a public market trade into a private client RFQ.
    ///
    /// # Arguments
    /// * `trade` - The trade event from the market data feed.
    /// * `source_symbol` - The symbol of the market where this trade occurred (context).
    pub fn try_generate(&self, trade: &Trade, source_symbol: &Symbol) -> Option<Rfq<Open>> {
        // 1. Deterministic Seeding (Trade-bound)
        let mut hasher = DefaultHasher::new();
        self.config.global_seed.hash(&mut hasher);
        trade.trade_id.hash(&mut hasher);
        trade
            .timestamp
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .hash(&mut hasher);
        let local_seed = hasher.finish();

        let mut rng = StdRng::seed_from_u64(local_seed);

        // 2. Dynamic Probability Logic
        // Logic: Large public trades trigger institutional anxiety/FOMO.
        let volume_impact = trade.quantity.0 * self.config.volume_sensitivity;
        let dynamic_probability = (self.config.base_probability + volume_impact).clamp(0.0, 1.0);

        // Probability Gate
        if !rng.gen_bool(dynamic_probability) {
            return None;
        }

        // 3. Side Logic (Inverse Pressure)
        let side = if trade.is_buyer_maker {
            Side::Sell
        } else {
            Side::Buy
        };

        // 4. Quantity Logic (Fat Tail Distribution)
        let base_qty = trade.quantity.0 * self.config.quantity_multiplier;

        // Pareto Distribution (80-20 rule)
        let pareto = Pareto::new(1.0, 1.16).unwrap_or_else(|_| Pareto::new(1.0, 1.0).unwrap());
        let size_factor: f64 = pareto.sample(&mut rng);

        // Clamp to prevent overflow or unrealistic sizes
        let quantity = (base_qty * size_factor).clamp(base_qty, base_qty * 50.0);

        // 5. Client Segmentation (Deterministic Identity)
        // We pick one of the 1000 simulated clients.
        let client_id_seed = rng.gen_range(0..1000);

        // The Tier is an intrinsic property of the ID.
        let client_tier = Self::determine_tier_from_id(client_id_seed);

        // Generate a stable name (e.g., "Client_0042")
        let client_name = format!("Client_{:04}", client_id_seed);
        let client_id = AgentIdentifier::Named(Arc::new(client_name));

        // 6. Asset Class & Instrument Logic
        let (symbol, settlement, quote_mode) = match self.config.asset_mode {
            AssetGenerationMode::SyntheticFixedIncome => {
                let bond = self.generate_bond_spec(&mut rng);
                let sett = if bond.currency == Currency::USD {
                    SettlementType::T1
                } else {
                    SettlementType::T2
                };
                let mode = if rng.gen_bool(0.3) {
                    QuoteMode::Yield
                } else {
                    QuoteMode::Price
                };

                (Symbol::Bond(bond), sett, mode)
            }

            AssetGenerationMode::UseUnderlyingSymbol => {
                let settlement = SettlementType::T0;
                let quote_mode = QuoteMode::Price;
                (source_symbol.clone(), settlement, quote_mode)
            }
        };

        // 7. Timing & TTL
        let created_at_ts = trade.timestamp.timestamp_millis() as u64;
        let ttl_seconds = rng.random_range(5..=30);
        let time_to_live = trade.timestamp + Duration::seconds(ttl_seconds);

        Some(Rfq {
            rfq_id: RfqId(local_seed),
            revision_id: 0,

            // BUGFIX HERE: Use the generated unique ID, not the tier grouping!
            client_id,

            client_tier,
            symbol,
            side,
            quantity: (quantity * 100.0).round() / 100.0,
            settlement,
            quote_mode,
            created_at: created_at_ts,
            time_to_live,
            state: Open,
        })
    }

    /// Helper: Deterministically maps a numeric seed to a client tier.
    /// This ensures consistent behavior: Client ID X is ALWAYS Tier Y.
    fn determine_tier_from_id(client_id_seed: u64) -> ClientTier {
        // Simulate a universe of 1000 distinct institutional clients.
        let slot = client_id_seed % 1000;

        match slot {
            0..=99 => ClientTier::Tier1,    // 10% Hedge Funds (IDs 0-99)
            100..=499 => ClientTier::Tier2, // 40% Asset Managers (IDs 100-499)
            _ => ClientTier::Tier3,         // 50% Retail/Corporate (IDs 500-999)
        }
    }

    /// Helper to generate synthetic bond specifications.
    fn generate_bond_spec(&self, rng: &mut StdRng) -> BondSpec {
        let is_us = rng.gen_bool(0.5);

        if is_us {
            BondSpec {
                isin: format!("US{:09}", rng.gen::<u64>() % 1_000_000_000),
                currency: Currency::USD,
                tick_size: OrderedFloat(0.015625), // 1/64
                face_value: OrderedFloat(100.0),
            }
        } else {
            BondSpec {
                isin: format!("DE{:09}", rng.gen::<u64>() % 1_000_000_000),
                currency: Currency::EUR,
                tick_size: OrderedFloat(0.01),
                face_value: OrderedFloat(100.0),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GeneratorConfig {
    global_seed: u64,
    probability: f64,
    quantity_multiplier: f64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            global_seed: u64::default(),
            probability: 0.01,
            quantity_multiplier: 10.0,
        }
    }
}

impl GeneratorConfig {
    fn with_global_seed(self, global_seed: u64) -> Self {
        Self {
            global_seed,
            ..self
        }
    }

    // clamps to 0.0 and 1.0
    fn with_probability(self, probability: f64) -> Self {
        Self {
            probability: probability.clamp(0.0, 1.0),
            ..self
        }
    }

    fn with_quantity_multiplier(self, quantity_multiplier: f64) -> Self {
        Self {
            quantity_multiplier,
            ..self
        }
    }
}

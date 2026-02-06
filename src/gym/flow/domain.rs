use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{agent::AgentIdentifier, data::domain::{LiquiditySide, Symbol}, impl_from_primitive};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct RfqId(pub u64);
impl_from_primitive!(RfqId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct RevisionId(pub u64);
impl_from_primitive!(RevisionId, u64);

/// Represents the desk's realized capital (Free Capital).
///
/// **Relationship to PnL:**
/// - **Realized PnL:** When a trade is finalized (filled), the profit or loss is immediately booked into `Cash`.
/// - **Unrealized PnL:** Tracks the Mark-to-Market value of the `Inventory` but does NOT affect `Cash` until liquidation.
///
/// *Simulation Note:* We currently simplify settlement mechanics. Proceeds from sales are available immediately
/// for new trading, assuming the desk has a sufficient credit line or prime brokerage facility.
#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Cash(pub f64);

/// Represents the desk's current holding of assets (Position Keeping).
///
/// Unlike a simple scalar, an institutional inventory must track positions
/// per unique instrument (`Symbol`).
///
/// - **Positive Quantity:** Long Position (We own the asset).
/// - **Negative Quantity:** Short Position (We owe the asset / Sold short).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Inventory {
    /// Maps the instrument identifier to the signed quantity held.
    /// Key: Symbol (e.g., `BTC-USDT` or `DE000...`)
    /// Value: Quantity (e.g., `+50.0` or `-10,000,000.0`)
    pub positions: HashMap<Symbol, f64>,
}

impl Inventory {
    /// Updates the position for a specific symbol.
    /// Returns the new net quantity.
    pub fn update(&mut self, symbol: Symbol, delta: f64) -> f64 {
        let entry = self.positions.entry(symbol).or_insert(0.0);
        *entry += delta;
        
        // Optional: Clean up zero positions to save memory/compute
        if entry.abs() < f64::EPSILON {
            self.positions.remove(&symbol);
            0.0
        } else {
            *entry
        }
    }

    /// Returns the current position for a symbol (0.0 if flat).
    pub fn get(&self, symbol: &Symbol) -> f64 {
        *self.positions.get(symbol).unwrap_or(&0.0)
    }
}

/// Represents the static master data (KYC) associated with a counterparty.
///
/// This struct serves as the "Credit File" for the agent. It defines WHO the client is,
/// HOW sophisticated they are (Tier), and WHAT the risk limits are.
///
/// # Risk Management
/// This data is immutable during a trading session and is used to validate
/// incoming RFQs against the `client_exposure` tracked in the state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientProfile {
    /// Unique identifier for the counterparty (e.g., LEI or internal ID).
    pub id: AgentIdentifier,

    /// **Client Segmentation.**
    /// Determines the pricing model sensitivity and fill probability.
    /// - `Tier1`: Tight spreads, high information ratio.
    /// - `Tier3`: Wide spreads, flow trading.
    pub tier: ClientTier,

    /// **Credit Limit (Counterparty Risk).**
    /// The maximum allowable net exposure (in base currency, e.g., USD) for this client.
    ///
    /// *Usage:* Before responding to an RFQ, the agent must check:
    /// `current_exposure + new_trade_value <= max_credit_limit`.
    pub max_credit_limit: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Side {
    /// The client wants to BUY from us. We must ASK (Offer).
    Buy,
    /// The client wants to SELL to us. We must BID.
    Sell,
}

impl Side {
    pub fn liquidity_side(&self) -> LiquiditySide {
        self.into()
    }
}

impl From<Side> for LiquiditySide {
    fn from(value: Side) -> Self {
        match value {
            Side::Buy => Self::Ask,
            Side::Sell => Self::Bid,
        }
    }
}

impl From<&Side> for LiquiditySide {
    fn from(value: &Side) -> Self {
        (*value).into()
    }
}

/// Represents a value in Basis Points (1/100th of 1%).
/// Keeps internal precision as f64 to support fractional bps (e.g., 0.5 bps).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BasisPoints(pub f64);

impl BasisPoints {
    /// Converts a raw price delta (relative to mid) into basis points.
    pub fn from_price_diff(price_delta: f64, mid_price: f64) -> Self {
        // 10,000 bps = 100%
        Self((price_delta / mid_price) * 10_000.0)
    }

    /// Access inner value
    pub fn value(&self) -> f64 {
        self.0
    }
}

/// Defines when the trade obligations (delivery of asset vs payment of cash) are settled.
///
/// In institutional fixed income markets, the settlement convention affects the **Dirty Price**
/// calculation due to **Cost of Carry** and **Accrued Interest**.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum SettlementType {
    /// **T+0 (Cash / Same Day):** Settlement occurs on the trade date.
    /// Common for US Treasuries or repo transactions.
    T0,
    
    /// **T+1 (Tom / Tomorrow):** Settlement occurs on the next business day.
    T1,
    
    /// **T+2 (Spot):** Settlement occurs two business days after the trade date.
    /// Standard convention for most FX spot pairs and European Corporate Bonds.
    T2,
    
    /// **Forward / Custom Date:** Settlement occurs on a specific future date.
    /// Used for forward contracts or non-standard OTC agreements.
    Date(DateTime<Utc>),
}

/// Specifies the convention used to quote the price of the instrument.
///
/// Since bond prices, yields, and spreads are mathematically linked, traders may negotiate
/// on any of these terms depending on the asset class and market conditions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum QuoteMode {
    /// **Percentage of Par (Clean Price).**
    ///
    /// The standard quoting convention for most European government and corporate bonds.
    /// Example: A quote of `98.50` means 98.50% of the face value.
    /// Note: Settlement usually requires adding accrued interest (Dirty Price).
    Price,

    /// **Yield to Maturity (YTM).**
    ///
    /// The standard quoting convention for US Treasuries and many High Yield bonds.
    /// Represents the annualized return if held to maturity.
    /// *Logic:* Lower price = Higher yield.
    Yield,

    /// **Spread over Benchmark (Basis Points).**
    ///
    /// Quoted as an offset to a reference curve (e.g., "+50 bps over Bunds").
    /// Common for new issuances and corporate credit trading to hedge interest rate risk.
    Spread,
}

/// Classifies the counterparty based on their sophistication and trading behavior.
///
/// This classification is the primary driver for the **Probabilistic Fill Model**.
/// It determines the spread (margin) the agent can capture and the risk of
/// "toxic flow" (adverse selection).
///
/// # Market Structure Context
///
/// - **Tier 1 (Sharp Flow):** High information asymmetry. If they trade, they likely know something you don't.
///   Requires tight spreads, resulting in lower margins but higher volume.
/// - **Tier 3 (Soft Flow):** Low information asymmetry. Driven by liquidity needs rather than alpha.
///   Allows for wider spreads and higher profitability per trade.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ClientTier {
    /// **Hedge Funds, HFTs, and Prop Trading Desks.**
    ///
    /// Represents highly sophisticated participants with low latency and precise pricing models.
    ///
    /// * **Behavior:** extremely price-sensitive.
    /// * **Fill Probability:** drops sharply if the quote deviates even slightly from Fair Value.
    /// * **Risk:** High probability of short-term adverse price movement after execution ("Toxic Flow").
    Tier1,

    /// **Asset Managers, Pension Funds, and Insurance Companies (Real Money).**
    ///
    /// Represents institutional participants trading primarily for rebalancing or hedging purposes
    /// rather than pure arbitrage.
    ///
    /// * **Behavior:** Moderately price-sensitive, often volume-weighted.
    /// * **Fill Probability:** Standard logistic curve.
    Tier2,

    /// **Corporate Treasuries, Private Banks, and Retail Aggregators.**
    ///
    /// Represents "uninformed" flow driven by commercial needs (e.g., FX conversion for import/export)
    /// rather than speculative intent.
    ///
    /// * **Behavior:** Low price sensitivity.
    /// * **Strategy:** The "Cash Cow" segment. Agents should learn to quote wider spreads here.
    Tier3,
}

impl ClientTier {
    /// Returns the **intrinsic sensitivity** ($\alpha_{base}$) of this client tier.
    ///
    /// This value represents the slope of the acceptance probability curve relative to other tiers.
    /// - **Higher values:** The client is more price-sensitive (curve is steeper).
    /// - **Lower values:** The client is more lenient (curve is flatter).
    pub fn intrinsic_sensitivity(&self) -> f64 {
        match self {
            // Highly sensitive. Rejects quickly if price is not optimal.
            Self::Tier1 => 0.8, 
            // Moderate sensitivity. Standard institutional behavior.
            Self::Tier2 => 0.4, 
            // Low sensitivity. Capturable spread is higher.
            Self::Tier3 => 0.1, 
        }
    }
}
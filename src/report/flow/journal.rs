use strum::{Display, EnumIter, IntoStaticStr};

/// Defines the columns for the high-level RFQ Journal (Summary).
/// Each row represents one unique RFQ lifecycle from start to finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RfqJournalCol {
    // === Identity ===
    EpisodeId,
    RfqId,
    ClientId,
    Symbol,
    
    // === Request Specs ===
    Side,          // Buy/Sell
    Quantity,      // Requested Amount
    StartTimestamp, // When did the phone ring?
    
    // === Outcome ===
    EndTimestamp,   // When was it finalized?
    DurationMs,     // How long did the negotiation take?
    State,          // Filled, Rejected, Expired?
    RoundsCount,    // How many ping-pongs? (len of history)
    
    // === Financials ===
    FinalPrice,     // If filled
    NotionalValue,  // Price * Qty
    RealizedPnL,    // (ExitPrice - EntryPrice) * Qty -> Only if we assume instant hedge
}


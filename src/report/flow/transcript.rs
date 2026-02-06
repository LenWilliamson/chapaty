use strum::{Display, EnumIter, IntoStaticStr};


/// Defines the columns for the detailed Negotiation Audit Log.
/// Each row represents a single state transition (event) within an RFQ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum TranscriptCol {
    EpisodeId,
    RfqId,
    RevisionId,     // 0, 1, 2...
    Timestamp,
    
    // === State Info ===
    StateType,      // "Open", "Quoted", "Countered", "Finalized"
    Side,           // Wer ist am Zug? (Client vs Agent)
    
    // === Pricing ===
    Price,          // Der Preis in DIESEM Schritt (Quote oder Counter-Offer)
    SpreadToMid,    // Wie weit waren wir vom Fair Value weg? (Analytics!)
}
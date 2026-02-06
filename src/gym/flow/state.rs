use std::{collections::HashMap, fmt::Debug};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    agent::AgentIdentifier,
    data::domain::{Price, Quantity, Symbol},
    gym::flow::domain::{Cash, ClientTier, Inventory, QuoteMode, RfqId, SettlementType, Side},
};

pub mod countered;
pub mod finalized;
pub mod open;
pub mod quoted;

pub trait RfqState: Debug + Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Open;

impl RfqState for Open {}

#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct Quoted {
    responder_id: AgentIdentifier,
    my_quote: Price,
}

impl RfqState for Quoted {}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct Countered {
    client_price: Price,
}

impl RfqState for Countered {}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Finalized {
    outcome: RfqOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RfqOutcome {
    Filled { price: Price, quantity: Quantity },
    Rejected,
    Expired,
}

impl RfqState for Finalized {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rfq<S: RfqState> {
    // === Identity ===
    /// Unique identifier for this specific Request for Quote event.
    pub rfq_id: RfqId,

    /// Tracks the negotiation round. Increments with every counter-offer.
    /// Initial request starts at revision 0.
    pub revision_id: u32,

    /// The counterparty requesting the quote.
    /// Used to look up credit limits and historical trading behavior.
    pub client_id: AgentIdentifier,

    /// **Client Segmentation / Sophistication Level.**
    ///
    /// Critical for the pricing model.
    /// - **Tier 1:** Requires tight spreads; high risk of adverse selection.
    /// - **Tier 3:** Allows wider spreads; typically uniformed flow.
    pub client_tier: ClientTier,

    // === Domain Data ===
    /// The tradable instrument (e.g., Bond ISIN, Crypto Pair, or Future).
    pub symbol: Symbol,

    /// The direction of the client's interest.
    /// - `Side::Buy`: Client wants to BUY from us (We sell).
    /// - `Side::Sell`: Client wants to SELL to us (We buy).
    pub side: Side,

    /// The requested nominal amount (for Bonds) or quantity (for Spot).
    pub quantity: f64,

    /// **Settlement Convention (Valuta).**
    ///
    /// Defines when the asset exchange occurs relative to the trade date.
    /// Crucial for Fixed Income calculation (Cost of Carry, Accrued Interest).
    /// - Crypto Spot: Usually T+0.
    /// - Bonds: Usually T+2.
    pub settlement: SettlementType,

    /// **Quoting Convention.**
    ///
    /// Determines the unit of negotiation.
    /// - `Price`: Percentage of Par (Clean Price).
    /// - `Yield`: Yield to Maturity (common for US Treasuries).
    /// - `Spread`: Basis points over benchmark curve.
    pub quote_mode: QuoteMode,

    // === Lifecycle ===
    /// Timestamp when the RFQ was received by the desk (in microseconds).
    pub created_at: u64,

    /// Expiration time. If no quote is issued by this time, the RFQ expires.
    /// Institutional clients typically expect quotes within seconds.
    pub time_to_live: DateTime<Utc>,

    // === State ===
    /// The current state of the negotiation (Open, Quoted, Countered, Finalized).
    pub state: S,
}

#[derive(Debug, Clone)]
pub enum State {
    Open(Rfq<Open>),
    Quoted(Rfq<Quoted>),
    Countered(Rfq<Countered>),
    Finalized(Rfq<Finalized>),
}

/// The central in-memory database of the environment's state.
///
/// This struct manages the lifecycle of all negotiations, the desk's risk exposure,
/// and the financial performance metrics. It is designed for O(1) access on the "Hot Path"
/// (active trading) while maintaining a complete audit trail in the "Cold Path".
#[derive(Debug, Clone)]
pub struct States {
    // ========================================================================
    // 1. Hot Path (Active Negotiations)
    // ========================================================================
    /// **The Active Negotiation Store.**
    ///
    /// Stores the complete transcript (history of state transitions) for every currently open RFQ.
    ///
    /// * **Key:** `RfqId` (Unique identifier).
    /// * **Value:** A vector of `State` enums representing the timeline (Open -> Quoted -> Countered...).
    ///   - The `last()` element is always the *current* state.
    ///   - This allows the agent to view the negotiation history (e.g., "Client rejected my last 2 quotes").
    pub live: HashMap<RfqId, Vec<State>>,
    
    // TODO: RAII enforcement that live is always partitioned by incoming_index and pending_index

    // ========================================================================
    // 2. Work Queues (Secondary Indices)
    // ========================================================================
    /// **Incoming Queue (Action Required).**
    ///
    /// An ordered list of RFQs requiring agent intervention (Open, Countered).
    ///
    /// * **Data Structure:** `Vec` is used over `HashSet` to ensure **deterministic ordering**
    ///   for the RL observation and faster iteration for small N (< 100).
    /// * **Invariant:** Must stay synchronized with `live`. Contains subset of keys from `live`.
    pub incoming_index: Vec<RfqId>,

    /// **Pending Queue (Awaiting Response).**
    ///
    /// An ordered list of RFQs where the desk is waiting for a client or market response.
    pub pending_index: Vec<RfqId>,

    // ========================================================================
    // 3. Cold Path (Archival)
    // ========================================================================
    /// **The Trade Journal.**
    ///
    /// Stores the finalized transcripts of completed RFQs (Filled, Rejected, Expired).
    /// Used for:
    /// - Generating the `blotter` (Summary DataFrame).
    /// - Generating the `transcript` (Audit Log).
    /// - Calculating post-episode statistics (Hit Rate, Spread Capture).
    pub archive: HashMap<RfqId, Vec<State>>,

    // ========================================================================
    // 4. Position & Risk Management
    // ========================================================================
    /// **Position Keeping System.**
    /// Tracks the net quantity held per instrument (Long/Short).
    pub inventory: Inventory,

    /// **Realized Capital.**
    /// Tracks the PnL realized from closed trades.
    pub cash: Cash,

    /// **Counterparty Credit Risk Exposure.**
    ///
    /// Tracks the utilized credit limit per client.
    ///
    /// * **Logic:** When we trade with a client, settlement is not instant (e.g., T+2).
    ///   Until settlement, we carry "Replacement Cost Risk" or "Principal Risk".
    /// * **Usage:** Before quoting, the agent checks `client_exposure < client_profile.max_limit`.
    /// * **Reset:** In a simplified simulation, this might reset after the trade is finalized,
    ///   or decay over simulated time.
    pub client_exposure: HashMap<AgentIdentifier, f64>,

    // ========================================================================
    // 5. RL Metrics (Transient)
    // ========================================================================
    /// Accumulated reward for the current step. Resets after `step()`.
    step_reward: f64,

    /// Total PnL (Realized + Unrealized MtM) since the start of the episode.
    cumulative_pnl: f64,
}

impl States {
    /// Helper: Gibt den aktuellen Status einer RFQ zurück (Head of the stack)
    pub fn get_current_state(&self, id: &RfqId) -> Option<&State> {
        self.live.get(id).and_then(|history| history.last())
    }

    /// Helper: Zugriff auf die gesamte Historie (Transcript)
    pub fn get_transcript(&self, id: &RfqId) -> Option<&[State]> {
        self.live.get(id).map(|v| v.as_slice())
    }
    /// Check: Darf ich diesem Client noch ein Quote geben?
    pub fn check_credit_limit(
        &self,
        client_id: &AgentIdentifier,
        profile: &ClientProfile,
        new_amount: f64,
    ) -> bool {
        let current_usage = self.client_exposure.get(client_id).copied().unwrap_or(0.0);
        (current_usage + new_amount) <= profile.max_credit_limit
    }

    /// Update Logic: Schiebt neuen Status in die History und updated Indizes
    pub(super) fn update_rfq(&mut self, id: RfqId, new_state: State) {
        /*

        TODO Probabilistic fill model

         */
        // 1. History Update
        let history = self.live.entry(id).or_default();
        history.push(new_state.clone()); // Push new head

        // 2. Index Management (State Transition)
        match new_state {
            State::Open(_) | State::Countered(_) => {
                // Das erfordert Action vom Agenten -> Incoming
                self.pending_index.remove(&id);
                self.incoming_index.insert(id);
            }
            State::Quoted(_) => {
                // Wir warten auf Kunden -> Pending
                self.incoming_index.remove(&id);
                self.pending_index.insert(id);
            }
            State::Finalized(_) => {
                // Raus aus Live, rein ins Archiv
                self.incoming_index.remove(&id);
                self.pending_index.remove(&id);

                if let Some(full_history) = self.live.remove(&id) {
                    self.archive.insert(id, full_history);
                }
            }
        }
    }
    /// Returns an iterator over ALL RFQs (Active and Archived).
    /// Used for reporting and global state analysis.
    ///
    /// # Returns
    /// An iterator yielding `(RfqId, &Vec<State>)`.
    /// The `Vec<State>` represents the full negotiation transcript.
    pub fn iter_all_rfqs(&self) -> impl Iterator<Item = (&RfqId, &Vec<State>)> {
        let live_iter = self.live.iter();
        let archive_iter = self.archive.iter(); // Annahme: Archive ist auch HashMap<RfqId, Vec<State>>

        live_iter.chain(archive_iter)
    }
}

impl States {
    /// Helper to efficiently remove an ID from a vector index.
    /// Since order matters for RL observations, we use `retain` or `position` + `remove`.
    /// `swap_remove` is faster O(1) but destroys ordering.
    pub(super) fn remove_from_index(index: &mut Vec<RfqId>, id: &RfqId) {
        if let Some(pos) = index.iter().position(|x| x == id) {
            index.remove(pos); // O(N), but N is small, so it's fine (nanoseconds).
        }
    }
    
    /// Helper to insert unique ID into index (maintaining order of arrival).
    pub(super) fn add_to_index(index: &mut Vec<RfqId>, id: RfqId) {
        if !index.contains(&id) { // Linear scan O(N) is fine for N < 100
            index.push(id);
        }
    }
}
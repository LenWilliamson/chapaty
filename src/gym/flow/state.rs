use std::{collections::HashMap, fmt::Debug, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    agent::AgentIdentifier,
    data::domain::{Price, Quantity, Symbol},
    error::{ChapatyResult, EnvError},
    flow::state::quoted::QuoteDetails,
    gym::{
        Reward,
        flow::domain::{
            Cash, ClientProfile, ClientTier, Inventory, QuoteMode, RfqId, SettlementType, Side,
        },
    },
    sorted_vec_map::SortedVecMap,
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
pub struct RfqHeader {
    // === Identity ===
    /// Unique identifier for this specific Request for Quote event.
    pub rfq_id: RfqId,

    /// Tracks the revisions. Increments with every counter-offer.
    /// Initial request starts at revision 0.
    pub revision_id: u32,

    /// The counterparty requesting the quote.
    /// Used to look up credit limits and historical trading behavior.
    pub client_id: AgentIdentifier,

    /// **Client Segmentation / Sophistication Level.**
    ///
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

    /// The requested nominal amount for Bonds.
    pub quantity: Quantity,

    /// **Settlement Convention (Valuta).**
    ///
    /// Defines when the asset exchange occurs relative to the trade date.
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
    /// Timestamp when the RfQ was received by the desk (in microseconds).
    pub created_at: DateTime<Utc>,

    /// Expiration time. If no quote is issued by this time, the RfQ expires.
    pub time_to_live: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rfq<S: RfqState> {
    pub header: Arc<RfqHeader>,

    // === State ===
    /// The current state of the negotiation (Open, Quoted, Countered, Finalized).
    pub state: S,
}

/// Trait for RfQ states that have a Time-To-Live (TTL) and can expire.
pub trait Expirable {
    /// Consumes the current state and returns a Finalized RfQ with `Outcome::Expired`.
    fn expire(self) -> Rfq<Finalized>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum State {
    Open(Rfq<Open>),
    Quoted(Rfq<Quoted>),
    Countered(Rfq<Countered>),
    Finalized(Rfq<Finalized>),
}

impl State {
    pub fn header(&self) -> &Arc<RfqHeader> {
        match self {
            State::Open(r) => &r.header,
            State::Quoted(r) => &r.header,
            State::Countered(r) => &r.header,
            State::Finalized(r) => &r.header,
        }
    }

    pub fn as_open_rfq(&self) -> Option<&Rfq<Open>> {
        match self {
            State::Open(rfq) => Some(rfq),
            _ => None,
        }
    }

    pub fn as_quoted_rfq(&self) -> Option<&Rfq<Quoted>> {
        match self {
            State::Quoted(rfq) => Some(rfq),
            _ => None,
        }
    }

    pub fn as_countered_rfq(&self) -> Option<&Rfq<Countered>> {
        match self {
            State::Countered(rfq) => Some(rfq),
            _ => None,
        }
    }

    pub fn as_finalized_rfq(&self) -> Option<&Rfq<Finalized>> {
        match self {
            State::Finalized(rfq) => Some(rfq),
            _ => None,
        }
    }
}

impl TryFrom<State> for Rfq<Open> {
    type Error = EnvError;

    fn try_from(value: State) -> Result<Self, Self::Error> {
        match value {
            State::Open(inner) => Ok(inner),
            other => Err(EnvError::InvalidTransition(format!(
                "Expected Open state, found {:?}",
                other
            ))),
        }
    }
}

impl TryFrom<State> for Rfq<Quoted> {
    type Error = EnvError;

    fn try_from(value: State) -> Result<Self, Self::Error> {
        match value {
            State::Quoted(inner) => Ok(inner),
            other => Err(EnvError::InvalidTransition(format!(
                "Expected Quoted state, found {:?}",
                other
            ))),
        }
    }
}

impl TryFrom<State> for Rfq<Countered> {
    type Error = EnvError;

    fn try_from(value: State) -> Result<Self, Self::Error> {
        match value {
            State::Countered(inner) => Ok(inner),
            other => Err(EnvError::InvalidTransition(format!(
                "Expected Countered state, found {:?}",
                other
            ))),
        }
    }
}

impl TryFrom<State> for Rfq<Finalized> {
    type Error = EnvError;

    fn try_from(value: State) -> Result<Self, Self::Error> {
        match value {
            State::Finalized(inner) => Ok(inner),
            other => Err(EnvError::InvalidTransition(format!(
                "Expected Finalized state, found {:?}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct States {
    /// **The Active Revision Store.**
    ///
    /// Stores the complete transcript (history of state transitions) for every currently live RfQ.
    ///
    /// * **Key:** `RfqId` (Unique identifier).
    /// * **Value:** A vector of `State` enums representing the timeline (Open -> Quoted -> Countered...).
    ///   - The `last()` element is always the *current* state.
    ///   - This allows the agent to view the negotiation history (e.g., "Client rejected my last 2 quotes").
    ///
    /// # Invariants
    ///
    /// Let $K_{live}$ be the set of keys in `live`.
    /// Let $S_{in}$ be the set of ids in `incoming_index`.
    /// Let $S_{pen}$ be the set of ids in `pending_index`.
    ///
    /// The indices form a **strict partition** of the live set:
    ///
    /// 1.  **Completeness:** $K_{live} = S_{in} \cup S_{pen}$
    ///     (Every live RfQ is continuously tracked in exactly one queue).
    /// 2.  **Disjointness:** $S_{in} \cap S_{pen} = \emptyset$
    ///     (An RfQ cannot require action and await response simultaneously).
    pub live: SortedVecMap<RfqId, Vec<State>>,

    /// **Incoming Queue (Action Required).**
    ///
    /// An ordered list of RfQs requiring agent intervention (Open, Countered).
    pub incoming_index: Vec<RfqId>,

    /// **Pending Queue (Awaiting Response).**
    ///
    /// An ordered list of RfQs where the desk is waiting for a client response.
    pub pending_index: Vec<RfqId>,

    /// **The Trade Journal.**
    ///
    /// Stores the finalized transcripts of completed RfQs (Filled, Rejected, Expired).
    pub archive: HashMap<RfqId, Vec<State>>,

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
    pub client_exposure: HashMap<AgentIdentifier, f64>,

    // ========================================================================
    // 5. RL Metrics (Transient)
    // ========================================================================
    /// **TRANSIENT:** Accumulated reward for the current step. Resets after `step()`.
    step_reward: f64,

    /// **PERSISTENT:** Total PnL (Realized + Unrealized MtM) since the start of the episode.
    cumulative_pnl: f64,
}

impl States {
    /// Helper: Gibt den aktuellen Status einer RfQ zurück (Head of the stack)
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

    pub fn init_exposure(&mut self, client_id: AgentIdentifier) {
        self.client_exposure.entry(client_id).or_insert(0.0);
    }

    /// Registers a brand new RfQ from the generator.
    /// Sets the initial state to 'Open' and queues it for agent observation.
    pub fn insert_new(&mut self, rfq: Rfq<Open>) {
        let id = rfq.header.rfq_id;
        let client_id = rfq.header.client_id.clone();

        // 1. Convert strongly typed Rfq<Open> into generic State enum wrapper
        // Wir nehmen an, dass 'State' ein Enum ist, das 'Open(Rfq<Open>)' wrappt.
        let initial_state = State::Open(rfq);

        // 2. Insert into Hot Path Store (Live Map)
        // Wir starten den Transcript mit dem initialen State.
        self.live.insert(id, vec![initial_state]);

        // 3. Queue for Agent Intervention
        // Ein neuer Request muss vom Agenten beantwortet werden -> Incoming Queue.
        self.incoming_index.push(id);

        // 4. Ensure Risk Tracking exists
        // (Vermeidet KeyNotFound Errors bei späterem Credit Check)
        self.client_exposure.entry(client_id).or_insert(0.0);

        // Optional: Logging
        // tracing::debug!(%id, "New RfQ registered in state.");
    }

    // Helper to get details for the generic agent observation
    pub fn get_quote_details(&self, rfq_id: RfqId) -> ChapatyResult<QuoteDetails> {
        let history = self
            .live
            .get(&rfq_id)
            .ok_or(EnvError::RfqNotFound(rfq_id.0.to_string()))?;
        let last_state = history.last().expect("History cannot be empty");

        let quoted = match last_state {
            State::Quoted(q) => q,
            _ => return Err(EnvError::InvalidState("RfQ is not in Quoted state".into()).into()),
        };

        Ok(QuoteDetails {
            rfq_id,
            client_id: quoted.header.client_id.clone(),
            symbol: quoted.header.symbol.clone(),
            side: quoted.header.side,
            quantity: quoted.header.quantity,
            price: quoted.state.my_quote,
        })
    }

    /// Returns an iterator over ALL RfQs (Active and Archived).
    /// Used for reporting and global state analysis.
    ///
    /// # Returns
    /// An iterator yielding `(RfqId, &Vec<State>)`.
    /// The `Vec<State>` represents the full revision transcript.
    pub fn iter_all_rfqs(&self) -> impl Iterator<Item = (&RfqId, &Vec<State>)> {
        let live_iter = self.live.iter();
        let archive_iter = self.archive.iter(); // Annahme: Archive ist auch HashMap<RfqId, Vec<State>>

        live_iter.chain(archive_iter)
    }
}

impl States {
    /// Consumes and resets the accumulated step reward.
    pub(crate) fn pop_reward(&mut self) -> Reward {
        let r = self.step_reward;
        self.step_reward = 0.0;
        Reward(r.round() as i64)
    }
}

impl States {
    /// **Agent Action:** Propose a price to the client.
    /// Valid for: `Open` (Initial Quote) and `Countered` (Re-Quote).
    pub(super) fn quote(&mut self, id: RfqId, price: Price) -> ChapatyResult<()> {
        self.modify_rfq(id, |state| match state {
            State::Open(r) => {
                // Initial Quote: Open -> Quoted
                let next = r.quote(price);
                Ok(Transition {
                    new_state: State::Quoted(next),
                    output: (),
                })
            }
            State::Countered(r) => {
                // Re-Quote: Countered -> Quoted
                let next = r.requote(price);
                Ok(Transition {
                    new_state: State::Quoted(next),
                    output: (),
                })
            }
            other => Err(EnvError::InvalidTransition(format!(
                "Cannot quote RfQ {:?} from state {:?}",
                id, other
            ))
            .into()),
        })
    }

    /// **Environment Event:** Client proposes a counter-price.
    /// Invariant: Only valid if the RfQ is currently `Quoted`.
    pub(super) fn counter(&mut self, id: RfqId, client_price: Price) -> ChapatyResult<()> {
        self.modify_rfq(id, |state| {
            // 1. Enforce Invariant by Type System
            // If 'state' is not Quoted, this returns Err(InvalidTransition) immediately.
            let quoted: Rfq<Quoted> = state.try_into()?;

            // 2. Domain Transition
            let next = quoted.customer_counters(client_price);

            Ok(Transition {
                new_state: State::Countered(next),
                output: (),
            })
        })
    }

    /// **Agent/Env Action:** The trade is agreed upon (Filled).
    /// Valid for:
    /// 1. `Quoted`: Customer accepts Agent's price.
    /// 2. `Countered`: Agent accepts Customer's price.
    pub(super) fn fill(&mut self, id: RfqId) -> ChapatyResult<QuoteDetails> {
        self.modify_rfq(id, |state| {
            // 1. Delegate to Domain Logic
            let (finalized_rfq, exec_price) = match state {
                State::Quoted(r) => {
                    // Scenario A: Customer accepts our quote
                    let p = r.state.my_quote;
                    (r.customer_accepts(), p)
                }
                State::Countered(r) => {
                    // Scenario B: We accept customer's counter
                    let p = r.state.client_price;
                    (r.accept_counter(), p)
                }
                other => {
                    return Err(EnvError::InvalidTransition(format!(
                        "Cannot fill from state: {:?}",
                        other
                    ))
                    .into());
                }
            };

            // 2. Create Receipt (Immutable data from Header)
            let details = QuoteDetails {
                rfq_id: id,
                client_id: finalized_rfq.header.client_id.clone(),
                symbol: finalized_rfq.header.symbol.clone(),
                side: finalized_rfq.header.side,
                quantity: finalized_rfq.header.quantity,
                price: exec_price,
            };

            Ok(Transition {
                new_state: State::Finalized(finalized_rfq),
                output: details,
            })
        })
    }

    /// **Agent/Env Action:** The trade is rejected (No Deal).
    /// Valid for: `Quoted` (Client rejects) or `Countered` (Agent rejects).
    pub(super) fn reject(&mut self, id: RfqId) -> ChapatyResult<()> {
        self.modify_rfq(id, |state| {
            let finalized_rfq = match state {
                State::Quoted(r) => r.customer_rejects(),
                State::Countered(r) => r.reject_counter(),
                other => {
                    return Err(EnvError::InvalidTransition(format!(
                        "Cannot reject from state: {:?}",
                        other
                    ))
                    .into());
                }
            };

            Ok(Transition {
                new_state: State::Finalized(finalized_rfq),
                output: (),
            })
        })
    }

    /// **System Event:** Time to Live (TTL) expired.
    /// Moves any active RfQ to Finalized(Expired).
    pub(super) fn expire(&mut self, id: RfqId) -> ChapatyResult<()> {
        if !self.live.contains_key(&id) {
            return Ok(());
        }

        self.modify_rfq(id, |state| {
            let expired_rfq = match state {
                State::Open(r) => r.expire(),
                State::Quoted(r) => r.expire(),
                State::Countered(r) => r.expire(),
                State::Finalized(_) => {
                    return Err(EnvError::InvalidTransition(format!(
                        "Cannot expire RfQ {:?} because it is already Finalized.",
                        id
                    ))
                    .into());
                }
            };

            Ok(Transition {
                new_state: State::Finalized(expired_rfq),
                output: (),
            })
        })
    }

    // TODO should be private
    pub(super) fn update_valuation(&mut self, total_pnl: f64) {
        let delta = total_pnl - self.cumulative_pnl;
        self.step_reward += delta;
        self.cumulative_pnl = total_pnl;
    }
}

impl States {
    /// The Core Transaction Kernel.
    /// Executes a transaction on a specific rfq location.
    ///
    /// # Safety
    /// This uses `StateGuard` to clone the state first. The vector is NOT modified
    /// until the closure returns successfully and `guard.commit()` is called.
    fn modify_rfq<F, R>(&mut self, id: RfqId, f: F) -> ChapatyResult<R>
    where
        // Closure returns a clear 'Transition' struct
        F: FnOnce(State) -> ChapatyResult<Transition<R>>,
    {
        // 1. Create Guard (Clones state, leaves vector untouched)
        let guard = StateGuard::new(self, id)?;

        // 2. Work on the Clone
        let working_copy = guard.get().clone();

        // 3. Logic & Commit
        match f(working_copy) {
            Ok(t) => {
                guard.commit(t.new_state); // Commit the state
                Ok(t.output) // Return the output
            }
            Err(e) => Err(e), // Guard drops, rolling back automatically
        }
    }
}

// ================================================================================================
// StateGuard: The Traffic Controller
// ================================================================================================

/// RAII Guard that manages RfQ state transitions and enforces index partitioning.
///
/// # Mechanism
/// 1. `new()`: Snapshots the current head state from `live`. Does NOT remove it (Optimistic).
/// 2. `commit()`: Appends to history and updates partition indices.
///    - **History:** Pushes the new state to the `live` vector (Hot Storage).
///    - **Queues:** Calculates `TargetIndex` change. If changed:
///      - Removes from old index via `retain` (Order Preserving O(N)).
///      - Pushes to new index (O(1)).
///    - **Finalization:** If terminal, moves the entire transcript from `live` to `archive`.
/// 3. `drop()`: If not committed, does nothing. The `States` struct remains untouched (Rollback).
#[must_use = "StateGuard must be committed to persist changes"]
#[derive(Debug)]
pub struct StateGuard<'a> {
    id: RfqId,
    working_state: Option<State>,
    states: &'a mut States,
}
impl<'a> StateGuard<'a> {
    fn new(states: &'a mut States, id: RfqId) -> ChapatyResult<Self> {
        // We act on the *last* element of the history (the current state).
        let working_state = states
            .live
            .get(&id)
            .and_then(|history| history.last())
            .cloned()
            .ok_or_else(|| EnvError::RfqNotFound(id.0.to_string()))?;

        Ok(Self {
            id,
            working_state: Some(working_state),
            states,
        })
    }

    /// Access the working copy (Immutable).
    fn get(&self) -> &State {
        self.working_state
            .as_ref()
            .expect("StateGuard invariant violated: state missing")
    }

    fn commit(mut self, new_state: State) {
        // 1. Identify Source & Destination
        // We know exactly where the ID is now (old_idx) and where it must go (new_idx).
        let working = self.working_state.as_ref()
            .expect("StateGuard invariant violated: working_state is None (double commit or use-after-free?)");

        let old_idx = TargetIndex::from(working);
        let new_idx = TargetIndex::from(&new_state);

        // 2. Append History (Hot Path)
        self.states
            .live
            .get_mut(&self.id)
            .expect("StateGuard invariant violated: RfQ ID vanished from Live map during transaction scope")
            .push(new_state);

        // 3. Transition (Only move if indices differ)
        if old_idx != new_idx {
            // A. Remove from OLD location
            match old_idx {
                TargetIndex::Incoming => self.states.incoming_index.retain(|x| x != &self.id),
                TargetIndex::Pending => self.states.pending_index.retain(|x| x != &self.id),
                TargetIndex::Archive => {
                    panic!(
                        "\n\n\
                            ┌────────────────────────────────────────────────────────────────────┐\n\
                            │ **[INVARIANT VIOLATION]:** StateGuard Logic Error                  │\n\
                            │ DETAIL: RfQ {:?} is in 'Live' map but has 'Finalized' state.       │\n\
                            │ CAUSE:  A previous transition failed to archive the trade?         │\n\
                            │ ACTION: This is a bug in the state machine. Investigate commit().  │\n\
                            └────────────────────────────────────────────────────────────────────┘\n\n",
                        self.id
                    );
                }
            }

            // B. Add to NEW location
            match new_idx {
                TargetIndex::Incoming => self.states.incoming_index.push(self.id),
                TargetIndex::Pending => self.states.pending_index.push(self.id),
                TargetIndex::Archive => {
                    // Cold Path: Move payload to Archive
                    if let Some(transcript) = self.states.live.remove(&self.id) {
                        self.states.archive.insert(self.id, transcript);
                    }
                }
            }
        }
        self.working_state = None;
    }
}

impl<'a> Drop for StateGuard<'a> {
    fn drop(&mut self) {
        // If we drop without commit, we do NOTHING.
        // The original state is still sitting safely in the vector.
        // This effectively "Rolls Back" to the state before the transaction.
    }
}

// ================================================================================================
// Helper Enums
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetIndex {
    Incoming,
    Pending,
    Archive,
}

impl From<State> for TargetIndex {
    fn from(value: State) -> Self {
        match value {
            State::Open(_) | State::Countered(_) => Self::Incoming,
            State::Quoted(_) => Self::Pending,
            State::Finalized(_) => Self::Archive,
        }
    }
}

impl From<&State> for TargetIndex {
    fn from(value: &State) -> Self {
        match value {
            State::Open(_) | State::Countered(_) => Self::Incoming,
            State::Quoted(_) => Self::Pending,
            State::Finalized(_) => Self::Archive,
        }
    }
}

// ================================================================================================
// Helper Sructs
// ================================================================================================

/// Represents a successful state transition.
/// - `state`: The new state to commit to storage.
/// - `output`: The value to return to the caller (e.g., Reward, Events).
struct Transition<T> {
    new_state: State,
    output: T,
}

#[cfg(test)]
mod test {
    #[test]
    fn partition_invariatn() {
        assert!(
            false,
            "// TODO: RAII enforcement that live is always partitioned by incoming_index and pending_index"
        )
    }
    fn uniqueness() {
        assert!(
            false,
            "// TODO: we use retain, check that each index only occurs once for uniquneess claims"
        )
    }
}

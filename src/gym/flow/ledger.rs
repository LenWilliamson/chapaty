use crate::{
    agent::AgentIdentifier,
    data::{episode::Episode, view::MarketView},
    error::ChapatyResult,
    gym::{
        Reward,
        flow::{
            action::Action,
            context::{ActionCtx as FlowActionCtx, ActionSummary as FlowActionSummary},
            domain::{Cash, Inventory, RfqId},
            scheduler::CustomerDecision,
            state::{Open, quoted::QuoteDetails, Rfq, States as FlowStates},
        },
        trading::{
            context::{
                ActionCtx as StreetActionCtx, ActionSummary as StreetActionSummary, UpdateCtx,
            },
            ledger::{Ledger as StreetLedger, ep_not_found_err},
            state::States as StreetStates,
        },
    },
};

/// The Central Clearing House of the simulation.
///
/// The Ledger manages the financial reality of the agent across a simulation run.
/// It distinguishes between **Continuous State** (Position/Cash) and **Episodic History** (Transaction Logs).
///
/// # The "Zipper" Architecture
///
/// The Ledger merges two distinct trading activities into a single financial outcome:
/// 1.  **Flow Book (OTC):** Client requests, quotes, and manual fills.
/// 2.  **Street Book (Exchange):** Market hedges, limit orders, and execution costs.
///
/// While the history (logs) of these activities is kept separate for analysis,
/// their impact on `Inventory` and `Cash` is immediate and unified.
#[derive(Clone, Debug)]
pub(super) struct Ledger {
    // ========================================================================
    // 1. Episodic History (The Journals)
    // ========================================================================
    /// **The Sales Book (OTC) - Per Episode.**
    ///
    /// Stores the OTC history partitioned by episode.
    flow_states: Vec<FlowStates>,

    /// **The Street Book (Exchange).**
    /// Manages market execution (Hedges, Limit Orders).
    /// Reuses the full logic from the Trading Environment!
    street_ledger: StreetLedger,

    /// **Episode PnL History.**
    ///
    /// Tracks the realized + unrealized PnL generated within each specific episode.
    pnl_history: Vec<f64>,

    // ========================================================================
    // 2. Continuous State (The Rolling Portfolio)
    // ========================================================================
    /// **Global Net Inventory.**
    ///
    /// The current quantity held in the asset. This value **carries over** between episodes.
    pub inventory: Inventory,

    /// **Global Realized Capital.**
    ///
    /// The accumulated net cash flow since the start of the **Epoch** (simulation run).
    pub cash: Cash,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            flow_states: Vec::new(),
            street_ledger: StreetLedger::default(),
            pnl_history: Vec::new(),
            inventory: Inventory::default(),
            cash: Cash::default(),
        }
    }

    pub fn clear(&mut self) {
        self.flow_states.clear();
        self.street_ledger.clear();
        self.pnl_history.clear();
        self.inventory = Inventory::default();
        self.cash = Cash::default();
    }

    pub fn flow_states(&self, ep: &Episode) -> ChapatyResult<&FlowStates> {
        self.flow_states
            .get(ep.id().0)
            .ok_or_else(|| ep_not_found_err(ep))
    }

    /// Access the Flow Book for the current episode.
    fn flow_states_mut(&mut self, ep: &Episode) -> ChapatyResult<&mut FlowStates> {
        self.flow_states
            .get_mut(ep.id().0)
            .ok_or_else(|| ep_not_found_err(ep))
    }

    // ========================================================================
    // ACTION HANDLING (Command Sourcing)
    // ========================================================================

    /// Initializes the risk exposure record for a new client.
    /// Ensures that the 'client_exposure' map has an entry for this ID.
    pub fn init_client_exposure(&mut self, client_id: AgentIdentifier) {
        // Wir greifen auf das aktive Buch zu (das letzte in der Liste)
        if let Some(book) = self.flow_states.last_mut() {
            book.init_exposure(client_id);
        } else {
            // Edge Case: RfQ kommt rein, aber noch keine Episode gestartet.
            // Sollte durch Logik in `reset()` verhindert werden, aber sicher ist sicher.
            tracing::warn!(
                "Tried to init exposure for client {:?} but no active episode found.",
                client_id
            );
        }
    }

    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0))]
    pub fn apply_street_actions(
        &mut self,
        ep: &Episode,
        ctx: StreetActionCtx,
    ) -> ChapatyResult<StreetActionSummary> {
        self.street_ledger.apply_actions(ep, ctx)
    }

    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0))]
    pub fn apply_flow_actions(
        &mut self,
        ep: &Episode,
        ctx: FlowActionCtx,
    ) -> ChapatyResult<FlowActionSummary> {
        let mut summary = FlowActionSummary::default();

        for action in ctx.actions {
            // A. Trace Intent
            let span =
                tracing::debug_span!("flow_cmd", rfq_id = ?action.rfq_id(), payload = ?action);
            let _enter = span.enter();

            // B. State Transition Logic
            // Flow actions update the negotiation state but usually don't move money yet
            // (Money moves on 'Fill', which happens in process_client_reply).
            match self.apply_single_flow(ep, &action) {
                Ok(_) => {
                    tracing::debug!(outcome = "applied", "Flow State Updated");
                    summary.executed.push(action);
                }
                Err(e) => {
                    // Logic Error (e.g., Quoting an expired RfQ)
                    tracing::warn!(outcome = "rejected", error = %e, "Flow Action Invalid");
                    summary.rejected.push(action);
                }
            }
        }
        Ok(summary)
    }

    // ========================================================================
    // EVENT HANDLING (The "Zipper" Logic)
    // ========================================================================

    /// Handles a new inbound RfQ.
    pub fn register_request(&mut self, rfq: Rfq<Open>, ep: &Episode) -> ChapatyResult<()> {
        self.flow_states_mut(ep)
            .map(|states| states.insert_new(rfq))
    }

    /*
    TODO prüfe ob in tracing::instrument überall der span richtig ist und die parameter captured
    */

    /// Handles the client's reaction to our Quote.
    /// **CRITICAL:** This is where Flow turns into Inventory/Cash (The Zipper).
    #[tracing::instrument(skip(self), fields(rfq_id = ?rfq_id, decision = ?decision))]
    pub fn process_client_reply(
        &mut self,
        rfq_id: RfqId,
        decision: CustomerDecision,
        ep: &Episode,
    ) -> ChapatyResult<()> {
        // 1. Locate the RfQ state
        // We assume we are working on the active episode
        let book = self.flow_states_mut(ep)?;

        match decision {
            CustomerDecision::Accept => {
                self.finalize_fill(ep, rfq_id)?;
                tracing::info!(rfq_id = %rfq_id.0, "Client ACCEPTED quote. Trade booked.");
                Ok(())
            }
            CustomerDecision::Reject => {
                book.reject(rfq_id)?;
                tracing::debug!("Client REJECTED quote.");
                Ok(())
            }
            CustomerDecision::Counter(price) => {
                book.counter(rfq_id, price)?;
                tracing::debug!("Client COUNTERED quote.");
                Ok(())
            }
        }
    }

    /// Executes the financial impact of a filled quote (The Zipper).
    /// Updates Inventory and Cash atomically.
    fn book_execution(&mut self, quote: &QuoteDetails) {
        // 1. Determine direction (Client Buy -> We Sell -> Negative Sign)
        let sign = quote.side.mm_sign();

        // 2. Calculate Signed Quantity
        let signed_qty = quote.quantity.0 * sign;

        // 3. Calculate Cash Flow
        // Accounting Identity: Cash Flow = - (SignedQty * Price)
        // If we BUY (Qty > 0): Cash = - (Pos * Price) = Negative (Outflow)
        // If we SELL (Qty < 0): Cash = - (Neg * Price) = Positive (Inflow)
        let cash_impact = -(signed_qty * quote.price.0);

        // 4. Apply Updates
        self.inventory.update(quote.symbol.clone(), signed_qty);
        self.cash += cash_impact;

        // TODO: Audit Log / Transaction Trace hier einfügen
    }

    pub fn archive_expired(&mut self, rfq_id: RfqId, ep: &Episode) -> ChapatyResult<()> {
        self.flow_states_mut(ep)
            .map(|states| states.expire(rfq_id))
            .flatten()
    }

    /// Retrieves the details of the active quote for a given RfQ ID.
    /// Returns an error if the RfQ is not found or not in the 'Quoted' state.
    pub fn get_quote_details(&self, rfq_id: RfqId) -> ChapatyResult<QuoteDetails> {
        // Wir suchen im aktiven Buch (letzte Episode)
        let book = self.flow_states.last().unwrap();
        // TODO .ok_or(EnvError::NoActiveEpisode)?;

        // Wir delegieren an das FlowBook (States struct)
        book.get_quote_details(rfq_id)
    }

    // ========================================================================
    // VALUATION & REWARDS
    // ========================================================================

    /// Performs Mark-to-Market updates on all active positions and calculates RL Rewards.
    ///
    /// # Logic
    /// 1. **Street Update:** Delegates to `street_ledger` to update limit orders and hedges.
    /// 2. **Global Valuation:** Calculates the total Net Worth (Cash + Inventory MtM).
    /// 3. **Reward Signal:** Pushes the new valuation to the current `FlowStates` to calculate
    ///    the step reward (PnL Delta).
    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0, ts = %ctx.market.current_timestamp()))]
    pub fn apply_updates(&mut self, ep: &Episode, ctx: &UpdateCtx) -> ChapatyResult<()> {
        /*
        Da wir im Flow-Trading (anders als im Street-Trading) meist keine langlebigen Limit-Orders
        mit komplexen Exit-Kriterien haben, ist apply_updates hier hauptsächlich für das Mark-to-Market (MtM)
        Reporting und die Reward-Berechnung zuständig.
        */

        // 1. Delegate to Street Ledger
        // Handles execution of limit orders, stop-losses, etc. on the exchange.
        self.street_ledger.apply_updates(ep, ctx)?;

        // 2. Calculate Global Valuation (The "Zipper" View)
        // Value = Cash + (Inventory * MidPrice)
        // This captures both the realized PnL from Flow/Street trades
        // and the unrealized PnL from holding inventory.
        let current_valuation = self.global_total_pnl(ctx.market)?;

        // 3. Update Flow States
        // We push the global valuation into the current episode's state
        // so it can calculate the Reward Delta (RL Signal).
        // TODO the ledger shouldn't do this
        self.flow_states_mut(ep)?
            .update_valuation(current_valuation);

        Ok(())
    }

    /// Calculates the Total Global PnL (Realized + Unrealized).
    pub fn global_total_pnl(&self, market: &MarketView) -> ChapatyResult<f64> {
        let realized = self.cash.0;
        let unrealized = self.inventory.mark_to_market(market)?;
        Ok(realized + unrealized)
    }

    /// Consumes the accumulated reward delta.
    pub(crate) fn pop_step_reward(&mut self, ep: &Episode) -> ChapatyResult<Reward> {
        let street_reward = self.street_ledger.pop_step_reward(ep)?;
        let rfq_reward = self.flow_states_mut(ep)?.pop_reward();

        Ok(street_reward + rfq_reward)
    }

    pub(super) fn is_terminal(&self, ep: &Episode) -> ChapatyResult<bool> {
        self.street_ledger.is_terminal(ep)
    }

    pub(super) fn street_states(&self, ep: &Episode) -> ChapatyResult<&StreetStates> {
        self.street_ledger.get(ep)
    }

    // ========================================================================
    // INTERNAL HELPERS
    // ========================================================================

    fn apply_single_flow(&mut self, ep: &Episode, action: &Action) -> ChapatyResult<()> {
        let book = self.flow_states_mut(ep)?;
        match action {
            Action::Quote(cmd) => book.quote(cmd.rfq_id, cmd.price),
            Action::Reject(cmd) => book.reject(cmd.rfq_id),
            Action::Ignore(_) => Ok(()),
            Action::Accept(cmd) => {
                self.finalize_fill(ep, cmd.rfq_id)?;
                tracing::info!(rfq_id = %cmd.rfq_id.0, "Agent ACCEPTED counter. Trade booked.");
                Ok(())
            }
        }
    }

    /// Atomically finalizes a trade: Transitions state to 'Filled' and books execution.
    /// Used both when Client accepts our Quote AND when Agent accepts Client's Counter.
    fn finalize_fill(&mut self, ep: &Episode, rfq_id: RfqId) -> ChapatyResult<()> {
        let execution_receipt = {
            let book = self.flow_states_mut(ep)?;
            // transition_to_filled ist intelligent genug, den korrekten Preis
            // (Quote vs. Counter) basierend auf dem Vor-Status zu wählen.
            book.fill(rfq_id)?
        };

        // 2. Financial Booking
        self.book_execution(&execution_receipt);

        Ok(())
    }
}

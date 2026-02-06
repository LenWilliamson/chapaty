use polars::frame::DataFrame;

use crate::{
    data::episode::Episode,
    error::ChapatyResult,
    gym::flow::{
        action::Action,
        context::{ActionCtx, ActionSummary},
        state::States,
    },
};

#[derive(Clone, Debug)]
pub(super) struct Ledger(Vec<States>);

impl Ledger {
    /// Applies agent actions (Open, Close, Modify).
    /// Returns a report summarizing successes and failures for reward shaping.
    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0, ts = %ctx.market.current_timestamp()))]
    pub fn apply_actions(&mut self, ep: &Episode, ctx: ActionCtx) -> ChapatyResult<ActionSummary> {
        let mut report = ActionSummary::default();
        let market_view = ctx.market;

        let states = self.get_mut(ep)?;

        for action in ctx.actions.0.into_sorted_iter() {
            // A. Trace the Intent (Command Sourcing Requirement)
            let span = tracing::debug_span!(
                "cmd",
                agent = ?action.agent_id(),
                rfq_id = ?action.rfq_id(),
                revision_id = ?action.revision_id(),
                payload = ?action
            );
            let _enter = span.enter();

            // B. Intrinsic Validation
            if let Err(e) = action.validate() {
                tracing::warn!(reason = "validation", error = %e, "Command Rejected");
                report.rejected += 1;
                continue;
            }

            // C. Application
            let result = match action {
                Action::Quote(cmd) => states.quote(cmd, &market_view),
                Action::Ignore(cmd) => states.ignore(cmd),
            };

            // D. Trace the Outcome
            match result {
                Ok(_) => {
                    // For Command Sourcing, an "Applied" log confirms the state
                    // machine accepted the transition.
                    tracing::debug!(outcome = "applied", "State Transition Successful");
                    report.executed += 1;
                }
                Err(e) => {
                    // "Rejected" means this command had NO EFFECT on state.
                    tracing::warn!(
                        outcome = "rejected",
                        error = %e,
                        "Command Failed Business Logic"
                    );
                    report.rejected += 1;
                }
            }
        }

        Ok(report)
    }

    /// Performs Mark-to-Market updates on all active and pending positions.
    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0, ts = %ctx.market.current_timestamp()))]
    pub fn apply_updates(&mut self, ep: &Episode, ctx: UpdateCtx) -> ChapatyResult<()> {
        self.get_mut(ep)?
            .update_all_active_rfqs(&ctx, |m_id, result| {
                match result {
                    Ok(exit_event) => {
                        // Log Lifecycle Events
                        // For recovery validation, it helps to log the financial result here.
                        if let Some(reason) = exit_event {
                            // Access the trade state safely to log PnL (Optional but helpful)
                            // Note: In a real replay, you rely on deterministic market data
                            // to produce the same exit, but this log verifies it.
                            tracing::info!(
                                market = ?m_id,
                                reason = ?reason,
                                "Trade Finalized (Exit Triggered)"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!(
                            market = ?m_id,
                            error = %e,
                            "CRITICAL: Mark-to-Market Calculation Failed"
                        );
                        // Return Err(e) to stop the whole loop.
                        Err(e)
                    }
                }
            })
    }

/// Generates a summary DataFrame of all RFQs processed in the simulation.
    ///
    /// This table serves as the primary "Trade Journal" for Flow Trading.
    /// It aggregates the history of each RFQ into a single result row,
    /// allowing for high-level analysis of Hit-Rates, PnL, and Client Performance.
    ///
    /// # Structure
    /// - Granularity: One row per `RfqId`.
    /// - Scope: Includes both active (unresolved) and archived (finalized) RFQs.
    pub fn blotter(&self) -> ChapatyResult<DataFrame> {
        // Wir iterieren über alle Episoden
        let rows = self.0.iter().enumerate().flat_map(|(ep_idx, states)| {
            // Wir iterieren über ALLE RFQs (Live + Archive) in diesem State
            states.iter_all_rfqs().map(move |(rfq_id, history)| {
                // Analyse der Historie für Summary
                let first = history.first().expect("History cannot be empty");
                let last = history.last().expect("History cannot be empty");
                
                // Extrahiere Basis-Daten aus dem "Open" State (immer der erste)
                let meta = first.as_open_rfq().expect("First state must be Open");
                
                // Extrahiere Ergebnis aus dem letzten State
                let (outcome_str, final_price) = match last {
                    State::Finalized(f) => (f.outcome.to_string(), f.outcome.price()),
                    _ => ("In Progress".to_string(), None),
                };

                // Mapping auf Spalten (Vereinfacht als Struct oder Tuple für den Builder)
                (
                    ep_idx as u64,
                    rfq_id.0,
                    meta.client_id.to_string(),
                    meta.symbol.to_string(),
                    meta.side.to_string(),
                    meta.quantity,
                    meta.created_at,
                    last.timestamp(), // End Time
                    history.len() as u32, // Rounds
                    outcome_str,
                    final_price,
                )
            })
        });

        // Hier würdest du deinen Polars Builder (Soa/Series) nutzen, 
        // um "rows" in einen DataFrame zu verwandeln.
        // (Code analog zu deinem Trading-Modul, nur mit anderen Spalten)
        unimplemented!("Construct DataFrame from rows iterator")
    }

    /// Generates a detailed audit log of every state change during negotiations.
    ///
    /// This table is used for "Micro-Structure Analysis":
    /// - How often do we re-quote?
    /// - How fast does the client counter?
    /// - What was the price trajectory during the negotiation?
    ///
    /// # Structure
    /// - Granularity: One row per `Revision` (State Change).
    /// - Scope: Flattens the history vector of every RFQ.
    pub fn transcript(&self) -> ChapatyResult<DataFrame> {
        let rows = self.0.iter().enumerate().flat_map(|(ep_idx, states)| {
            // Doppeltes flat_map: Episode -> RFQ -> Revisions
            states.iter_all_rfqs().flat_map(move |(rfq_id, history)| {
                history.iter().enumerate().map(move |(rev_idx, state)| {
                    
                    // Bestimme Preis für diese Revision (falls vorhanden)
                    let price = match state {
                        State::Quoted(q) => Some(q.my_price),
                        State::Countered(c) => Some(c.client_price),
                        State::Finalized(f) => f.outcome.price(),
                        State::Open(_) => None,
                    };

                    (
                        ep_idx as u64,
                        rfq_id.0,
                        rev_idx as u32, // Revision ID
                        state.timestamp(),
                        state.variant_name(), // "Quoted", "Open"...
                        price
                    )
                })
            })
        });

        unimplemented!("Construct DataFrame from rows iterator")
    }


}

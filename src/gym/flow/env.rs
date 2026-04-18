use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

use chrono::Duration;
use rand::rngs::ThreadRng;
use tracing::{info, warn};

use crate::{
    agent::AgentIdentifier,
    data::{common::RiskMetricsConfig, episode::Episode, event::TradesId, view::StreamView},
    error::{ChapatyResult, EnvError},
    gym::{
        EnvStatus, InvalidActionPenalty, Reward, StepOutcome,
        flow::{
            Env,
            action::{Action as FlowAction, Actions},
            context::{ActionCtx as FlowActionCtx, ActionSummary as FlowActionSummary},
            domain::{ClientProfile, ClientTier},
            fill::{ClientReaction, ResponseModel},
            generator::RfqGenerator,
            ledger::Ledger,
            observation::Observation,
            scheduler::{RfqEvent, ScheduledEvent, ScheduledStep, Scheduler},
            state::{Open, Rfq, States as FlowStates},
        },
        trading::{
            config::ExecutionBias,
            context::{
                ActionCtx as StreetActionCtx, ActionSummary as StreetActionSummary, UpdateCtx,
            },
            state::States as StreetStates,
        },
    },
    sim::data::SimulationData,
};

#[derive(Clone, Debug)]
pub struct Environment {
    // === Public (configurable) ===
    /// Determines how ambiguous trade outcomes are resolved during evaluation.
    ///
    /// In edge cases where a trade could both hit stop-loss and take-profit
    /// within the same timeframe, this mode selects the evaluation strategy:
    ///
    /// - [`ExecutionBias::Optimistic`] chooses the most favorable result for the agent.
    /// - [`ExecutionBias::Pessimistic`] chooses the least favorable outcome.
    ///
    /// Defaults to [`ExecutionBias::Pessimistic`] for conservative evaluation.
    bias: ExecutionBias,

    /// Penalty applied when an agent submits an **invalid action**.
    ///
    /// Invalid actions are ignored, but the environment still advances time
    /// and returns this penalty as the reward for the step.
    ///
    /// Defaults to `Reward(0)` but can be tuned per environment to control
    /// exploration pressure.
    invalid_action_penalty: InvalidActionPenalty,

    /// Configuration for risk metrics calculations, such as Sharpe ratio.
    risk_metrics_cfg: RiskMetricsConfig,

    // === Internal only ===
    /// Shared simulation data backing the environment.
    sim_data: Arc<SimulationData>,

    /// Simulates client behavior (acceptance/rejection of quotes).
    response_model: ResponseModel,

    /// Generates synthetic RfQs based on market activity.
    rfq_generator: RfqGenerator,

    /// The Priority Queue (Min-Heap) for future events.
    /// Uses `Reverse` wrapper to ensure the smallest timestamp pops first.
    event_queue: BinaryHeap<Reverse<ScheduledEvent>>,

    /// The central state database (Hot & Cold storage).
    ledger: Ledger,

    /// The Master Clock. Coordinates Market Data and Event Queue.
    scheduler: Scheduler,

    /// Current Episode configuration.
    ep: Episode,

    /// **Counterparty Master Data (CRM).**
    /// Stores immutable profiles (KYC, Risk Limits) for all known clients.
    /// Ensures consistent client identity across trades.
    counterpart_master: HashMap<AgentIdentifier, ClientProfile>,

    /// Snapshot for resetting.
    initial_ep: Episode,

    /// Lifecycle status.
    env_status: EnvStatus,

    trade_feed_ids: Arc<[TradesId]>,

    rng: ThreadRng,
}

impl Environment {
    /*

    TODO: Publich helper and setter methods
    - fill_simulator
    - rfq_generator
    - cache
    - evaluate_agent
    - evaluate_agents
    - invalid_action_penalty
    - other...?

    -> Note, that we want event driven logging similar to kafka and the command
       sourcing approach in trading module

    User Story: Asynchrones Cross-Hedging von OTC-TradesAls Quantitative Trader oder Risk Manager
        Bei einer Tier-2/3 Bankmöchte ich ein High-Performance Framework nutzen, um das Risiko
        meiner RfQ-Trades automatisch gegen ein liquides Instrument (Proxy-Hedge) abzusichern
        und die Performance dieser Strategie über historische Daten zu simulieren,damit ich
        das Restrisiko (Basis-Risk) und die Auswirkungen von Latenz auf meine Profitabilität (PnL)
        verstehen kann, bevor ich echtes Kapital im Fixed-Income-Markt einsetze.Akzeptanzkriterien (Requirements für deine Rust-Engine)
            1. Die "Simulation Engine" (Asynchroner Kern)A1 (RfQ Trigger): Wenn ein RfQ durch die Sigmoid-Funktion (Probabilistic Filling) als "Gewonnen" markiert wird, muss das System ein PositionUpdateEvent auslösen.
            A2 (Hedge Execution): Die Engine muss sofort (asynchron) eine Gegenposition in einem definierten Hedge-Instrument (z.B. BTC-Future für Altcoin-Spot oder Bund-Future für Italien-Bond) eröffnen.
            A3 (Latenz-Simulation): Der Hedge darf nicht zum Preis des RfQ-Zeitpunkts \(t\) ausgeführt werden, sondern muss eine konfigurierbare Verzögerung (z.B. \(t+500ms\) oder die nächste 1m-Kerze) berücksichtigen.
            2. Das "Risk Mapping" (Flexible Logik)B1 (Hedge Provider): Es muss ein Trait existieren, der für ein Asset \(X\) das korrekte Hedge-Instrument \(Y\) und die berechnete Hedge-Ratio (basierend auf Beta oder PVBP) zurückgibt.
            B2 (Dynamic Inventory): Das System muss das Netto-Exposure (Asset-Position minus Hedge-Position) in Echtzeit tracken.
            3. Die Simulation von "Markt-Stress" (Basis Risk)C1 (Correlation Breakdown): Der Backtester muss Szenarien erlauben, in denen die Korrelation zwischen Asset und Hedge künstlich verschlechtert wird (z.B. Asset fällt, Hedge steigt), um das Risiko bei Extremereignissen zu messen.
            C2 (Slippage): Beim Ausführen des Hedges muss Slippage basierend auf dem simulierten Volumen berechnet werden.Technischer Implementierungs-Plan (für dich als Entwickler)Phase 1 (Krypto-MVP):Asset: SOL/USDT (Spot).Hedge-Instrument: BTC/USDT (Perpetual Future).Hedge-Ratio: Fixes Beta (z.B. 1.2) via JSON-Config.Daten: Deine OHLCV-1m Daten für beide Assets.
            Phase 2 (Die Visualisierung im Marimo-Dashboard):Zeige zwei Linien: "Unhedged PnL" (Volatilität des Assets) vs. "Hedged PnL" (nur der eingefangene Spread abzüglich des Basis-Risikos).Metrik: "Hedge Efficiency" (Wie viel der Varianz wurde durch den Hedge eliminiert?).
            Phase 3 (Der "Concierge"-Pitch für die Bank):Du ersetzt den JSON-Hedge-Provider durch eine Logik, die Bund-Futures versteht.Du erklärst: "Die Engine bleibt gleich, wir tauschen nur die Datenquelle und die Hedge-Ratio-Berechnung aus."
    */
}

impl Env for Environment {
    #[tracing::instrument(skip(self), fields(ep_id = %self.ep.id().0))]
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        use EnvStatus::*;

        match self.env_status {
            EpisodeDone => {
                if let Some(next_ep) = self
                    .scheduler
                    .advance_to_next_episode(&self.sim_data, self.ep)?
                {
                    self.ep = next_ep;
                    info!(
                        episode_id = %self.ep.id().0,
                        start_time = %self.ep.start(),
                        "Episode Starting (Next Sequence)"
                    );
                } else {
                    self.restart();
                    info!("End of Data Reached. Performing Full Environment Reset.");
                }
            }
            Ready | Done | Running => {
                self.restart();
                info!("Environment Reset Initiated.");
            }
        }

        self.env_status = EnvStatus::Running;
        self.event_queue.clear();
        self.counterpart_master.clear();

        let obs = Observation {
            market_view: self.scheduler.market_view(&self.sim_data)?,
            flow_states: self.flow_states(&self.ep)?,
            street_states: self.street_states(&self.ep)?,
        };

        Ok((obs, Reward(0), StepOutcome::InProgress))
    }

    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        self.check_step_status()?;
        let episode = self.episode();

        // Snapshot für Validierung & Pricing
        let market_before = self.scheduler.market_view(&self.sim_data)?;

        // -----------------------------------------------------------------
        // PHASE 1: STREET TRADING (Liquidität & Hedges)
        // -----------------------------------------------------------------
        let street_ctx = StreetActionCtx {
            actions: actions.street,
            market: market_before.clone(),
        };
        let street_summary = self.ledger.apply_street_actions(&episode, street_ctx)?;

        // -----------------------------------------------------------------
        // PHASE 2: CLIENT FLOW (Business)
        // -----------------------------------------------------------------
        let flow_ctx = FlowActionCtx {
            actions: actions.flow,
            market: market_before,
        };
        let flow_summary = self.ledger.apply_flow_actions(&episode, flow_ctx)?;

        // Side Effects: "Verarbeite die Konsequenzen der erfolgreichen Aktionen"
        // Der Loop ist jetzt hier versteckt.
        self.schedule_client_responses(&flow_summary.executed)?;

        // -----------------------------------------------------------------
        // PHASE 3: TIMING (Scheduling)
        // -----------------------------------------------------------------
        // Der Loop ist jetzt hier versteckt.
        self.schedule_wakeups(&actions.wait);

        // 4. Transition Dynamics
        let (outcome, total_reward) = self.transition(&episode, street_summary, flow_summary)?;

        // 5. Update Status & Observe
        self.update_env_status(outcome)?;

        let obs = Observation {
            market_view: self.scheduler.market_view(&self.sim_data)?,
            flow_states: self.flow_states(&self.ep)?,
            street_states: self.street_states(&self.ep)?,
        };

        Ok((obs, total_reward, outcome))
    }
}

impl Environment {
    /*
    // TODO implement for grid search
    fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.journal()?.portfolio_performance()
    }
    */

    fn restart(&mut self) {
        self.scheduler.reset(&self.sim_data);
        self.ledger.clear();
        self.ep = self.initial_ep;
        self.counterpart_master.clear();
    }

    fn flow_states(&self, ep: &Episode) -> ChapatyResult<&FlowStates> {
        self.ledger.flow_states(ep)
    }
    fn street_states(&self, ep: &Episode) -> ChapatyResult<&StreetStates> {
        self.ledger.street_states(ep)
    }

    /// Advances the simulation loop until an agent intervention is required or the episode ends.
    ///
    /// # Loop Logic
    /// 1. Takes the next chronological step from the Scheduler.
    /// 2. If it's market data (Tick) -> Try generating new RfQs -> Continue.
    /// 3. If it's a business event (Event):
    ///    - `WakeUp` -> Agent timer expired -> Break.
    ///    - `NewRequest` -> New opportunity -> Break.
    ///    - `CustomerReply` ->
    ///         - If Trade (Accept) -> Inventory changed -> Break (Hedge!).
    ///         - If Counter -> Negotiation needed -> Break.
    ///         - If Reject -> Nothing to do -> Continue.
    fn transition(
        &mut self,
        ep: &Episode,
        street_summary: StreetActionSummary, // Input from step()
        flow_summary: FlowActionSummary,     // Input from step()
    ) -> ChapatyResult<(StepOutcome, Reward)> {
        use ScheduledStep::*;

        // --- THE FAST-FORWARD LOOP ---
        loop {
            // 1. Get next tick from Scheduler (Option API)
            // Wir nutzen direkt den Scheduler, advance_time Helper ist hier unnötiger Boilerplate.
            let step = match self.advance_time()? {
                Some(s) => s,
                None => break, // End of Data -> Break loop to finalize episode
            };

            match step {
                // Case A: Market Tick (Noise or Generator Trigger)
                Market { timestamp: _ } => {
                    // Check if market move triggers new client interest
                    self.try_generate_rfq()?;
                    // Continue loop implicitly
                }

                // Case B: Discrete Events
                Event(evt) => match evt.event_type {
                    // 1. Wecker (Agent Strategy)
                    RfqEvent::WakeUp => {
                        break; // Intervention Point
                    }

                    // 2. Neuer Kunde (Sales Opportunity)
                    RfqEvent::NewRequest(rfq) => {
                        self.ledger.register_request(rfq, &self.ep)?;
                        break; // Intervention Point
                    }

                    // 3. Antwort vom Kunden
                    RfqEvent::CustomerReply { rfq_id, decision } => {
                        // Ledger updated den State (Quotes -> Filled/Rejected)
                        // Wichtig: process_client_reply muss robust sein gegen "Zombie RfQs"
                        self.ledger
                            .process_client_reply(rfq_id, decision, &self.ep)?;

                        // Decision Logic:
                        // - Accept: Inventory hat sich geändert -> Agent muss hedgen können -> Break.
                        // - Counter: Agent muss accept/reject entscheiden -> Break.
                        // - Reject: RfQ ist tot. Nichts zu tun -> Continue.
                        if decision.requires_agent_reply() {
                            break; // Intervention Point
                        }
                    }

                    // 4. Timeout
                    RfqEvent::Expired { rfq_id } => {
                        self.ledger.archive_expired(rfq_id, &self.ep)?;
                        // Continue loop
                    }
                },
            }
        } // End Loop

        // --- POST-LOOP: OBSERVATION & REWARD ---

        // 1. Mark-to-Market Valuation
        // Wir bewerten das Portfolio zum Zeitpunkt des Breaks (t_now).
        // Hinweis: Wenn Loop via 'None' (EOF) beendet wurde, nehmen wir den allerletzten Preis.
        let market_now = self.scheduler.market_view(&self.sim_data)?;
        let update_ctx = UpdateCtx {
            market: &market_now,
            bias: self.bias,
        };
        self.ledger.apply_updates(ep, &update_ctx)?;

        // 2. Calculate Rewards
        // PnL Delta (aus Ledger) + Penalty für ungültige Aktionen (aus Summaries)
        let reward_delta = self.ledger.pop_step_reward(ep)?;

        // Wir berechnen die Penalty für BEIDE Action-Typen
        let penalty = self.penalty(&street_summary, &flow_summary);

        let total_reward = reward_delta + penalty.0;

        // 3. Determine Episode Status (Done/Truncated/InProgress)
        let outcome = self.evaluate_outcome(ep)?;

        Ok((outcome, total_reward))
    }

    fn advance_time(&mut self) -> ChapatyResult<Option<ScheduledStep>> {
        self.scheduler
            .step(&self.sim_data, &self.ep, &mut self.event_queue)
    }

    fn evaluate_outcome(&self, ep: &Episode) -> ChapatyResult<StepOutcome> {
        if self.ep.is_episode_end(self.scheduler.current_ts())
            || self
                .scheduler
                .is_end_of_data(&self.sim_data, &self.event_queue)
        {
            if self.ledger.is_terminal(ep)? {
                Ok(StepOutcome::Terminated)
            } else {
                Ok(StepOutcome::Truncated)
            }
        } else {
            Ok(StepOutcome::InProgress)
        }
    }

    fn schedule_wakeup(&mut self, duration: Duration) {
        let wake_time = self.scheduler.current_ts() + duration;
        self.event_queue.push(Reverse(ScheduledEvent {
            timestamp: wake_time,
            event_type: RfqEvent::WakeUp,
        }));
    }

    // ================================================================================================
    // Generator & Client Logic
    // ================================================================================================

    /// Checks all trade feeds. If a trade happened *exactly* at the current scheduler timestamp,
    /// we try to generate an RfQ from it.
    fn try_generate_rfq(&mut self) -> ChapatyResult<()> {
        let current_ts = self.scheduler.current_ts();

        // Phase 1: Collect (Immutable Borrow von self via market_view)
        // Wir sammeln die RfQs in einem temporären Vektor.
        let mut new_rfqs = Vec::with_capacity(self.trade_feed_ids.len());

        {
            // Scope limitieren, damit der Borrow von market_view hier endet
            let market_view = self.scheduler.market_view(&self.sim_data)?;

            for trade_id in self.trade_feed_ids.iter() {
                if let Some(trade_event) = market_view.trades().last_event(trade_id) {
                    // Freshness Check
                    if trade_event.timestamp != current_ts {
                        continue;
                    }

                    // Generator Logik
                    if let Some(rfq) = self
                        .rfq_generator
                        .try_generate(trade_event, &trade_id.symbol)?
                    {
                        new_rfqs.push(rfq);
                    }
                }
            }
        } // <--- Hier wird market_view gedroppt, der Borrow auf self endet.

        // Phase 2: Process (Mutable Borrow von self)
        for rfq in new_rfqs {
            // A. Client Stammdaten (Static Data) Update
            self.register_client_if_new(&rfq);

            // B. Events pushen
            self.event_queue.push(Reverse(ScheduledEvent {
                timestamp: current_ts,
                event_type: RfqEvent::NewRequest(rfq.clone()),
            }));

            self.event_queue.push(Reverse(ScheduledEvent {
                timestamp: rfq.header.time_to_live,
                event_type: RfqEvent::Expired { rfq_id: rfq.header.rfq_id },
            }));
        }

        Ok(())
    }

    /// Ensures consistency between Transient RfQs and Persistent Client Profiles.
    fn register_client_if_new(&mut self, rfq: &Rfq<Open>) {
        if !self.counterpart_master.contains_key(&rfq.header.client_id) {
            // Define default limits based on the Tier (Intrinsic property)
            let default_limit = match rfq.header.client_tier {
                ClientTier::Tier1 => 50_000_000.0, // Hedge Funds
                ClientTier::Tier2 => 10_000_000.0, // Asset Managers
                ClientTier::Tier3 => 1_000_000.0,  // Retail
            };

            let profile = ClientProfile {
                id: rfq.header.client_id.clone(),
                tier: rfq.header.client_tier,
                max_credit_limit: default_limit,
            };

            self.counterpart_master
                .insert(rfq.header.client_id.clone(), profile);

            // Sync with Ledger's view of exposure (init to 0.0)
            self.ledger.init_client_exposure(rfq.header.client_id.clone());
        }
    }

    /// Verarbeitet erfolgreich ausgeführte Flow-Aktionen und plant notwendige
    /// Reaktionen der Umwelt (Kunden-Antworten).
    fn schedule_client_responses(&mut self, executed_actions: &[FlowAction]) -> ChapatyResult<()> {
        let market_view = self.scheduler.market_view(&self.sim_data)?;

        for action in executed_actions {
            // 1. Filter: Braucht diese Aktion überhaupt eine Antwort?
            // (Quote = Ja, Reject = Nein, Ignore = Nein)
            if !action.requires_client_reply() {
                continue;
            }

            let rfq_id = action.rfq_id();

            // Ledger fragen: Was haben wir eigentlich angeboten?
            let quote = self.ledger.get_quote_details(rfq_id)?;

            // Marktdaten holen (Close als Approximation für Mid Price)
            // Wenn wir keine Daten haben, kann der Kunde nicht entscheiden -> "Technisches Ghosting"
            let mid_price = match market_view.try_resolved_close_price(&quote.symbol) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Profil holen für Tier-Information
            let client_profile = self.counterpart_master.get(&quote.client_id).unwrap();
            // TODO .ok_or(EnvError::ClientNotFound(quote.client_id.clone()))?;

            // JETZT: Delegieren wir alles an das ResponseModel (Das Orakel)
            // Die Env weiß nicht, wie Ghosting oder Latenz funktioniert.
            let reaction = self.response_model.react(
                &mut self.rng,
                quote.price,
                mid_price,
                quote.side,
                client_profile.tier,
            );

            match reaction {
                ClientReaction::Respond { decision, latency } => {
                    // Der Kunde antwortet! (Entweder ACCEPT oder REJECT)
                    let reply_time = self.scheduler.current_ts() + latency;

                    // Wir pushen das Event. Der 'decision' Enum enthält das Ergebnis.
                    self.event_queue.push(Reverse(ScheduledEvent {
                        timestamp: reply_time,
                        event_type: RfqEvent::CustomerReply { rfq_id, decision },
                    }));
                }
                ClientReaction::Ignore => {
                    // TODO invariante testen
                    // Ghosting -> Nichts tun.
                    // Das System verlässt sich darauf, dass der RfQ später durch das
                    // 'Expired' Event (das bei Erstellung geplant wurde) aufgeräumt wird.
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Verarbeitet explizite Warte-Wünsche des Agenten.
    fn schedule_wakeups(&mut self, durations: &[chrono::Duration]) {
        let now = self.scheduler.current_ts();

        for duration in durations {
            let wake_time = now + *duration;

            self.event_queue.push(Reverse(ScheduledEvent {
                timestamp: wake_time,
                event_type: RfqEvent::WakeUp,
            }));
        }
    }

    // ================================================================================================
    // Boilerplate & Helpers
    // ================================================================================================

    fn penalty(
        &self,
        street_summary: &StreetActionSummary,
        flow_summary: &FlowActionSummary,
    ) -> Reward {
        let p = flow_summary.rejected.len() as i64 + street_summary.rejected as i64;
        Reward(p * self.invalid_action_penalty.0.0)
    }

    fn episode(&self) -> Episode {
        self.ep
    }

    fn check_step_status(&self) -> ChapatyResult<()> {
        use EnvStatus::*;
        match self.env_status {
            Running => Ok(()),
            Ready => Err(EnvError::InvalidState("Call reset() first".into()).into()),
            EpisodeDone | Done => Err(EnvError::InvalidState("Episode Done".into()).into()),
        }
    }

    fn update_env_status(&mut self, outcome: StepOutcome) -> ChapatyResult<()> {
        // TODO check for possible bug if sim_data is empty but queue is not empty then we should still return DONE, shouldn't we?
        if outcome.is_terminal() {
            self.env_status = if self
                .scheduler
                .is_end_of_data(&self.sim_data, &self.event_queue)
            {
                EnvStatus::Done
            } else {
                EnvStatus::EpisodeDone
            };
        }
        Ok(())
    }
}

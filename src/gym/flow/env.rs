// src/gym/flow/env.rs

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use tracing::{debug, info, trace, warn};

use crate::{
    agent::AgentIdentifier,
    data::{episode::Episode, view::MarketView, domain::Symbol},
    error::{ChapatyResult, EnvError},
    gym::{
        EnvStatus, InvalidActionPenalty, Reward, StepOutcome,
        flow::{
            Env,
            action::Actions,
            context::{ActionCtx, ActionSummary},
            domain::{ClientProfile, ClientTier},
            fill::FillSimulator,
            generator::RfqGenerator,
            ledger::Ledger,
            observation::Observation,
            scheduler::{Scheduler, SchedulerOutcome, ScheduledEvent, RfqEvent},
            state::{States, Open},
        },
    },
    sim::{data::SimulationData, cursor_group::CursorGroup},
};

use std::collections::HashMap;

/// The Flow Trading Environment.
///
/// Unlike standard OHLTCv environments, this environment is **Event-Driven**.
/// Time is handled by a `Scheduler` that interleaves discrete market data ticks
/// with synthetic events (RFQs, Client Replies, Expirations).
///
/// # The Master Clock Logic
/// The `step` function advances time continuously until one of two things happens:
/// 1. **Agent Intervention Required:** A new RFQ arrives (`NewRequest`) or a negotiation updates.
/// 2. **Episode Ends:** The data stream is exhausted or time runs out.
#[derive(Clone, Debug)]
pub struct Environment {
    // === Internal only ===
    /// Shared simulation data backing the environment.
    sim_data: Arc<SimulationData>,

    /// Simulates client behavior (acceptance/rejection of quotes).
    fill_simulator: FillSimulator,
    
    /// Generates synthetic RFQs based on market activity.
    rfq_generator: RfqGenerator,
    
    /// The Priority Queue (Min-Heap) for future events.
    /// Uses `Reverse` wrapper to ensure the smallest timestamp pops first.
    event_queue: BinaryHeap<Reverse<ScheduledEvent>>,

    /// Penalty applied when an agent submits an **invalid action**.
    invalid_action_penalty: InvalidActionPenalty,

    /// The central state database (Hot & Cold storage).
    ledger: Ledger,

    /// The Master Clock. Coordinates Market Data and Event Queue.
    scheduler: Scheduler,

    /// Current Episode configuration.
    ep: Episode,

    /// **Counterparty Master Data (CRM).**
    /// Stores immutable profiles (KYC, Risk Limits) for all known clients.
    /// Used to prevent "Split Brain" by ensuring consistent client identity across trades.
    counterpart_master: HashMap<AgentIdentifier, ClientProfile>,
    
    /// Snapshot for resetting.
    initial_ep: Episode,

    /// Lifecycle status.
    env_status: EnvStatus,
}

impl Env for Environment {
    #[tracing::instrument(skip(self), fields(ep_id = %self.ep.id().0))]
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        use EnvStatus::*;

        match self.env_status {
            EpisodeDone => {
                // Try to advance to the next episode in the sequence
                // Note: Scheduler manages the cursor internally
                if let Some(next_ep) = self.scheduler.advance_to_next_episode(&self.sim_data, self.ep)? {
                    self.ep = next_ep;
                    info!(episode_id = %self.ep.id().0, "Episode Starting (Next Sequence)");
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

        // Reset Transient State
        self.ledger.clear();
        self.event_queue.clear();
        
        // Note: We DO NOT clear counterpart_master if we want to simulate long-term relationships (Memory).
        // If we want pure episodic RL independence, we should clear it.
        // For this implementation, we keep it to simulate a "Trading Day/Month".
        
        let obs = Observation {
            market_view: MarketView::new(&self.sim_data, &self.scheduler.cursor())?,
            states: self.states(&self.ep)?,
        };

        Ok((obs, Reward(0), StepOutcome::InProgress))
    }

    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        self.check_step_status()?;
        let episode = self.episode();

        // 1. Snapshot S(t) - Market view at the moment of decision
        let market_before = MarketView::new(&self.sim_data, &self.scheduler.cursor())?;

        // 2. Apply Actions A(t) -> S'(t)
        // The agent sends Quotes, Rejects, or Counters.
        let action_ctx = ActionCtx {
            actions,
            market: market_before,
        };
        
        // The ledger updates state to "Quoted", "Finalized", etc.
        let summary = self.ledger.apply_actions(&episode, action_ctx)?;

        // CRITICAL: If the agent sent a Quote, we must schedule the Client's Reply!
        // This is where the Agent's action triggers a future event.
        self.schedule_client_responses(&summary)?;

        // 3. Transition Dynamics (The Master Clock Loop)
        // Fast-forward time until the next event the agent needs to see.
        let (outcome, total_reward) = self.transition(&episode, summary)?;

        // 4. Update Status & Observe
        self.update_env_status(outcome)?;

        let market_final = MarketView::new(&self.sim_data, &self.scheduler.cursor())?;
        let obs = Observation {
            market_view: market_final,
            states: self.states(&episode)?,
        };

        Ok((obs, total_reward, outcome))
    }
}

impl Environment {
    
    // ========================================================================
    // Core Transition Logic (The Heart of Flow)
    // ========================================================================

    /// Advances the simulation loop until an agent intervention is required or the episode ends.
    fn transition(
        &mut self,
        ep: &Episode,
        summary: ActionSummary,
    ) -> ChapatyResult<(StepOutcome, Reward)> {
        
        // Loop continuously...
        loop {
            // ...ask the Scheduler for the next event in chronological order.
            // This handles the min-heap priority vs market data tick logic.
            match self.scheduler.step(&self.sim_data, ep, &mut self.event_queue)? {
                
                // Case A: The Market Moved (Tick)
                SchedulerOutcome::MarketTick { timestamp: _ } => {
                    // 1. Check if this market move triggers a new RFQ (Generator)
                    self.try_generate_rfq()?;
                    
                    // 2. Check if any "Till-Maturity" logic needs update (e.g. expiring RFQs)
                    // (Handled implicitly by ScheduledEvent::Expiration in queue)
                    
                    // Continue loop! The agent doesn't need to wake up for every tick 
                    // unless an RFQ is waiting.
                }

                // Case B: A Synthetic Event Occurred (RFQ, Reply, Expiration)
                SchedulerOutcome::SyntheticEvent(event) => {
                    match event.event_type {
                        // B1: New RFQ -> WAKE UP AGENT
                        RfqEvent::NewRequest(rfq) => {
                            // Register State
                            self.ledger.on_rfq_received(rfq);
                            
                            // Break loop so agent can act
                            break; 
                        },
                        
                        // B2: Client Replied -> Update Ledger -> Continue
                        RfqEvent::CustomerReply { rfq_id, decision } => {
                            self.ledger.on_customer_reply(rfq_id, decision)?;
                            // Don't break. Loop continues until next "NewRequest" or "Action required".
                            // Note: If we want the agent to see "Fill Confirmed" immediately, we could break here.
                            // But usually, fills are just booked.
                        },
                        
                        // B3: Time To Live Expired -> Update Ledger -> Continue
                        RfqEvent::Expired { rfq_id } => {
                            self.ledger.on_rfq_expired(rfq_id)?;
                        }
                    }
                }

                // Case C: End of Episode
                SchedulerOutcome::Done => {
                    return self.finalize_step(ep, summary, StepOutcome::Terminated);
                }
            }
            
            // Safety Check: If we have Pending Actions in the Ledger that require attention
            // (e.g., Counter-Offer received), we should break.
            // For MVP: We assume only NewRequest triggers Agent. 
            // Future: Counter-Offers also trigger break.
        }

        // Calculate Rewards after the time jump
        self.finalize_step(ep, summary, StepOutcome::InProgress)
    }

    /// Helper to wrap up the transition, calculating PnL and Rewards.
    fn finalize_step(
        &mut self,
        ep: &Episode,
        summary: ActionSummary,
        outcome: StepOutcome
    ) -> ChapatyResult<(StepOutcome, Reward)> {
        // Mark-to-Market Update (Portfolio Valuation)
        let market_now = MarketView::new(&self.sim_data, &self.scheduler.cursor())?;
        self.ledger.update_valuations(&market_now);

        // Pop accumulated rewards (PnL change + Invalid Actions)
        let reward_delta = self.ledger.pop_step_reward(ep)?;
        let penalty = self.penalty(summary);
        
        Ok((outcome, reward_delta + penalty))
    }

    // ========================================================================
    // Generator & Client Logic
    // ========================================================================

    /// Checks the generator against the current market tick.
    /// If an RFQ is generated, registers the client and pushes the event to the queue.
    fn try_generate_rfq(&mut self) -> ChapatyResult<()> {
        // Access current trade from cursor via scheduler
        let cursor = self.scheduler.cursor();
        
        if let Some(trade) = cursor.trade().current() {
            // Get Context (Symbol)
            let market_id = cursor.trade().market_id();
            let source_symbol = &self.sim_data.markets[market_id].symbol;

            if let Some(rfq) = self.rfq_generator.try_generate(trade, source_symbol) {
                // 1. SPLIT BRAIN PREVENTION: Lazy Load Client
                self.register_client_if_new(&rfq);

                // 2. Create Event
                let event = ScheduledEvent {
                    timestamp: rfq.created_at_dt(), // Helper needed on Rfq to get DateTime
                    event_type: RfqEvent::NewRequest(rfq),
                };

                // 3. Push to Min-Heap (Reverse for correct ordering)
                self.event_queue.push(Reverse(event));
            }
        }
        Ok(())
    }

    /// Ensures consistency between Transient RFQs and Persistent Client Profiles.
    fn register_client_if_new(&mut self, rfq: &Rfq<Open>) {
        if !self.counterpart_master.contains_key(&rfq.client_id) {
            
            // Define default limits based on the Tier (Intrinsic property)
            let default_limit = match rfq.client_tier {
                ClientTier::Tier1 => 50_000_000.0, // Hedge Funds
                ClientTier::Tier2 => 10_000_000.0, // Asset Managers
                ClientTier::Tier3 => 1_000_000.0,  // Retail
            };

            let profile = ClientProfile {
                id: rfq.client_id.clone(),
                tier: rfq.client_tier,
                max_credit_limit: default_limit,
            };

            self.counterpart_master.insert(rfq.client_id.clone(), profile);
            
            // Sync with Ledger's view of exposure (init to 0.0)
            self.ledger.init_client_exposure(rfq.client_id.clone());
        }
    }

    /// If the agent sent quotes, calculate outcomes and schedule future replies.
    fn schedule_client_responses(&mut self, summary: &ActionSummary) -> ChapatyResult<()> {
        for (rfq_id, my_quote) in &summary.quoted_rfqs {
            // 1. Get current market mid for this RFQ's symbol
            // Note: We need to find the mid-price. Ledger likely knows the symbol.
            // For MVP: We assume we can look it up or passed it in summary.
            // Simplified: We assume fill_simulator needs inputs we have.
            
            // ... Logic to get Mid Price ...
            let mid_price = 100.0; // Placeholder: Fetch from self.market_view or ledger

            // 2. Simulate Decision
            // Using a deterministic RNG based on RFQ ID to keep simulation stable
            // let outcome = self.fill_simulator.decide(..., my_quote, mid_price, ...);
            
            // 3. Schedule Event (e.g., 100ms later)
            // let reply_time = self.scheduler.current_time() + Duration::milliseconds(100);
            // self.event_queue.push(Reverse(ScheduledEvent { ... }));
        }
        Ok(())
    }

    // ========================================================================
    // Boilerplate & Helpers
    // ========================================================================

    fn states(&self, ep: &Episode) -> ChapatyResult<&States> {
        self.ledger.get(ep)
    }

    fn penalty(&self, report: ActionSummary) -> Reward {
        Reward((report.rejected as i64) * self.invalid_action_penalty.0.0)
    }

    fn episode(&self) -> Episode {
        self.ep
    }

    fn restart(&mut self) {
        self.scheduler.reset(&self.sim_data); 
        self.ep = self.initial_ep;
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
        if outcome.is_terminal() {
            self.env_status = if self.scheduler.cursor().is_end_of_data(&self.sim_data) {
                EnvStatus::Done
            } else {
                EnvStatus::EpisodeDone
            };
        }
        Ok(())
    }
}
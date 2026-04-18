use rand::Rng;

use crate::{
    data::domain::Price,
    gym::flow::{
        config::ClientConfig,
        domain::{ClientTier, Side},
        scheduler::CustomerDecision,
    },
};
use chrono::Duration;

/// Das Ergebnis einer Kunden-Interaktion.
pub enum ClientReaction {
    /// Der Kunde antwortet aktiv (Accept oder Reject) nach einer gewissen Zeit.
    Respond {
        decision: CustomerDecision,
        latency: Duration,
    },
    /// Der Kunde ignoriert uns komplett (Ghosting).
    /// Führt dazu, dass der RfQ später via Timeout (TTL) ausläuft.
    Ignore,
}

/// The "Oracle" that simulates client decision making based on the configuration.
#[derive(Debug, Clone)]
pub struct ResponseModel {
    config: ClientConfig,
}

impl ResponseModel {
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Der Haupteinstiegspunkt: Simuliert Ghosting, Latenz und die Entscheidung.
    pub fn react(
        &self,
        rng: &mut impl Rng,
        quote: Price,
        mid_price: Price,
        side: Side,
        tier: ClientTier,
    ) -> ClientReaction {
        // 1. Ghosting Check (Response Rate)
        // Simuliert technische Fehler oder Desinteresse, überhaupt zu klicken.
        if !rng.random_bool(self.config.response_rate) {
            return ClientReaction::Ignore;
        }

        // 2. Latenz berechnen
        // Wir rufen die Helper-Methode auf der Config auf (diese nutzt das LatencyModel)
        let latency_ms = self.config.sample_latency(rng);
        let latency = Duration::milliseconds(latency_ms);

        // 3. Inhaltliche Entscheidung treffen (Accept vs. Reject)
        let decision = self.decide(rng, quote, mid_price, side, tier);

        ClientReaction::Respond { decision, latency }
    }

    /// Determines whether the client accepts the quoted price based on a logistic function.
    ///
    /// $$ P(\text{Accept}) = \frac{1}{1 + e^{-\alpha \cdot (\text{Advantage} + \text{Bias})}} $$
    fn decide(
        &self,
        rng: &mut impl Rng,
        my_quote: Price,
        mid_price: Price,
        side: Side,
        tier: ClientTier,
    ) -> CustomerDecision {
        let params = &self.config.fill_model;

        // 1. Calculate Advantage (in pure Price delta first)
        // Side aus Sicht des CLIENTS (Rfq Side):
        // Client Buy (Wir verkaufen): Wir wollen hoch verkaufen, Client will niedrig kaufen.
        // -> Advantage = Mid - Quote (Positiv wenn Quote < Mid)
        // Client Sell (Wir kaufen): Wir wollen niedrig kaufen, Client will hoch verkaufen.
        // -> Advantage = Quote - Mid (Positiv wenn Quote > Mid)
        let price_delta = match side {
            Side::Buy => mid_price.0 - my_quote.0,
            Side::Sell => my_quote.0 - mid_price.0,
        };

        // 2. Convert to Basis Points (BPS)
        // Advantage in BPS relative to Mid Price.
        // 1 bp = 0.0001. Wir nutzen f64 arithmetic.
        let advantage_bps = (price_delta / mid_price.0) * 10_000.0;

        // 3. Determine Alpha (Steepness/Sensitivity)
        // Tier sensitivity * Global Scaler
        let alpha = tier.intrinsic_sensitivity() * params.global_sensitivity_scaler;

        // 4. Compute Logistic Probability
        // x = alpha * (advantage_bps + bias_bps)
        // Hinweis: bias_bps ist meist negativ (Spread den der Kunde zahlt).
        // Wenn bias = -2.0, muss der Advantage > 2.0 sein, damit x positiv wird (P > 0.5).
        let x = alpha * (advantage_bps + params.bias_bps);

        // Sigmoid Function: 1 / (1 + e^-x)
        let probability = 1.0 / (1.0 + (-x).exp());

        // 5. Roll the dice
        if rng.random_bool(probability) {
            CustomerDecision::Accept
        } else {
            // Explizites Reject: Der Kunde hat geantwortet, aber "Nein" gesagt.
            // TODO: Hier könnte man Logik für Counter-Offers einbauen
            // z.B. wenn probability zwar < Zufall, aber > 0.3 ist -> Counter.
            CustomerDecision::Reject
        }
    }
}

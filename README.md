# Chapaty

[![Discord](https://img.shields.io/discord/1495690333911257108.svg?label=Discord&logo=discord&color=7289da&logoColor=white)][discord]
[![Crates.io](https://img.shields.io/crates/v/chapaty.svg)](https://crates.io/crates/chapaty)
[![Docs.rs](https://img.shields.io/docsrs/chapaty)](https://docs.rs/chapaty)
[![CI (Main)](https://github.com/LenWilliamson/chapaty/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/LenWilliamson/chapaty/actions/workflows/ci.yml)
[![CI (Develop)](https://github.com/LenWilliamson/chapaty/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/LenWilliamson/chapaty/actions/workflows/ci.yml)

**Chapaty** is a Rust engine for **building and evaluating quantitative trading agents**. Designed with a familiar [**Gym-style API**][gymnasiumLink], Chapaty brings the rigor of standardized simulation interfaces to **event-driven financial backtesting**.

## Getting Started

> **Fast Track:** Use the [**Chapaty Starter Template**][chapatyTemplateLink] to instantly bootstrap a new project. It includes pre-configured AI prompts for backtesting with a LLM of your choice and built-in dashboard setups with [Quantstats][quantstatsLink]. For a library of ready-to-run strategies, including the top TradingView setups backtested across million-agent grids, see [**chapaty-zoo**][chapatyZooLink].

Chapaty supports two primary workflows: **Parallel Backtesting** for evaluating agent grids, and the **Canonical Gym Loop** for step-by-step control over the environment.

### 1. Parallel Backtesting

For grid searches, Chapaty leverages `rayon` to evaluate agents in parallel, automatically tracking the top performers.

**Run this example:** [`examples/quickstart.rs`](examples/quickstart.rs)

```bash
cargo run --release --example quickstart
```

Under the hood, the parallel path builds the environment, generates a grid of agents, and evaluates them in one call:

```rust
use chapaty::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize the environment (configure data sources, date ranges, filters)
    // See the full example file for the 'environment()' helper implementation.
    let mut env = environment().await?;

    // 2. Create the Agent Grid
    // Creates a vector of 1M distinct parameter combinations. `NoOpAgent` is a
    // placeholder. Swap in your own strategy (see chapaty-zoo for examples).
    let num_agents = 1_000_000;
    let agents = (0..num_agents)
        .map(|uid| (uid, NoOpAgent::default()))
        .collect::<Vec<_>>();

    // 3. Execute Parallel Evaluation
    // Chapaty manages the batching and threading, retaining the Top-100 agents.
    let leaderboard = env.evaluate_agents(agents, 100)?;

    // 4. Export the Leaderboard
    // Results are saved as a structured CSV dataset.
    leaderboard.to_file_sync(&FileConfig::default())?;

    Ok(())
}
```

### 2. The Canonical Gym Loop (Fine-Grained Control)

For custom integrations or those who prefer full control over the observation-action transition loop, Chapaty implements a standard API inspired by OpenAI Gym.

```rust
use chapaty::prelude::*;

#[tokio::main]
async fn main() -> ChapatyResult<()> {
    // Initialize the environment
    let mut env = chapaty::make(EnvPreset::BinanceBtcUsdt1d).await?;

    // Reset the environment to generate the first observation
    let (mut obs, mut reward, mut outcome) = env.reset()?;

    while !outcome.is_done() {
        // This is where you would insert your custom policy or agent logic
        let actions = obs.action_space().sample()?;

        // Step (transition) through the environment with the actions,
        // receiving the next observation, reward, and termination status.
        (obs, reward, outcome) = env.step(actions)?;

        // If the episode has ended, reset to start a new episode
        if outcome.is_terminal() {
            // Optionally use the final observation and outcome to bootstrap reward logic
            drop(obs);
            (obs, reward, outcome) = env.reset()?;
        }
    }

    // Explicitly drop 'obs' to release the borrow on 'env'.
    // We cannot call 'env.journal()' while 'obs' is still active.
    drop(obs);

    // Extract the trading journal (ledger) for post-simulation analysis.
    let journal = env.journal()?;

    // Save the journal to a directory (the filename is handled internally).
    journal.to_file_sync(&FileConfig::default())?;

    Ok(())
}
```

> **Note:** Environments are **async** because they stream large financial datasets directly from cloud storage (e.g. GCS, BigQuery, HuggingFace).

The [`examples/quickstart.rs`](examples/quickstart.rs) file demonstrates both workflows end to end for a single-agent baseline, a parallel grid, report export, and logging setup. For real, ready-to-run strategies, see [**chapaty-zoo**][chapatyZooLink].

## Related Projects

| Project                                   | Description                                          |
| ----------------------------------------- | ---------------------------------------------------- |
| [Gymnasium][gymnasiumLink]                | Standard API for Reinforcement Learning environments |
| [DeepMind Control Suite][deepmindLink]    | Physics-based simulation environments                |
| [Burn](https://github.com/tracel-ai/burn) | Deep learning framework in Rust                      |

## Community

If you are excited about the project, don't hesitate to join our [Discord][discord]! It is the perfect place to ask questions, file data requests, discuss new features, and share what you have built with the community.

## Contributing

Contributions are welcome! Before submitting a pull request, please make sure to run the pre-build script to verify your changes:

```bash
./bin/pre-push.sh
```

## Disclaimer

**Trading and investing involve substantial risk. You may lose some or all of your capital.**

Chapaty is an **open-source software project** provided for **research and educational purposes only**. It **does not constitute financial, investment, legal, or trading advice**.

This software is provided **“AS IS”**, without warranties or conditions of any kind, express or implied, as stated in the **Apache License, Version 2.0**. The software may contain bugs, errors, or inaccuracies.

**In no event shall the authors or contributors be liable for any direct or indirect losses, damages, or consequences**, including but not limited to financial losses, arising from the use of this software.

By using Chapaty, you acknowledge that **you are solely responsible for any trading decisions, strategies, or outcomes**.

[discord]: https://discord.gg/MmMAB6NCuK
[gymnasiumLink]: https://github.com/Farama-Foundation/Gymnasium
[deepmindLink]: https://github.com/deepmind/dm_control
[quantstatsLink]: https://github.com/ranaroussi/quantstats
[chapatyTemplateLink]: https://github.com/LenWilliamson/chapaty-template
[chapatyZooLink]: https://github.com/LenWilliamson/chapaty-zoo

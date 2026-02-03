# Chapaty

[![Crates.io](https://img.shields.io/crates/v/chapaty.svg)](https://crates.io/crates/chapaty)
[![Docs.rs](https://img.shields.io/docsrs/chapaty)](https://docs.rs/chapaty)

**Chapaty** is a high-performance, async-first Rust library for **training and evaluating reinforcement learning agents in financial environments**. Inspired by [OpenAI Gymnasium][gymnasiumLink], Chapaty brings the rigor of standardized simulation interfaces to **real-world financial markets**.

## Getting Started

Chapaty supports two primary workflows: **High-Performance Backtesting** for evaluating strategy grids, and **Standard Reinforcement Learning** for training agents step-by-step.

### 1. High-Performance Backtesting (1M+ Agents)

For massive grid searches, Chapaty leverages `rayon` to evaluate millions of agents in parallel, automatically tracking the top performers without memory overhead.

**Run this example:** [`examples/news_breakout_grid.rs`](examples/news_breakout_grid.rs)

```rust
use std::path::Path;

use chapaty::prelude::*;
use polars::prelude::CsvWriterOptions;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize the environment (configure data sources, date ranges, filters)
    // See the full example file for the 'environment()' helper implementation.
    let mut env = environment().await?;

    // 2. Generate the Agent Grid
    // Creates a lazy parallel iterator of 1,000,000+ distinct parameter combinations.
    // This allows efficient streaming without loading all agents into RAM.
    let (count, agents) = news_breakout_grid();

    println!("Evaluating {count} agents...");

    // 3. Execute Parallel Evaluation
    // Chapaty manages the batching and threading, retaining the Top-100 agents
    let leaderboard = env.evaluate_agents(agents, 100, count as u64)?;

    // 4. Export the Leaderboard
    // Results are saved as a structured CSV dataset.
    leaderboard.to_csv(Path::new("examples/reports/news_breakout"), &CsvWriterOptions::default())?;

    Ok(())
}
```

### 2. Custom Reinforcement Learning (Gym Interface)

For researchers needing fine-grained control over the observation-action loop, Chapaty implements a standard API similar to OpenAI Gym.

```rust
use std::path::Path;

use chapaty::prelude::*;

#[tokio::main]
async fn main() -> ChapatyResult<()> {
    // Initialize the environment
    let mut env = chapaty::make(EnvPreset::BtcUsdtEod).await?;

    // Reset the environment to generate the first observation
    let (mut obs, mut reward, mut outcome) = env.reset()?;

    while !outcome.is_done() {
        // this is where you would insert your policy
        let actions = obs.action_space().sample()?;

        // step (transition) through the environment with the actions
        // receiving the next observation, reward and if the episode has terminated
        (obs, reward, outcome) = env.step(actions)?;

        // If the episode has ended then we can reset to start a new episode
        if outcome.is_terminal() {
            // optionally use the final observation and outcome to bootstrap reward
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
    journal.to_csv(Path::new("chapaty/reports"), None, None)?;

    Ok(())
}
```

> **Note:** Environments are **async** because they stream large financial datasets from cloud storage (e.g. GCS, BigQuery, S3, etc.).

For practical, _ready-to-run agents_, check out the examples to get started quickly.

## Related Projects

| Project                                   | Description                                  |
| ----------------------------------------- | -------------------------------------------- |
| [Gymnasium][gymnasiumLink]                | RL API standard for Python environments      |
| [DeepMind Control Suite][deepmindLink]    | Physics-based simulation and RL environments |
| [Burn](https://github.com/tracel-ai/burn) | Deep learning framework in Rust              |

## Chapaty Platform

To access hosted market data, simply log in at [chapaty.com][chapatyLink] to obtain an API key. End-of-day OHLCV data is free to use. Prefer your own data? You can **bring your own market data** and start using Chapaty at no cost.

[chapatyLink]: https://www.chapaty.com
[gymnasiumLink]: https://github.com/Farama-Foundation/Gymnasium
[deepmindLink]: https://github.com/deepmind/dm_control

## Disclaimer

**Trading and investing involve substantial risk. You may lose some or all of your capital.**

Chapaty is an **open-source software project** provided for **research and educational purposes only**. It **does not constitute financial, investment, legal, or trading advice**.

This software is provided **“AS IS”**, without warranties or conditions of any kind, express or implied, as stated in the **Apache License, Version 2.0**. The software may contain bugs, errors, or inaccuracies.

**In no event shall the authors or contributors be liable for any direct or indirect losses, damages, or consequences**, including but not limited to financial losses, arising from the use of this software.

By using Chapaty, you acknowledge that **you are solely responsible for any trading decisions, strategies, or outcomes**.

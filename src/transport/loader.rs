use std::{collections::HashMap, time::Duration};

use crate::{
    error::{ChapatyError, ChapatyResult, IoError, TransportError},
    transport::{fetcher::Fetchable, source::ChapatyClient},
};
use polars::prelude::{LazyFrame, SchemaRef};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[derive(Debug, Clone, Copy)]
struct Year(pub u16);
impl From<Year> for i32 {
    fn from(value: Year) -> Self {
        value.0 as i32
    }
}

#[tracing::instrument(skip_all)]
pub async fn load_batch<T: Fetchable>(
    client: &mut ChapatyClient,
    specs: Vec<T>,
    years: Vec<u16>,
) -> ChapatyResult<HashMap<T::Id, (SchemaRef, LazyFrame)>> {
    let cx = CancellationToken::new();
    let num_workers = std::thread::available_parallelism()
        .map_err(|e| {
            TransportError::Stream(format!("Failed to fetch cpu cores to build pipeline: {e}"))
        })?
        .get();
    let num_fetcher = num_workers.div_ceil(2).min(4);

    // === 1. Generator Stage (1-to-N) Fan-Out  ===
    let (job_tx, job_rx) = async_channel::bounded::<(T::Id, T, Year)>(2 * num_workers);
    let args = generator::Args {
        cx: cx.clone(),
        tx: job_tx.clone(),
        specs,
        years,
    };

    let mut generator = JoinSet::new();
    generator.spawn(generator::run(args));
    drop(job_tx);

    // === 2. Fetcher Stage (N-to-N) Fan-Out ===
    let (resp_tx, resp_rx) = async_channel::bounded::<(T::Id, T::Response)>(2 * num_workers);
    let mut fetcher = JoinSet::new();

    for _ in 0..num_fetcher {
        let args = fetcher::Args {
            cx: cx.clone(),
            rx: job_rx.clone(),
            tx: resp_tx.clone(),
            client: client.clone(),
        };
        fetcher.spawn(fetcher::run(args));
    }
    drop(job_rx);
    drop(resp_tx);

    // === 3. Processor Stage (N-to-1) Fan-In ===
    let (lf_tx, lf_rx) = mpsc::channel(2 * num_workers);
    let mut worker = JoinSet::new();

    for _ in 0..num_workers {
        let args = processor::Args::<T> {
            cx: cx.clone(),
            rx: resp_rx.clone(),
            tx: lf_tx.clone(),
        };
        worker.spawn(processor::run(args));
    }
    drop(resp_rx);
    drop(lf_tx);

    // === 4. Collector Stage (Sink) ===
    let args = collector::Args::<T> {
        cx: cx.clone(),
        rx: lf_rx,
    };
    let collector = tokio::spawn(collector::run(args));

    // === 5. LIFO Cleanup & Verification ===
    let result = collector
        .await
        .map_err(|e| IoError::ReadFailed(e.to_string()))?;
    worker.try_drain(cx.clone()).await?;
    fetcher.try_drain(cx.clone()).await?;
    generator.try_drain(cx.clone()).await?;

    result
}

// ================================================================================================
// Generator
// ================================================================================================

mod generator {
    use crate::{
        error::ChapatyResult,
        transport::loader::{Fetchable, Year},
    };
    use std::collections::HashMap;
    use tokio_util::sync::CancellationToken;

    pub struct Args<T: Fetchable> {
        pub cx: CancellationToken,
        pub tx: async_channel::Sender<(T::Id, T, Year)>,
        pub specs: Vec<T>,
        pub years: Vec<u16>,
    }

    #[tracing::instrument(skip_all)]
    pub async fn run<T: Fetchable>(args: Args<T>) -> ChapatyResult<()> {
        let Args {
            cx,
            specs,
            years,
            tx,
        } = args;

        let mut unique_jobs = HashMap::new();
        for spec in specs {
            let id = match spec.to_id() {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(?e, "Failed to get id from config");
                    cx.cancel();
                    return Err(e);
                }
            };
            unique_jobs.entry(id).or_insert(spec);
        }

        for (id, job) in unique_jobs {
            for year in &years {
                tokio::select! {
                _ = cx.cancelled() => {
                    tracing::info!("Generator cancelled; exiting early.");
                    return Ok(());
                },
                res = tx.send((id, job.clone(), Year(*year))) => {
                    if res.is_err() {
                        tracing::info!("Work channel closed; generator exiting.");
                        return Ok(());
                    }
                }
                }
            }
        }
        tracing::info!("Generator finished.");
        Ok(())
    }
}

// ================================================================================================
// Fetcher
// ================================================================================================

mod fetcher {
    use crate::{
        error::{ChapatyResult, TransportError},
        transport::{
            loader::{Fetchable, TryDrain, Year},
            source::ChapatyClient,
        },
    };
    use tokio_util::sync::CancellationToken;

    pub struct Args<T: Fetchable> {
        pub cx: CancellationToken,
        pub rx: async_channel::Receiver<(T::Id, T, Year)>,
        pub tx: async_channel::Sender<(T::Id, T::Response)>,
        pub client: ChapatyClient,
    }

    #[tracing::instrument(skip_all)]
    pub async fn run<T: Fetchable>(args: Args<T>) -> ChapatyResult<()> {
        let Args { cx, rx, tx, client } = args;
        let mut tasks = tokio::task::JoinSet::new();

        let mut fatal_error = None;

        loop {
            tokio::select! {
            // A. External Cancellation
            _ = cx.cancelled() => {
                tracing::info!("Fetcher received cancellation signal.");
                break;
            }

            // B. Incoming Work
            work = rx.recv() => {
                match work {
                    Ok((id, job, year)) => {
                        let cx = cx.clone();
                        let tx = tx.clone();
                        let client = client.clone();
                        tasks.spawn(async move {
                            stream(cx, tx, id, job, year, client).await
                        });
                    }
                    Err(_) => {
                        tracing::info!("Job queue closed (End of Input).");
                        break;
                    }
                }
            }

            // C. Task Supervision
            Some(result) = tasks.join_next() => {
                match result {
                    Ok(Ok(())) => tracing::debug!("Stream task completed."),

                    // Logic Error
                    Ok(Err(e)) => {
                        tracing::error!(?e, "Stream task failed.");
                        fatal_error = Some(e);
                        cx.cancel();
                        break;
                    }

                    // Panic / Join Error
                    Err(e) => {
                        tracing::error!(?e, "Stream task panicked.");
                        fatal_error = Some(TransportError::Stream(e.to_string()).into());
                        cx.cancel();
                        break;
                    }
                }
            }
            }
        }

        let drain_result = tasks.try_drain(cx).await;
        match (fatal_error, drain_result) {
            // Priority 1: The error that broke the loop (The Root Cause)
            (Some(root), other) => {
                // We log the secondary error if it occurred, but don't return it.
                if let Err(secondary) = other {
                    tracing::warn!(?secondary, "Secondary failure during shutdown (ignored).");
                }
                Err(root)
            }

            // Priority 2: An error that happened during graceful shutdown
            (None, Err(shutdown_err)) => Err(shutdown_err),

            // Priority 3: Clean Success
            (None, Ok(())) => {
                tracing::info!("Fetcher finished cleanly.");
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip_all, fields(spec_id = ?id, year = ?year))]
    async fn stream<T: Fetchable>(
        cx: CancellationToken,
        tx: async_channel::Sender<(T::Id, T::Response)>,
        id: T::Id,
        job: T,
        year: Year,
        mut client: ChapatyClient,
    ) -> ChapatyResult<()> {
        let req = job.make_request(year.into())?;
        let mut stream = T::fetch(&mut client, req)
            .await
            .map_err(|e| TransportError::Stream(e.to_string()))?
            .into_inner();

        loop {
            tokio::select! {
            _ = cx.cancelled() => {
                tracing::debug!("Stream task cancelled cleanly.");
                return Ok(());
            }

            message = stream.message() => {
                match message.map_err(|e| TransportError::Stream(e.to_string()))? {
                    Some(batch) => {
                        if tx.send((id, batch)).await.is_err() {
                            return Err(TransportError::Stream("Downstream died (Receiver dropped). Sending on closed channel.".to_string()).into());
                        }
                    }
                    None => return Ok(()),
                }
            }
            }
        }
    }
}

// ================================================================================================
// Processor
// ================================================================================================

mod processor {
    use polars::prelude::LazyFrame;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info};

    use crate::{
        error::{ChapatyResult, IoError},
        transport::{codec::ProtoBatch, loader::Fetchable},
    };

    pub struct Args<T>
    where
        T: Fetchable,
    {
        pub cx: CancellationToken,
        pub rx: async_channel::Receiver<(T::Id, T::Response)>,
        pub tx: Sender<(T::Id, LazyFrame)>,
    }

    #[tracing::instrument(skip_all)]
    pub async fn run<T>(args: Args<T>) -> ChapatyResult<()>
    where
        T: Fetchable,
    {
        let Args { cx, rx, tx } = args;

        loop {
            tokio::select! {
            _ = cx.cancelled() => {
                info!("Processor worker cancelled");
                break Ok(());
            }
            res = rx.recv() => {
                let (fetcher, batch) = match res {
                    Ok((f, b)) => (f, b),
                    Err(_) => {
                        // This happens if the sender side is dropped/closed, meaning
                        // the generator has shut down. The processor should also stop.
                        tracing::info!("No more jobs; processor exiting.");
                        break Ok(());
                    }
                };

                // Transform the collected events into a LazyFrame
                let (send, recv) = tokio::sync::oneshot::channel();
                rayon::spawn(move || {
                    let result = batch.into_lazyframe();
                    let _ = send.send(result);
                });

                let lf_res = match recv.await {
                    Ok(result) => result,
                    Err(_) => {
                        error!(?fetcher, "Rayon thread panicked while converting batch to LazyFrame");
                        return Err(IoError::ReadFailed("Rayon worker panicked during batch conversion".to_string()).into());
                    }
                };

                match lf_res {
                    Ok(lf) => {
                        let send_res = tx.send((fetcher, lf)).await;
                        if send_res.is_err() {
                            tracing::info!("Channel closed; exiting processor.");
                            break Ok(());
                        }
                    },
                    Err(e) => {
                        error!(?fetcher, ?e, "Failed to convert batch into LazyFrame");
                        return Err(e);
                    }
                }
            }
            }
        }
    }
}

// ================================================================================================
// Collector
// ================================================================================================

mod collector {

    use std::collections::HashMap;

    use polars::prelude::{LazyFrame, SchemaRef, UnionArgs};
    use tokio::sync::mpsc::Receiver;
    use tokio_util::sync::CancellationToken;
    use tracing::info;

    use crate::{
        error::{ChapatyResult, DataError, IoError},
        transport::loader::Fetchable,
    };

    pub struct Args<T: Fetchable> {
        pub cx: CancellationToken,
        pub rx: Receiver<(T::Id, LazyFrame)>,
    }

    #[tracing::instrument(skip_all)]
    pub async fn run<T: Fetchable>(
        args: Args<T>,
    ) -> ChapatyResult<HashMap<T::Id, (SchemaRef, LazyFrame)>> {
        let Args { cx, mut rx } = args;
        let mut staging: HashMap<T::Id, Vec<LazyFrame>> = HashMap::new();

        loop {
            tokio::select! {
            _ = cx.cancelled() => {
                tracing::info!("Collector worker cancelled; exiting early");
                return Err(IoError::ReadFailed("Collector worker cancelled".to_string()).into());
            }
            maybe_res = rx.recv() => {
                match maybe_res {
                    Some((id, lf)) => staging.entry(id).or_default().push(lf),
                    None => {
                        info!("All results received. Processing final LazyFrames");
                        let mut results = HashMap::with_capacity(staging.len());

                        for (id, frames) in staging {
                            if frames.is_empty() { continue; }

                            let concatenated_lf = polars::prelude::concat(
                                frames,
                                UnionArgs {
                                    parallel: true,
                                    rechunk: true,
                                    ..Default::default()
                                }
                            ).map_err(|e| DataError::DataFrame(e.to_string()))?;

                            results.insert(id, (T::schema_ref(), concatenated_lf));
                        }
                        return Ok(results);
                    }
                }
            }
            }
        }
    }
}

// ================================================================================================
// Drainer
// ================================================================================================

trait Drainer {
    async fn drain(&mut self);
}

impl<T: 'static> Drainer for JoinSet<T> {
    async fn drain(&mut self) {
        while self.join_next().await.is_some() {}
    }
}

trait DrainSafely {
    async fn drain_safely(&mut self, secs: u64);
}

impl<T: 'static> DrainSafely for JoinSet<T> {
    async fn drain_safely(&mut self, secs: u64) {
        tokio::select! {
        _ = self.drain() => {
            tracing::debug!("All workers drained successfully.");
        },
        _ = tokio::time::sleep(Duration::from_secs(secs)) => {
            tracing::warn!("Workers stuck during shutdown (timeout). Dropping handle.");
        }
        }
    }
}

trait TryDrain {
    async fn try_drain(self, cancel: CancellationToken) -> ChapatyResult<()>;
}

impl TryDrain for JoinSet<ChapatyResult<()>> {
    async fn try_drain(mut self, cancel: CancellationToken) -> ChapatyResult<()> {
        while let Some(result) = self.join_next().await {
            match result {
                // Happy Path: Task succeeded
                Ok(Ok(())) => continue,

                // Case A: Application Error (Logic failed)
                Ok(Err(e)) => {
                    error!(?e, "Worker failed, triggering graceful cancellation.");
                    cancel.cancel();
                    self.drain_safely(5).await;
                    return Err(e);
                }

                // Case B: Join Error (Panic or Cancellation)
                Err(e) => {
                    error!(?e, "Worker task panicked/failed to join, cancelling.");
                    cancel.cancel();
                    self.drain_safely(5).await;

                    return Err(ChapatyError::Transport(TransportError::Stream(
                        e.to_string(),
                    )));
                }
            }
        }

        Ok(())
    }
}

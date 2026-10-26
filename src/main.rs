use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::signal;

mod errors;
mod models;
mod parser;
mod storage;

use errors::PipelineError;
use models::{LogMessage, Raw};
use storage::LogStorage;

const BATCH_SIZE: usize = 1000;
const FLUSH_INTERVAL_MS: u64 = 100;
const CHANNEL_CAPACITY: usize = 10_000;
const SERVER_ADDRESS: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() -> Result<(), PipelineError> {
    tracing_subscriber::fmt::init();
    tracing::info!("Initializing Concurrent Log Aggregator & Metrics Pipeline...");

    // let storage_backend: Arc<dyn LogStorage + Send + Sync> = Arc::new(InMemoryBackend::new());
    let storage_backend: Arc<dyn LogStorage + Send + Sync> = Arc::new(
        storage::WalDiskBackend::new("pipeline.wal").await?
    );

    let (tx, rx) = mpsc::channel::<LogMessage<Raw>>(CHANNEL_CAPACITY);

    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let processor_backend = Arc::clone(&storage_backend);
    let processor_token = shutdown_token.clone();
    let processor_handle = tokio::spawn(async move {
        tracing::info!("Processing engine worker started.");
        if let Err(e) = run_processor(rx, processor_backend, processor_token).await {
            tracing::error!("Processor engine crashed unexpectedly: {:?}", e);
        }
    });

    let listener = TcpListener::bind(SERVER_ADDRESS).await?;
    tracing::info!("Ingestion Layer listening on TCP://{}", SERVER_ADDRESS);

    let ingestion_token = shutdown_token.clone();
    tokio::select! {
        res = run_ingestion_loop(listener, tx, ingestion_token) => {
            if let Err(e) = res {
                tracing::error!("Ingestion loop encountered an error: {:?}", e);
            }
        }
        _ = signal::ctrl_c() => {
            tracing::info!("SIGINT/Ctrl+C detected! Initiating graceful shutdown sequence...");
            shutdown_token.cancel();
        }
    }

    let _ = processor_handle.await;
    tracing::info!("Pipeline shutdown safely completed cleanly. Goodbye.");
    Ok(())
}

async fn run_ingestion_loop(
    listener: TcpListener,
    tx: mpsc::Sender<LogMessage<Raw>>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), PipelineError> {
    loop {
        tokio::select! {
            biased; // Prioritize checking shutdown over processing incoming connections
            _ = shutdown_token.cancelled() => {
                break;
            }
            accept_res = listener.accept() => {
                let (stream, peer_addr) = accept_res?;
                tracing::debug!("New ingestion stream connection from: {}", peer_addr);

                let thread_tx = tx.clone();
                let client_token = shutdown_token.clone();

                tokio::spawn(async move {
                    if let Err(e) = parser::handle_client_stream(
                        stream, thread_tx, client_token
                    ).await {
                        tracing::error!("Error handling ingestion client {}: {:?}", peer_addr, e);
                    }
                });
            }
        }
    }
    Ok(())
}

async fn run_processor(
    mut rx: mpsc::Receiver<LogMessage<Raw>>,
    storage: Arc<dyn LogStorage + Send + Sync>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), PipelineError> {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                match maybe_msg {
                    Some(raw_msg) => {
                        match parser::parse_raw_message(&raw_msg) {
                            Ok(parsed_msg) => {
                                batch.push(parsed_msg);
                                // Size-based threshold trigger
                                if batch.len() >= BATCH_SIZE {
                                    storage.append_batch(&mut batch).await?;
                                }
                            }
                            Err(e) => tracing::warn!("Dropping malformed log packet: {:?}", e),
                        }
                    }
                    None => {
                        // Channel dropped/closed upstream
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    storage.append_batch(&mut batch).await?;
                }

                if shutdown_token.is_cancelled() && rx.is_empty() {
                    break;
                }
            }
        }
    }

    if !batch.is_empty() {
        tracing::info!("Flushing remaining {} logs in buffer before thread exit...", batch.len());
        storage.append_batch(&mut batch).await?;
    }

    Ok(())
}


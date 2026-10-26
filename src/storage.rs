use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::errors::PipelineError;
use crate::models::{LogMessage, Parsed};

const MAX_MEMORY_CAPACITY: usize = 50_000;

#[async_trait]
pub trait LogStorage: Send + Sync {
    async fn append_batch(&self, batch: &mut Vec<LogMessage<Parsed>>) -> Result<(), PipelineError>;
    async fn fetch_snapshot(&self) -> Result<Vec<Vec<u8>>, PipelineError>;
    async fn len(&self) -> usize;
}

pub struct InMemoryBackend {
    buffer: RwLock<VecDeque<Vec<u8>>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            buffer: RwLock::new(VecDeque::with_capacity(MAX_MEMORY_CAPACITY)),
        }
    }
}

#[async_trait]
impl LogStorage for InMemoryBackend {
    async fn append_batch(&self, batch: &mut Vec<LogMessage<Parsed>>) -> Result<(), PipelineError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut write_guard = self.buffer.write().await;
        for message in batch.drain(..) {
            if write_guard.len() >= MAX_MEMORY_CAPACITY {
                write_guard.pop_front();
            }
            write_guard.push_back(message.payload);
        }

        tracing::debug!("Successfully flushed log batch to In-Memory Storage Engine.");
        Ok(())
    }

    async fn fetch_snapshot(&self) -> Result<Vec<Vec<u8>>, PipelineError> {
        let read_guard = self.buffer.read().await;
        Ok(read_guard.iter().cloned().collect())
    }

    async fn len(&self) -> usize {
        self.buffer.read().await.len()
    }
}

pub struct WalDiskBackend {
    file_handle: RwLock<File>,
    _file_path: PathBuf,
}

impl WalDiskBackend {
    pub async fn new<P: Into<PathBuf>>(path: P) -> Result<Self, PipelineError> {
        let file_path = path.into();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&file_path)
            .await?;

        Ok(Self {
            file_handle: RwLock::new(file),
            _file_path: file_path,
        })
    }
}

#[async_trait]
impl LogStorage for WalDiskBackend {
    async fn append_batch(&self, batch: &mut Vec<LogMessage<Parsed>>) -> Result<(), PipelineError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut file_guard = self.file_handle.write().await;

        let mut buffer_payload = Vec::new();
        for message in batch.drain(..) {
            buffer_payload.extend_from_slice(&message.payload);
            buffer_payload.push(b'\n'); // Standard newline line delimiter spacing on disk
        }

        file_guard.write_all(&buffer_payload).await?;
        file_guard.sync_data().await?;

        tracing::debug!("Successfully committed log batch to persistent WAL Storage Engine.");
        Ok(())
    }

    async fn fetch_snapshot(&self) -> Result<Vec<Vec<u8>>, PipelineError> {
        Ok(vec![])
    }

    async fn len(&self) -> usize {
        0
    }
}

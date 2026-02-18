use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use scoped_futures::ScopedBoxFuture;
use tokio::sync::Mutex;

use sui_indexer_alt_framework::store::{
    CommitterWatermark, Connection, PrunerWatermark, ReaderWatermark, Store, TransactionalStore,
};

#[derive(Clone, Copy, Default)]
struct RuntimeWatermark {
    epoch_hi_inclusive: u64,
    checkpoint_hi_inclusive: u64,
    tx_hi: u64,
    timestamp_ms_hi_inclusive: u64,
    reader_lo: u64,
    pruner_timestamp_ms: u64,
    pruner_hi: u64,
}

#[derive(Clone, Default)]
pub struct RuntimeStore {
    watermarks: Arc<Mutex<HashMap<String, RuntimeWatermark>>>,
}

pub struct RuntimeConnection<'c> {
    store: &'c RuntimeStore,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
impl Connection for RuntimeConnection<'_> {
    async fn init_watermark(
        &mut self,
        pipeline_task: &str,
        default_next_checkpoint: u64,
    ) -> anyhow::Result<Option<u64>> {
        let mut map = self.store.watermarks.lock().await;

        if let Some(existing) = map.get(pipeline_task) {
            return Ok(Some(existing.checkpoint_hi_inclusive));
        }

        let Some(checkpoint_hi_inclusive) = default_next_checkpoint.checked_sub(1) else {
            return Ok(None);
        };

        map.insert(
            pipeline_task.to_string(),
            RuntimeWatermark {
                checkpoint_hi_inclusive,
                reader_lo: default_next_checkpoint,
                pruner_hi: default_next_checkpoint,
                ..Default::default()
            },
        );

        Ok(Some(checkpoint_hi_inclusive))
    }

    async fn committer_watermark(
        &mut self,
        pipeline_task: &str,
    ) -> anyhow::Result<Option<CommitterWatermark>> {
        let map = self.store.watermarks.lock().await;
        Ok(map.get(pipeline_task).map(|w| CommitterWatermark {
            epoch_hi_inclusive: w.epoch_hi_inclusive,
            checkpoint_hi_inclusive: w.checkpoint_hi_inclusive,
            tx_hi: w.tx_hi,
            timestamp_ms_hi_inclusive: w.timestamp_ms_hi_inclusive,
        }))
    }

    async fn reader_watermark(
        &mut self,
        pipeline: &'static str,
    ) -> anyhow::Result<Option<ReaderWatermark>> {
        let map = self.store.watermarks.lock().await;
        Ok(map.get(pipeline).map(|w| ReaderWatermark {
            checkpoint_hi_inclusive: w.checkpoint_hi_inclusive,
            reader_lo: w.reader_lo,
        }))
    }

    async fn pruner_watermark(
        &mut self,
        pipeline: &'static str,
        delay: Duration,
    ) -> anyhow::Result<Option<PrunerWatermark>> {
        let map = self.store.watermarks.lock().await;
        let now = now_ms() as i64;
        Ok(map.get(pipeline).map(|w| PrunerWatermark {
            wait_for_ms: (w.pruner_timestamp_ms as i64 + delay.as_millis() as i64) - now,
            reader_lo: w.reader_lo,
            pruner_hi: w.pruner_hi,
        }))
    }

    async fn set_committer_watermark(
        &mut self,
        pipeline_task: &str,
        watermark: CommitterWatermark,
    ) -> anyhow::Result<bool> {
        let mut map = self.store.watermarks.lock().await;
        let entry = map.entry(pipeline_task.to_string()).or_default();
        if watermark.checkpoint_hi_inclusive < entry.checkpoint_hi_inclusive {
            return Ok(false);
        }

        entry.epoch_hi_inclusive = watermark.epoch_hi_inclusive;
        entry.checkpoint_hi_inclusive = watermark.checkpoint_hi_inclusive;
        entry.tx_hi = watermark.tx_hi;
        entry.timestamp_ms_hi_inclusive = watermark.timestamp_ms_hi_inclusive;
        Ok(true)
    }

    async fn set_reader_watermark(
        &mut self,
        pipeline: &'static str,
        reader_lo: u64,
    ) -> anyhow::Result<bool> {
        let mut map = self.store.watermarks.lock().await;
        let entry = map.entry(pipeline.to_string()).or_default();
        if reader_lo <= entry.reader_lo {
            return Ok(false);
        }
        entry.reader_lo = reader_lo;
        entry.pruner_timestamp_ms = now_ms();
        Ok(true)
    }

    async fn set_pruner_watermark(
        &mut self,
        pipeline: &'static str,
        pruner_hi: u64,
    ) -> anyhow::Result<bool> {
        let mut map = self.store.watermarks.lock().await;
        let entry = map.entry(pipeline.to_string()).or_default();
        if pruner_hi <= entry.pruner_hi {
            return Ok(false);
        }
        entry.pruner_hi = pruner_hi;
        Ok(true)
    }
}

#[async_trait]
impl Store for RuntimeStore {
    type Connection<'c>
        = RuntimeConnection<'c>
    where
        Self: 'c;

    async fn connect<'c>(&'c self) -> anyhow::Result<Self::Connection<'c>> {
        Ok(RuntimeConnection { store: self })
    }
}

#[async_trait]
impl TransactionalStore for RuntimeStore {
    async fn transaction<'a, R, F>(&self, f: F) -> anyhow::Result<R>
    where
        R: Send + 'a,
        F: Send + 'a,
        F: for<'r> FnOnce(
            &'r mut Self::Connection<'_>,
        ) -> ScopedBoxFuture<'a, 'r, anyhow::Result<R>>,
    {
        let mut conn = self.connect().await?;
        f(&mut conn).await
    }
}

use std::sync::Arc;

use serde_with::serde_as;
use serde::{ Serialize, Deserialize };
use sui_sdk::{ rpc_types::CheckpointId, SuiClient };
use sui_types::{
    committee::EpochId,
    messages_checkpoint::{ CheckpointSequenceNumber, CheckpointTimestamp },
    sui_serde::BigInt,
};

use crate::error::DeepLookOrderbookError;

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointDigest {
    #[serde_as(as = "BigInt<u64>")]
    pub epoch: EpochId,
    #[serde_as(as = "BigInt<u64>")]
    pub sequence_number: CheckpointSequenceNumber,
    #[serde_as(as = "BigInt<u64>")]
    pub timestamp_ms: CheckpointTimestamp,
}

impl CheckpointDigest {
    pub async fn from_sequence_number(
        sui_client: Arc<SuiClient>,
        n: CheckpointSequenceNumber
    ) -> Result<Self, DeepLookOrderbookError> {
        let checkpoint: sui_sdk::rpc_types::Checkpoint = sui_client
            .read_api()
            .get_checkpoint(CheckpointId::SequenceNumber(n)).await?;

        Ok(CheckpointDigest {
            epoch: checkpoint.epoch,
            sequence_number: checkpoint.sequence_number,
            timestamp_ms: checkpoint.timestamp_ms,
        })
    }

    pub async fn get_latest(sui_client: Arc<SuiClient>) -> Result<Self, DeepLookOrderbookError> {
        let latest_checkpoint = sui_client
            .read_api()
            .get_latest_checkpoint_sequence_number().await?;
        let checkpoint: sui_sdk::rpc_types::Checkpoint = sui_client
            .read_api()
            .get_checkpoint(CheckpointId::SequenceNumber(latest_checkpoint)).await?;

        Ok(CheckpointDigest {
            epoch: checkpoint.epoch,
            sequence_number: checkpoint.sequence_number,
            timestamp_ms: checkpoint.timestamp_ms,
        })
    }

    pub async fn get_sequence_number(
        sui_client: Arc<SuiClient>
    ) -> Result<CheckpointSequenceNumber, DeepLookOrderbookError> {
        let latest_sequence = sui_client.read_api().get_latest_checkpoint_sequence_number().await?;
        Ok(latest_sequence)
    }
}

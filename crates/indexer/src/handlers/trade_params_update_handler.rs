use crate::define_handler;
use crate::models::deepbook::governance::TradeParamsUpdateEvent;
use crate::utils::ms_to_secs;
use deeplook_schema::models::TradeParamsUpdate;

define_handler! {
    name: TradeParamsUpdateHandler,
    processor_name: "trade_params_update",
    event_type: TradeParamsUpdateEvent,
    db_model: TradeParamsUpdate,
    table: trade_params_update,
    tx_context: |tx, checkpoint, env| {
        let deepbook_addresses = env.package_addresses();
        let pool = tx.input_objects(&checkpoint.object_set).find(|o| {
            matches!(o.data.struct_tag(), Some(struct_tag)
                if deepbook_addresses.iter().any(|addr| struct_tag.address == *addr)
                    && struct_tag.module.as_str() == "pool"
                    && struct_tag.name.as_str() == "Pool")
        });
        pool.map(|o| o.id().to_hex_uncompressed())
            .unwrap_or_else(|| "0x0".to_string())
    },
    map_event: |event, meta, pool_id| TradeParamsUpdate {
        event_digest: meta.event_digest(),
        digest: meta.digest(),
        sender: meta.sender(),
        checkpoint: meta.checkpoint(),
        checkpoint_timestamp_ms: meta.checkpoint_timestamp_ms(),
        timestamp: ms_to_secs(meta.checkpoint_timestamp_ms()),
        package: meta.package(),
        pool_id: pool_id.clone(),
        taker_fee: event.taker_fee as i64,
        maker_fee: event.maker_fee as i64,
        stake_required: event.stake_required as i64,
    }
}

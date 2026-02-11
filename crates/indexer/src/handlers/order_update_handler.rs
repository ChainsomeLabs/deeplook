use crate::define_multi_handler;
use crate::models::deepbook::order::{OrderCanceled, OrderModified};
use crate::models::deepbook::order_info::{OrderExpired, OrderPlaced};
use crate::utils::ms_to_secs;
use deeplook_schema::models::{OrderUpdate, OrderUpdateStatus};

define_multi_handler! {
    name: OrderUpdateHandler,
    processor_name: "order_update",
    db_model: OrderUpdate,
    table: order_updates,
    events: [
        {
            event_type: OrderPlaced,
            map_event: |event, meta| OrderUpdate {
                event_digest: meta.event_digest(),
                digest: meta.digest(),
                sender: meta.sender(),
                checkpoint: meta.checkpoint(),
                checkpoint_timestamp_ms: meta.checkpoint_timestamp_ms(),
                timestamp: ms_to_secs(meta.checkpoint_timestamp_ms()),
                package: meta.package(),
                status: OrderUpdateStatus::Placed,
                pool_id: event.pool_id.to_string(),
                order_id: event.order_id.to_string(),
                client_order_id: event.client_order_id as i64,
                price: event.price as i64,
                is_bid: event.is_bid,
                onchain_timestamp: event.timestamp as i64,
                original_quantity: event.placed_quantity as i64,
                quantity: event.placed_quantity as i64,
                filled_quantity: 0,
                trader: event.trader.to_string(),
                balance_manager_id: event.balance_manager_id.to_string(),
            }
        },
        {
            event_type: OrderModified,
            map_event: |event, meta| OrderUpdate {
                event_digest: meta.event_digest(),
                digest: meta.digest(),
                sender: meta.sender(),
                checkpoint: meta.checkpoint(),
                checkpoint_timestamp_ms: meta.checkpoint_timestamp_ms(),
                timestamp: ms_to_secs(meta.checkpoint_timestamp_ms()),
                package: meta.package(),
                status: OrderUpdateStatus::Modified,
                pool_id: event.pool_id.to_string(),
                order_id: event.order_id.to_string(),
                client_order_id: event.client_order_id as i64,
                price: event.price as i64,
                is_bid: event.is_bid,
                onchain_timestamp: event.timestamp as i64,
                original_quantity: event.previous_quantity as i64,
                quantity: event.new_quantity as i64,
                filled_quantity: event.filled_quantity as i64,
                trader: event.trader.to_string(),
                balance_manager_id: event.balance_manager_id.to_string(),
            }
        },
        {
            event_type: OrderCanceled,
            map_event: |event, meta| OrderUpdate {
                event_digest: meta.event_digest(),
                digest: meta.digest(),
                sender: meta.sender(),
                checkpoint: meta.checkpoint(),
                checkpoint_timestamp_ms: meta.checkpoint_timestamp_ms(),
                timestamp: ms_to_secs(meta.checkpoint_timestamp_ms()),
                package: meta.package(),
                status: OrderUpdateStatus::Canceled,
                pool_id: event.pool_id.to_string(),
                order_id: event.order_id.to_string(),
                client_order_id: event.client_order_id as i64,
                price: event.price as i64,
                is_bid: event.is_bid,
                onchain_timestamp: event.timestamp as i64,
                original_quantity: event.original_quantity as i64,
                quantity: event.base_asset_quantity_canceled as i64,
                filled_quantity: (event.original_quantity - event.base_asset_quantity_canceled) as i64,
                trader: event.trader.to_string(),
                balance_manager_id: event.balance_manager_id.to_string(),
            }
        },
        {
            event_type: OrderExpired,
            map_event: |event, meta| OrderUpdate {
                event_digest: meta.event_digest(),
                digest: meta.digest(),
                sender: meta.sender(),
                checkpoint: meta.checkpoint(),
                checkpoint_timestamp_ms: meta.checkpoint_timestamp_ms(),
                timestamp: ms_to_secs(meta.checkpoint_timestamp_ms()),
                package: meta.package(),
                status: OrderUpdateStatus::Expired,
                pool_id: event.pool_id.to_string(),
                order_id: event.order_id.to_string(),
                client_order_id: event.client_order_id as i64,
                price: event.price as i64,
                is_bid: event.is_bid,
                onchain_timestamp: event.timestamp as i64,
                original_quantity: event.original_quantity as i64,
                quantity: event.base_asset_quantity_canceled as i64,
                filled_quantity: (event.original_quantity - event.base_asset_quantity_canceled) as i64,
                trader: event.trader.to_string(),
                balance_manager_id: event.balance_manager_id.to_string(),
            }
        }
    ]
}

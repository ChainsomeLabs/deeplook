// @generated automatically by Diesel CLI.

diesel::table! {
    assets (type_) {
        #[sql_name = "type"]
        type_ -> Text,
        name -> Text,
        symbol -> Text,
        decimals -> Int2,
        ucid -> Nullable<Int4>,
        package_id -> Nullable<Text>,
        package_address_url -> Nullable<Text>,
    }
}

diesel::table! {
    balances (event_digest, timestamp) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        balance_manager_id -> Text,
        asset -> Text,
        amount -> Int8,
        deposit -> Bool,
    }
}

diesel::table! {
    flashloans (event_digest, timestamp) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        borrow -> Bool,
        pool_id -> Text,
        borrow_quantity -> Int8,
        type_name -> Text,
    }
}

diesel::table! {
    order_fills (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        maker_order_id -> Text,
        taker_order_id -> Text,
        maker_client_order_id -> Int8,
        taker_client_order_id -> Int8,
        price -> Int8,
        taker_fee -> Int8,
        taker_fee_is_deep -> Bool,
        maker_fee -> Int8,
        maker_fee_is_deep -> Bool,
        taker_is_bid -> Bool,
        base_quantity -> Int8,
        quote_quantity -> Int8,
        maker_balance_manager_id -> Text,
        taker_balance_manager_id -> Text,
        onchain_timestamp -> Int8,
    }
}

diesel::table! {
    order_updates (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        status -> Text,
        pool_id -> Text,
        order_id -> Text,
        client_order_id -> Int8,
        price -> Int8,
        is_bid -> Bool,
        original_quantity -> Int8,
        quantity -> Int8,
        filled_quantity -> Int8,
        onchain_timestamp -> Int8,
        balance_manager_id -> Text,
        trader -> Text,
    }
}

diesel::table! {
    orderbook_snapshots (pool_id, checkpoint) {
        checkpoint -> Int8,
        pool_id -> Text,
        asks -> Jsonb,
        bids -> Jsonb,
        timestamp -> Timestamp,
    }
}

diesel::table! {
    pool_prices (event_digest, timestamp) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        target_pool -> Text,
        reference_pool -> Text,
        conversion_rate -> Int8,
    }
}

diesel::table! {
    pools (pool_id) {
        pool_id -> Text,
        pool_name -> Text,
        base_asset_id -> Text,
        base_asset_decimals -> Int2,
        base_asset_symbol -> Text,
        base_asset_name -> Text,
        quote_asset_id -> Text,
        quote_asset_decimals -> Int2,
        quote_asset_symbol -> Text,
        quote_asset_name -> Text,
        min_size -> Int4,
        lot_size -> Int4,
        tick_size -> Int4,
    }
}

diesel::table! {
    proposals (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        balance_manager_id -> Text,
        epoch -> Int8,
        taker_fee -> Int8,
        maker_fee -> Int8,
        stake_required -> Int8,
    }
}

diesel::table! {
    rebates (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        balance_manager_id -> Text,
        epoch -> Int8,
        claim_amount -> Int8,
    }
}

diesel::table! {
    stakes (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        balance_manager_id -> Text,
        epoch -> Int8,
        amount -> Int8,
        stake -> Bool,
    }
}

diesel::table! {
    sui_error_transactions (id) {
        id -> Int4,
        txn_digest -> Text,
        sender_address -> Text,
        timestamp_ms -> Int8,
        failure_status -> Text,
        package -> Text,
        cmd_idx -> Nullable<Int8>,
    }
}

diesel::table! {
    trade_params_update (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        taker_fee -> Int8,
        maker_fee -> Int8,
        stake_required -> Int8,
    }
}

diesel::table! {
    votes (event_digest, timestamp, pool_id) {
        event_digest -> Text,
        digest -> Text,
        sender -> Text,
        checkpoint -> Int8,
        created_at -> Timestamp,
        checkpoint_timestamp_ms -> Int8,
        timestamp -> Timestamp,
        package -> Text,
        pool_id -> Text,
        balance_manager_id -> Text,
        epoch -> Int8,
        from_proposal_id -> Nullable<Text>,
        to_proposal_id -> Text,
        stake -> Int8,
    }
}

diesel::table! {
    watermarks (pipeline) {
        pipeline -> Text,
        epoch_hi_inclusive -> Int8,
        checkpoint_hi_inclusive -> Int8,
        tx_hi -> Int8,
        timestamp_ms_hi_inclusive -> Int8,
        reader_lo -> Int8,
        pruner_timestamp -> Timestamp,
        pruner_hi -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    assets,
    balances,
    flashloans,
    order_fills,
    order_updates,
    orderbook_snapshots,
    pool_prices,
    pools,
    proposals,
    rebates,
    stakes,
    sui_error_transactions,
    trade_params_update,
    votes,
    watermarks,
);

diesel::table! {
    balances_summary (asset) {
        asset -> Text,
        amount -> Int8,
        deposit -> Bool,
    }
}

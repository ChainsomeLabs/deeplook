// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

diesel::table! {
    ohlcv_1min (bucket, pool_id) {
        bucket -> Timestamp,
        pool_id -> Text,
        open -> BigInt,
        high -> BigInt,
        low -> BigInt,
        close -> BigInt,
        volume_base -> Numeric,
        volume_quote -> Numeric,
    }
}

diesel::table! {
    order_fill_24h_summary_view (pool_id) {
        pool_id -> Text,
        base_volume_24h -> Numeric,
        trade_count_24h -> Nullable<Numeric>,
        price_open_24h -> BigInt,
        price_close_24h -> BigInt,
    }
}

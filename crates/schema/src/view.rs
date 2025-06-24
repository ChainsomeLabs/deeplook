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
    }
}

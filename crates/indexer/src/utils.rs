use chrono::{DateTime, NaiveDateTime, Utc};

pub fn ms_to_secs(ms: i64) -> NaiveDateTime {
    DateTime::<Utc>::from_timestamp(ms / 1000, ((ms % 1000) * 1_000_000) as u32)
        .expect("failed parsing ms to secs")
        .naive_utc()
}

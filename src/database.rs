use std::collections::HashMap;

use crate::{common::current_time_ms, resp::RespValue};

pub(crate) struct Entry {
    value: RespValue,
    expiry_timestamp_ms: Option<u128>,
}

pub(crate) struct Database {
    dict: HashMap<String, Entry>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            dict: HashMap::new(),
        }
    }

    pub(crate) fn set(&mut self, key: String, value: RespValue, expiry_ms: Option<u128>) {
        let now_ms = current_time_ms();
        let expiry_timestamp_ms = expiry_ms.map(|ttl| now_ms + ttl);
        self.dict.insert(
            key,
            Entry {
                value,
                expiry_timestamp_ms,
            },
        );
    }

    pub(crate) fn get(&self, key: &String) -> Option<&RespValue> {
        self.dict.get(key).and_then(|entry| {
            if let Some(expiry_timestamp_ms) = entry.expiry_timestamp_ms {
                if expiry_timestamp_ms >= current_time_ms() {
                    Some(&entry.value)
                } else {
                    None
                }
            } else {
                Some(&entry.value)
            }
        })
    }
}

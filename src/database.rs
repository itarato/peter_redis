use std::collections::HashMap;

use crate::common::current_time_ms;

struct ValueEntry {
    value: String,
    expiry_timestamp_ms: Option<u128>,
}

enum Entry {
    Value(ValueEntry),
    Array(Vec<String>),
}

impl Entry {
    fn is_value(&self) -> bool {
        match self {
            Entry::Value(_) => true,
            _ => false,
        }
    }

    fn is_array(&self) -> bool {
        match self {
            Entry::Array(_) => true,
            _ => false,
        }
    }
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

    pub(crate) fn set(
        &mut self,
        key: String,
        value: String,
        expiry_ms: Option<u128>,
    ) -> Result<(), String> {
        self.assert_single_value(&key)?;

        let now_ms = current_time_ms();
        let expiry_timestamp_ms = expiry_ms.map(|ttl| now_ms + ttl);

        self.dict
            .entry(key)
            .and_modify(|entry| match entry {
                Entry::Value(value_entry) => {
                    value_entry.value = value.clone();
                    value_entry.expiry_timestamp_ms = expiry_timestamp_ms;
                }
                _ => unreachable!(),
            })
            .or_insert(Entry::Value(ValueEntry {
                value,
                expiry_timestamp_ms,
            }));

        Ok(())
    }

    pub(crate) fn get(&self, key: &String) -> Result<Option<&String>, String> {
        self.assert_single_value(key)?;

        Ok(self.dict.get(key).and_then(|entry| {
            let Entry::Value(value_entry) = entry else {
                unreachable!();
            };

            if let Some(expiry_timestamp_ms) = value_entry.expiry_timestamp_ms {
                if expiry_timestamp_ms >= current_time_ms() {
                    Some(&value_entry.value)
                } else {
                    None
                }
            } else {
                Some(&value_entry.value)
            }
        }))
    }

    pub(crate) fn push_to_array(
        &mut self,
        key: String,
        mut values: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_array(&key)?;

        let entry = self.dict.entry(key.clone()).or_insert(Entry::Array(vec![]));
        let Entry::Array(array) = entry else {
            unreachable!();
        };

        array.append(&mut values);

        Ok(array.len())
    }

    pub(crate) fn get_list_lrange(
        &self,
        key: &String,
        start: i64,
        end: i64,
    ) -> Result<Vec<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(vec![]);
        }

        let Entry::Array(array) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        let start = if start < 0 {
            (start + array.len() as i64).max(0)
        } else {
            start
        };

        let mut out = vec![];
        for i in start..=end {
            if i >= array.len() as i64 {
                break;
            }

            out.push(array[i as usize].clone());
        }

        Ok(out)
    }

    fn assert_array(&self, key: &String) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_array()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }

    fn assert_single_value(&self, key: &String) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_value()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }
}

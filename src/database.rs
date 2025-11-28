use std::collections::{HashMap, VecDeque};

use crate::common::current_time_ms;

struct ValueEntry {
    value: String,
    expiry_timestamp_ms: Option<u128>,
}

enum Entry {
    Value(ValueEntry),
    Array(VecDeque<String>),
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

    fn type_name(&self) -> &str {
        match self {
            Entry::Array(_) => "list",
            Entry::Value(_) => "string",
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
        values: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_array(&key)?;

        let entry = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::Array(VecDeque::new()));
        let Entry::Array(array) = entry else {
            unreachable!();
        };

        for value in values {
            array.push_back(value);
        }

        Ok(array.len())
    }

    pub(crate) fn insert_to_array(
        &mut self,
        key: String,
        values: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_array(&key)?;

        let entry = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::Array(VecDeque::new()));
        let Entry::Array(array) = entry else {
            unreachable!();
        };

        for value in values {
            array.push_front(value);
        }

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

        let end = if end < 0 {
            (end + array.len() as i64).max(0)
        } else {
            end
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

    pub(crate) fn list_length(&self, key: &str) -> Result<usize, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(0);
        }

        let Entry::Array(array) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(array.len())
    }

    pub(crate) fn list_pop_one_front(&mut self, key: &str) -> Result<Option<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        match array.pop_front() {
            Some(elem) => Ok(Some(elem)),
            _ => Ok(None),
        }
    }

    pub(crate) fn list_pop_one_back(&mut self, key: &str) -> Result<Option<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        match array.pop_back() {
            Some(elem) => Ok(Some(elem)),
            _ => Ok(None),
        }
    }

    pub(crate) fn list_pop_multi_front(
        &mut self,
        key: &str,
        n: usize,
    ) -> Result<Option<Vec<String>>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        if array.is_empty() {
            return Ok(None);
        }

        let mut out = vec![];
        for _ in 0..n {
            if array.is_empty() {
                break;
            }

            out.push(array.pop_front().unwrap());
        }

        Ok(Some(out))
    }

    pub(crate) fn list_pop_multi_back(
        &mut self,
        key: &str,
        n: usize,
    ) -> Result<Option<Vec<String>>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        if array.is_empty() {
            return Ok(None);
        }

        let mut out = vec![];
        for _ in 0..n {
            if array.is_empty() {
                break;
            }

            out.push(array.pop_back().unwrap());
        }

        Ok(Some(out))
    }

    pub(crate) fn get_key_type_name(&self, key: &str) -> &str {
        self.dict
            .get(key)
            .map(|elem| elem.type_name())
            .unwrap_or("none")
    }

    fn assert_array(&self, key: &str) -> Result<(), String> {
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

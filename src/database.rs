use std::collections::HashMap;

use crate::resp::RespValue;

pub(crate) struct Database {
    dict: HashMap<String, RespValue>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            dict: HashMap::new(),
        }
    }

    pub(crate) fn set(&mut self, key: String, value: RespValue) {
        self.dict.insert(key, value);
    }

    pub(crate) fn get(&self, key: &String) -> Option<&RespValue> {
        self.dict.get(key)
    }
}

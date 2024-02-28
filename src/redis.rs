use std::collections::HashMap;

#[derive(Debug)]
pub enum RedisValue {
    String(String),
}

pub struct Redis {
    store: HashMap<String, RedisValue>,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::<String, RedisValue>::new(),
        }
    }

    pub fn set(&mut self, key: String, value: RedisValue) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        match self.store.get(&key) {
            Some(RedisValue::String(value)) => Some(value.clone()),
            None => None,
        }
    }
}

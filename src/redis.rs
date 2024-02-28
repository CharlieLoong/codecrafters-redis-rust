use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

#[derive(Debug)]
pub enum RedisValue {
    String(String),
}
#[derive(PartialEq)]
pub enum Role {
    Master,
    Slave,
}

pub struct Redis {
    store: HashMap<String, (RedisValue, SystemTime)>,
    expr: Duration,
    pub role: Role,
    master_host: Option<String>,
}
const DEFAULT_EXPIRY: Duration = Duration::from_secs(60);

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Master,
            master_host: None,
        }
    }

    pub fn slave(master_host: String) -> Self {
        // TODO
        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Slave,
            master_host: Some(master_host),
        }
    }

    pub fn set(&mut self, key: String, value: RedisValue, expr: Option<Duration>) {
        self.store
            .insert(key, (value, SystemTime::now() + expr.unwrap_or(self.expr)));
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        match self.store.get(&key) {
            Some((RedisValue::String(value), expr)) => {
                if *expr < SystemTime::now() {
                    self.store.remove(&key);
                    None
                } else {
                    Some(value.clone())
                }
            }
            None => None,
        }
    }
}

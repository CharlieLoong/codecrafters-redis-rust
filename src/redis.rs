use rand::{distributions::Alphanumeric, Rng};

use std::net::Ipv4Addr;

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
    master_host: Option<Ipv4Addr>,
    master_port: Option<u16>,
    master_replid: Option<String>,
    master_repl_offset: u64,
}
const DEFAULT_EXPIRY: Duration = Duration::from_secs(60);

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Master,
            master_host: None,
            master_port: None,
            master_replid: Some(gen_id()),
            master_repl_offset: 0,
        }
    }

    pub fn slave(master_host: Ipv4Addr, master_port: u16) -> Self {
        // HandShake to master

        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Slave,
            master_host: Some(master_host),
            master_port: Some(master_port),
            master_replid: None,
            master_repl_offset: 0,
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

    pub fn info(&self) -> String {
        let mut info = vec![];
        match self.role {
            Role::Master => {
                info.push(format!("role:master"));
                info.push(format!(
                    "master_replid:{}",
                    self.master_replid.clone().unwrap()
                ));
                info.push(format!("master_repl_offset:{}", self.master_repl_offset));
            }
            Role::Slave => {
                info.push(format!("role:slave"));
            }
        }
        info.join("\n")
    }
}

fn gen_id() -> String {
    let mut rng = rand::thread_rng();
    let random_string: String = (0..40)
        .map(|_| rng.sample(Alphanumeric).to_string())
        .collect();

    random_string
}

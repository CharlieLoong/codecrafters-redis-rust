#[allow(dead_code)]
use anyhow::Result;
use bytes::BytesMut;
use rand::{distributions::Alphanumeric, Rng};

use tokio::sync::mpsc;
use tokio::time::sleep;

use std::fmt::Display;
use std::net::Ipv4Addr;

use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::command::SET;

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
    Stream(Vec<(String, Vec<(String, String)>)>),
}
impl Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RedisValue::String(s) => write!(f, "{}", s),
            _ => write!(f, "undefined"),
        }
    }
}
impl RedisValue {
    pub fn to_string(self) -> String {
        match self {
            RedisValue::String(s) => s,
            _ => "undefined".to_string(),
        }
    }

    // pub fn unwrap(self) -> _ {
    //     match self {
    //         Self::String(s) => s,
    //         Self::Stream(s) => s,
    //         _ => "undefined".to_string(),
    //     }
    // }
}

#[derive(PartialEq, Clone, Copy)]
pub enum Role {
    Master,
    Slave,
}
#[derive(Clone)]
pub struct Item {
    pub value: RedisValue,
    pub expire: SystemTime,
}

pub struct Redis {
    store: HashMap<String, Item>,
    expr: Duration,
    pub role: Role,
    pub port: u16,
    pub master_host: Option<Ipv4Addr>,
    pub master_port: Option<u16>,
    pub master_replid: Option<String>,
    pub slaves: Vec<Replica>,
    master_repl_offset: u64,
    pub processed: usize,
    pub offset: usize,
}
const DEFAULT_EXPIRY: Duration = Duration::from_secs(60);

impl Redis {
    pub fn new(port: u16, store: HashMap<String, Item>) -> Self {
        Self {
            store,
            expr: DEFAULT_EXPIRY,
            role: Role::Master,
            port,
            master_host: None,
            master_port: None,
            master_replid: Some(gen_id()),
            master_repl_offset: 0,
            slaves: Vec::new(),
            processed: 0,
            offset: 0,
        }
    }

    pub fn slave(port: u16, master_host: Ipv4Addr, master_port: u16) -> Self {
        // HandShake to master

        Self {
            store: HashMap::<String, Item>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Slave,
            port,
            master_host: Some(master_host),
            master_port: Some(master_port),
            master_replid: None,
            master_repl_offset: 0,
            slaves: Vec::new(),
            processed: 0,
            offset: 0,
        }
    }

    pub async fn set(
        &mut self,
        key: String,
        value: RedisValue,
        expr: Option<Duration>,
    ) -> Result<()> {
        println!("set happens on {}", self.port);
        self.store.insert(
            key.clone(),
            Item {
                value: value.clone(),
                expire: SystemTime::now() + expr.unwrap_or(self.expr),
            },
        );
        let set_command = SET::new(key.clone(), value.to_string(), expr).serialize();

        for slave in &self.slaves {
            print!("write to slave listening on port ");
            println!("{}", slave.port);
            let _ = slave
                .channel
                .send(BytesMut::from(set_command.clone().serialize()));
        }
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Option<String> {
        sleep(Duration::from_millis(20)).await; // test 13 sometimes failed
        match self.store.get(&key) {
            Some(item) => {
                if item.expire < SystemTime::now() {
                    self.store.remove(&key);
                    None
                } else {
                    Some(item.value.clone().to_string())
                }
            }
            None => None,
        }
    }

    pub fn keys(&self) -> Vec<String> {
        self.store.keys().map(|k| k.to_string()).collect()
    }

    pub async fn _type(&mut self, key: String) -> String {
        match self.store.get(&key) {
            Some(item) => match item.value {
                RedisValue::Stream(_) => "stream".to_owned(),
                RedisValue::String(_) => "string".to_owned(),
            },
            None => "none".to_string(),
        }
    }

    pub async fn xadd(
        &mut self,
        stream_key: String,
        id: String,
        items: Vec<(String, String)>,
    ) -> String {
        self.store
            .entry(stream_key)
            .and_modify(|item| {
                if let RedisValue::Stream(ref mut stream) = item.value {
                    stream.push((id.clone(), items.clone()))
                }
            })
            .or_insert(Item {
                value: RedisValue::Stream(vec![]),
                expire: SystemTime::now() + Duration::from_secs(6000),
            });
        id
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
                info.push(format!("master_repl_offset:{}", self.offset));
            }
            Role::Slave => {
                info.push(format!("role:slave"));
                info.push(format!("offset:{}", self.offset));
            }
        }
        info.join("\n")
    }

    pub fn add_slave(&mut self, r: Replica) {
        if self.role != Role::Master {
            return;
        }
        self.slaves.push(r);
    }

    pub fn is_slave(&self) -> bool {
        self.role == Role::Slave
    }

    pub fn slaves_count(&self) -> usize {
        self.slaves.len()
    }

    pub fn check_processed(&self) -> usize {
        self.processed
    }
    pub fn reset_processed(&mut self) {
        self.processed = 0;
    }
}

fn gen_id() -> String {
    let mut rng = rand::thread_rng();
    let random_string: String = (0..40)
        .map(|_| rng.sample(Alphanumeric).to_string())
        .collect();

    random_string
}

#[derive(Debug)]
pub struct Replica {
    pub port: String,
    pub channel: mpsc::UnboundedSender<BytesMut>,
    pub offset: usize,
    // pub stream: TcpStream,
}
#[allow(dead_code)]
impl Replica {
    pub fn new(
        port: String,
        channel: mpsc::UnboundedSender<BytesMut>, /*stream: TcpStream*/
    ) -> Self {
        Self {
            port,
            channel,
            offset: 0,
            // stream,
        }
    }
}

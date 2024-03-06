use anyhow::anyhow;
#[allow(dead_code)]
use anyhow::Result;
use bytes::BytesMut;
use itertools::Itertools;
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

    pub fn set(&mut self, key: String, value: RedisValue, expr: Option<Duration>) -> Result<()> {
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
        // sleep(Duration::from_millis(20)).await; // test 13 sometimes failed
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
    ) -> Result<String> {
        if let Some(item) = self.store.get_mut(&stream_key) {
            match item.value {
                RedisValue::Stream(ref mut stream) => {
                    let last_id = &stream.last().unwrap().0;
                    let id = Self::parse_stream_id(id.clone(), last_id.clone())?;
                    stream.push((id.clone(), items));
                    return Ok(id);
                }
                _ => return Err(anyhow!("Stream key is not a stream")),
            }
        } else {
            let id = Self::parse_stream_id(id.clone(), "0-0".to_string())?;
            self.store.insert(
                stream_key,
                Item {
                    value: RedisValue::Stream(vec![(id.clone(), items)]),
                    expire: SystemTime::now() + Duration::from_secs(6000), // TODO
                },
            );
            Ok(id)
        }
        // let (millis, sequence) = Self::parse_stream_id(&id)?;
        // let mut err: Option<&str> = None;
        // self.store
        //     .entry(stream_key)
        //     .and_modify(|item| {
        //         if let RedisValue::Stream(ref mut stream) = item.value {
        //             let last_id = &stream.last().unwrap().0;
        //             let (last_millis, last_sequence) = Self::parse_stream_id(last_id).unwrap();
        //             if sequence == '*' as u64 {

        //             }
        //             if !(millis > last_millis || (millis == last_millis && sequence > last_sequence)) {
        //                 err = Some("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        //             }
        //             stream.push((id.clone(), items.clone()));
        //         } else {
        //             err = Some("bad key, wrong type of value");
        //         }
        //     })
        //     .or_insert(Item {
        //         value: RedisValue::Stream(vec![(id.clone(), items)]),
        //         expire: SystemTime::now() + Duration::from_secs(6000),
        //     });
        // if let Some(err) = err {
        //     return Err(anyhow!(err));
        // }
    }

    pub fn xrange(
        &self,
        stream_key: String,
        mut start: String,
        mut end: String,
    ) -> Option<Vec<(String, Vec<(String, String)>)>> {
        if start == "-" {
            start = "0-0".to_string();
        }
        if end == "+" {
            end = "2000000000000-0".to_string() //TODO
        }
        match self.store.get(&stream_key) {
            Some(item) => {
                if let RedisValue::Stream(stream) = &item.value {
                    let mut result = Vec::new();
                    for (id, item) in stream.iter() {
                        if *id < start {
                            continue;
                        }
                        if *id > end {
                            break;
                        }
                        result.push((id.clone(), item.clone()));
                    }
                    return Some(result);
                }
                return None;
            }
            _ => None,
        }
    }

    pub fn xread(
        &self,
        count: usize,
        stream_keys: Vec<String>,
        starts: Vec<String>,
        block: u64,
    ) -> Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> {
        // if block > 0 {
        //     sleep(Duration::from_millis(block)).await;
        // }
        let mut ret = Vec::new();
        for i in 0..count {
            match self.store.get(&stream_keys[i]) {
                Some(item) => {
                    if let RedisValue::Stream(stream) = &item.value {
                        let mut result = Vec::new();
                        for (id, item) in stream.iter() {
                            if *id <= starts[i] {
                                continue;
                            }
                            result.push((id.clone(), item.clone()));
                        }
                        ret.push((stream_keys[i].clone(), result));
                    }
                }
                _ => {}
            }
        }
        Some(ret)
    }

    fn parse_stream_id(mut id: String, last_id: String) -> Result<String> {
        println!("cur id: {}, last id: {}", id, last_id);
        if id == "0-0" {
            return Err(anyhow!(
                "ERR The ID specified in XADD must be greater than 0-0"
            ));
        }
        if id == "*" {
            return Ok(format!(
                "{}-{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                0
            ));
        }
        let (last_millis, last_sequence) = last_id
            .split("-")
            .map(|s| s.parse::<u64>().unwrap())
            .take(2)
            .collect_tuple()
            .unwrap();
        let (cur_millis, cur_sequence) = id
            .split("-")
            .take(2)
            .map(str::to_string)
            .collect_tuple()
            .unwrap();
        let cur_millis = cur_millis.parse::<u64>()?;
        if cur_sequence == "*" {
            let cur_sequence = if cur_millis == last_millis {
                last_sequence + 1
            } else {
                0
            };
            id = format!("{}-{}", cur_millis, cur_sequence);
        } else {
            let cur_sequence = cur_sequence.parse::<u64>()?;
            // println!(
            //     "{} {} {} {}",
            //     cur_millis, cur_sequence, last_millis, last_sequence
            // );
            if !(cur_millis > last_millis
                || (cur_millis == last_millis && cur_sequence > last_sequence))
            {
                return Err(anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
            }
        }
        Ok(id)
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

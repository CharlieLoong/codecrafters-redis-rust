#[allow(dead_code)]
use anyhow::Result;
use bytes::BytesMut;
use rand::{distributions::Alphanumeric, Rng};

use tokio::sync::mpsc;
use tokio::time;

use std::fmt::Display;
use std::net::Ipv4Addr;

use std::thread::sleep;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::command::SET;

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
}
impl Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RedisValue::String(s) => write!(f, "{}", s),
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
pub enum Role {
    Master,
    Slave,
}

pub struct Redis {
    store: HashMap<String, (RedisValue, SystemTime)>,
    expr: Duration,
    pub role: Role,
    pub port: u16,
    pub master_host: Option<Ipv4Addr>,
    pub master_port: Option<u16>,
    pub master_replid: Option<String>,
    slaves: Vec<Replica>,
    master_repl_offset: u64,
}
const DEFAULT_EXPIRY: Duration = Duration::from_secs(60);

impl Redis {
    pub fn new(port: u16) -> Self {
        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Master,
            port,
            master_host: None,
            master_port: None,
            master_replid: Some(gen_id()),
            master_repl_offset: 0,
            slaves: Vec::new(),
        }
    }

    pub fn slave(port: u16, master_host: Ipv4Addr, master_port: u16) -> Self {
        // HandShake to master

        Self {
            store: HashMap::<String, (RedisValue, SystemTime)>::new(),
            expr: DEFAULT_EXPIRY,
            role: Role::Slave,
            port,
            master_host: Some(master_host),
            master_port: Some(master_port),
            master_replid: None,
            master_repl_offset: 0,
            slaves: Vec::new(),
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
            (value.clone(), SystemTime::now() + expr.unwrap_or(self.expr)),
        );
        let set_command = SET::new(key.clone(), value.to_string(), expr).serialize();
        // if self.slaves.len() > 0 {
        //     let slave_address = "127.0.0.1:6380".to_socket_addrs()?.next().unwrap();
        //     let mut stream = TcpStream::connect(slave_address).await?;
        //     stream
        //         .write(set_command.clone().serialize().as_bytes())
        //         .await?;
        // }

        // println!("{:?}", self.port);
        // println!("{:?}", self.slaves.len());
        for slave in self.slaves.iter() {
            print!("write to slave listening on port ");
            println!("{}", slave.port);
            // let slave_address = format!("127.0.0.1:{}", 6380).to_socket_addrs()?.next().unwrap();
            // let mut stream = TcpStream::connect(slave_address).await?;
            // stream
            //     .write(set_command.clone().serialize().as_bytes())
            //     .await?;
            // println!("{}", set_command.clone().serialize());
            slave
                .channel
                .send(BytesMut::from(set_command.clone().serialize()))?;
        }
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Option<String> {
        //sleep(time::Duration::from_millis(1000));
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

    pub fn add_slave(&mut self, r: Replica) {
        if self.role != Role::Master {
            return;
        }
        self.slaves.push(r);
    }

    pub fn is_slave(&self) -> bool {
        self.role == Role::Slave
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
    // pub stream: TcpStream,
}
#[allow(dead_code)]
impl Replica {

    pub fn new(port: String, channel: mpsc::UnboundedSender<BytesMut>, /*stream: TcpStream*/) -> Self {
        Self {
            port,
            channel,
            // stream,
        }
    }
}

#[allow(dead_code)]
mod command;
mod rdb;
mod redis;
mod resp;

use std::{
    collections::HashMap,
    env::args,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rdb::empty_rdb;
use redis::Replica;
// use clap::Parser;
use resp::Value;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex, RwLock},
};

use crate::rdb::RdbReader;
use crate::redis::Role;

/// Simple program to greet a person
// #[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
// struct Args {
//     #[arg(short, long, default_value_t = 6379)]
//     port: u16,
// }

#[tokio::main]
async fn main() {
    let mut args_iter = args().skip(1);
    let mut port: u16 = 6379;
    let mut role = Role::Master;
    let mut master_host = None;
    let mut master_host_ipv4 = None;
    let mut master_port = None;
    let mut is_slave = false;
    let mut dir = None;
    let mut dbfilename = None;

    // let mut slaves: Mutex<Vec<Replica>> = Mutex::new(vec![]);

    while let Some(arg) = args_iter.next() {
        match arg.to_lowercase().as_str() {
            "--port" => {
                port = args_iter
                    .next()
                    .unwrap()
                    .parse::<u16>()
                    .expect("invalid port number")
            }
            "--replicaof" => {
                role = Role::Slave;
                master_host = Some(args_iter.next().unwrap());
                master_port = Some(
                    args_iter
                        .next()
                        .unwrap()
                        .parse::<u16>()
                        .expect("invalid port number"),
                );
            }
            "--dir" => {
                dir = Some(args_iter.next().unwrap());
            }
            "--dbfilename" => {
                dbfilename = Some(args_iter.next().unwrap());
            }
            _ => {}
        }
    }

    println!("Logs from your program will appear here!");
    let rdb_map = if dir.is_none() || dbfilename.is_none() {
        HashMap::new()
    } else {
        let file_path = dir.clone().unwrap() + "/" + &dbfilename.clone().unwrap();

        RdbReader::read_from_file(file_path)
    };

    let redis = if role == Role::Master {
        redis::Redis::new(port, rdb_map)
    } else {
        is_slave = true;
        master_host_ipv4 = if master_host.clone().unwrap() == "localhost" {
            Some("127.0.0.1".parse::<Ipv4Addr>().unwrap())
        } else {
            Some(master_host.unwrap().parse::<Ipv4Addr>().unwrap())
        };
        redis::Redis::slave(port, master_host_ipv4.unwrap(), master_port.unwrap())
    };

    let redis = Arc::new(Mutex::new(redis));
    let shared_state = Arc::new(RwLock::new(State::new(
        role,
        port,
        dir.unwrap_or("null".to_string()),
        dbfilename.unwrap_or("null".to_string()),
    )));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to bind");
    println!("listening on port {}", port);
    // let master_socket = SocketAddrV4::new(master_host_ipv4.unwrap(), master_port.unwrap());

    if is_slave {
        if let Ok(master_stream) = TcpStream::connect(SocketAddrV4::new(
            master_host_ipv4.unwrap(),
            master_port.unwrap(),
        ))
        .await
        {
            let redis_clone = Arc::clone(&redis);
            let state_clone = Arc::clone(&shared_state);
            tokio::spawn(async move {
                // let ping = Value::Array(vec![Value::BulkString("PING".to_string())]);
                // let replconf1 = Value::Array(vec![
                //     Value::BulkString("REPLCONF".to_string()),
                //     Value::BulkString("listening-port".to_string()),
                //     Value::BulkString(port.to_string()),
                // ]);
                // let replconf2 = Value::Array(vec![
                //     Value::BulkString("REPLCONF".to_string()),
                //     Value::BulkString("capa".to_string()),
                //     Value::BulkString("psync2".to_string()),
                // ]);
                // let psync = Value::Array(vec![
                //     Value::BulkString("PSYNC".to_string()),
                //     Value::BulkString("?".to_string()),
                //     Value::BulkString("-1".to_string()),
                // ]);
                // let mut handler = resp::RespHandler::new(master_stream);
                // handler.write_value(ping).await.unwrap();
                // handler.read_values().await.unwrap(); // PONG
                // handler.write_value(replconf1).await.unwrap();
                // handler.read_values().await.unwrap(); // OK
                // handler.write_value(replconf2).await.unwrap();
                // handler.read_values().await.unwrap(); // OK
                // handler.write_value(psync).await.unwrap();
                // let sync = handler.read_values().await; // FULLRSYNC
                // println!("FULLRESYNC_command: {:?}", sync);
                //let rdb = handler.read_file().await.unwrap(); // rdb file
                //println!("got rdb file: {:?}", String::from_utf8_lossy(&rdb.unwrap()));
                handle_stream(master_stream, redis_clone, state_clone, true).await
            });
        } else {
            eprintln!("Could not connect to replication master");
        }
    }
    println!("start serving");

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut _stream, _addr)) => {
                println!(
                    "accepted new connection from port {}",
                    _stream.peer_addr().unwrap().port()
                );
                //handle_stream(&mut _stream);
                let redis_clone = Arc::clone(&redis);
                let state_clone = Arc::clone(&shared_state);

                // let shared_stream = Arc::new(Mutex::new(_stream));
                tokio::spawn(async move {
                    handle_stream(_stream, redis_clone, state_clone, false).await
                });
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

async fn handle_stream(
    stream: TcpStream,
    redis_clone: Arc<Mutex<redis::Redis>>,
    shared_state: Arc<RwLock<State>>,
    // slaves: Mutex<Vec<Replica>>,
    handshake: bool,
) -> Result<()> {
    //let mut handler = resp::RespHandler::new(stream);

    let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();
    let mut handler = resp::RespHandler::new(stream);

    if handshake {
        handler._handshake(shared_state.read().await.port).await;
    }

    //let mut processed_slave = 0;
    loop {
        // let value = handler.read_value().await.unwrap();
        tokio::select! {
            values = handler.read_values() => {
                //let mut responses = vec![];
                if let Ok(Some(v)) = values {
                    println!("[receive values]: {:?}", v);
                    for value in v {
                        let (command, args, mut len) = extract_command(value.clone()).unwrap_or(("Extract Failed".to_owned(), vec![], 0));

                        let response: Value = match command.to_lowercase().as_str() {
                            "ping" => {
                                let res = if redis_clone.lock().await.is_slave() {
                                    Value::Empty
                                } else {
                                    Value::SimpleString("PONG".to_string())
                                };
                                res
                            },
                            "echo" => args.first().unwrap().clone(),
                            "set" => {
                                let key = unpack_bulk_str(args[0].clone()).unwrap();
                                let val = unpack_bulk_str(args[1].clone()).unwrap();
                                let expr = if args.len() >= 4 {
                                    Some(Duration::from_millis(
                                        unpack_bulk_str(args[3].clone()).unwrap().parse().expect("invalid expr format"),
                                    ))
                                } else {
                                    None
                                };
                                redis_clone
                                    .lock()
                                    .await
                                    .set(key, redis::RedisValue::String(val), expr)
                                    .await.expect("set command failed");

                                // println!("{}",redis_clone.lock().await.master_port.unwrap_or(000));
                                // println!("{}",handler.stream.peer_addr().unwrap().port());
                                let res = if shared_state.read().await.is_slave() {
                                    // Value::Array(vec![
                                    //     Value::BulkString("REPLCONF".to_string()),
                                    //     Value::BulkString("ACK".to_string()),
                                    //     Value::BulkString(offset.to_string()),
                                    // ])
                                    Value::Empty
                                } else {
                                    Value::SimpleString("OK".to_string())
                                };

                                res
                            }
                            "get" => {
                                let key = unpack_bulk_str(args[0].clone()).unwrap();
                                let res = match redis_clone.lock().await.get(key).await {
                                    Some(val) => Value::BulkString(val),
                                    None => Value::Null,
                                };
                                println!("get-response: {:?}", res.clone().decode());
                                res
                            }
                            "info" => {
                                // todo
                                len = 0;
                                Value::BulkString(redis_clone.lock().await.info())
                            },
                            "replconf" => {
                                let res = match args[0].clone().decode().to_lowercase().as_str() {
                                    "listening-port" => {
                                        // slaves.lock().await.push(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                                        // let framed = Framed::new(handler.stream, LengthDelimitedCodec::new());
                                        redis_clone.lock().await.add_slave(Replica { port: args[1].clone().decode() , channel: tx.clone(), offset: 0 });
                                        shared_state.write().await.add_slave(Replica { port: args[1].clone().decode() , channel: tx.clone(), offset: 0 });
                                        Value::SimpleString("OK".to_string())
                                    }
                                    "getack" => {
                                        if shared_state.read().await.is_slave() {
                                            Value::Array(vec![
                                                Value::BulkString("REPLCONF".to_string()),
                                                Value::BulkString("ACK".to_string()),
                                                Value::BulkString(shared_state.read().await.offset.to_string()),
                                            ])
                                        } else {
                                            Value::Empty
                                        }
                                    }
                                    "ack" => {
                                        len = 0;
                                        let slave_offset: usize = args[1].clone().decode().parse().unwrap();
                                        println!("slave offset: {}, master offset: {}", slave_offset, shared_state.read().await.offset);
                                        if slave_offset >= shared_state.read().await.offset {
                                            shared_state.write().await.processed += 1;
                                        }

                                        Value::Empty
                                    }
                                    _ => Value::SimpleString("OK".to_string())
                                };
                                res
                            }
                            "psync" => {
                                // TODO
                                let lock = redis_clone.lock().await;
                                len = 0;
                                shared_state.write().await.offset = 0;
                                handler
                                    .write_value(Value::SimpleString(format!(
                                        "FULLRESYNC {} {}",
                                        lock.master_replid.clone().unwrap(),
                                        shared_state.read().await.offset
                                    )))
                                    .await
                                    .unwrap();
                                // handler
                                //     .write_bytes(format!("${}\r\n", empty_rdb().len()).as_bytes())
                                //     .await
                                //     .unwrap();
                                // handler.write_bytes(&empty_rdb()).await.unwrap();
                                handler.write_value(Value::File(empty_rdb())).await.unwrap();

                                Value::Empty
                                //Value::SYNC(redis_clone.lock().await.master_replid.clone().unwrap())
                            }

                            "wait" => {
                                len = 0;
                                let wait_cnt = args[0].clone().decode().parse::<usize>().unwrap();
                                let timeout = args[1].clone().decode().parse::<u64>().unwrap();
                                let cnt = handle_wait(wait_cnt, timeout, redis_clone.clone(), shared_state.clone()).await;

                                Value::Integers(cnt as i64)

                            }

                            "fullresync" => {
                                let sync_offset = args[1].clone().decode().parse::<usize>().unwrap();
                                shared_state.write().await.offset = sync_offset;
                                Value::Empty
                            }
                            "config" => {
                                let response = match args[0].clone().decode().to_lowercase().as_str() {
                                    "get" => {
                                        match args[1].clone().decode().to_lowercase().as_str() {
                                            "dir" => {
                                                Value::Array(vec![
                                                    Value::BulkString("dir".to_string()),
                                                    Value::BulkString(shared_state.read().await.dir.clone())
                                                ])
                                            }
                                            "dbfilename" => {
                                                Value::Array(vec![
                                                    Value::BulkString("dbfilename".to_string()),
                                                    Value::BulkString(shared_state.read().await.dbfilename.clone())
                                                ])
                                            }
                                            _ => todo!()
                                        }
                                    }
                                    _ => todo!()
                                };
                                response
                            }

                            "keys" => {
                                let _pattern = args[0].clone().decode().to_string();
                                let keys = redis_clone.lock().await.keys();
                                Value::Array(keys.iter().map(|k| Value::BulkString(k.to_string())).collect())
                            }

                            _ => {
                                println!("unknown command, {} : {:?}", command, args);
                                Value::Empty
                            },
                        };
                        //responses.push(response);
                        handler.write_value(response).await.unwrap();

                        shared_state.write().await.offset += len;
                        // redis_clone.lock().await.offset += len;
                    };
            };


            }
            cmd = rx.recv() => {
                    if let Some(cmd) = cmd {
                        println!("writing cmd to slave, {:?}", cmd);
                        handler.stream.write_all(cmd.as_ref()).await?;
                        handler.stream.flush().await?;
                        // handler.write_bytes(cmd.as_ref()).await.unwrap();
                    }

            }
        }
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>, usize)> {
    let len = value.serialize().len();
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
            len,
        )),
        Value::SimpleString(s) => Ok((
            s.split(' ').take(1).collect(),
            s.split(' ')
                .skip(1)
                .map(|s| Value::SimpleString(s.to_string()))
                .collect(),
            0,
        )),
        _ => Err(anyhow!("Invalid command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow!("Expect bulk string")),
    }
}

fn _broadcast_to_replica() {
    unimplemented!()
}

async fn handle_wait(
    wait_cnt: usize,
    timeout: u64,
    redis_clone: Arc<Mutex<redis::Redis>>,
    shared_state: Arc<RwLock<State>>,
) -> usize {
    if shared_state.read().await.offset == 0 {
        return shared_state.read().await.slaves.len();
    }
    shared_state.write().await.processed = 0;
    for slave in &shared_state.read().await.slaves {
        let _ = slave.channel.send(
            Value::Array(vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("GETACK".to_string()),
                Value::BulkString("*".to_string()),
            ])
            .serialize(),
        );
    }
    println!("master offset: {}", shared_state.read().await.offset);
    let expr_time = SystemTime::now() + Duration::from_millis(timeout);
    loop {
        let cur_cnt = shared_state.read().await.processed;
        if cur_cnt >= wait_cnt {
            break cur_cnt;
        }
        if SystemTime::now() > expr_time {
            break cur_cnt;
        }
    }
}

pub struct State {
    pub role: Role,
    pub slaves: Vec<Replica>,
    pub offset: usize,
    pub port: u16,
    pub processed: usize,
    pub dir: String,
    pub dbfilename: String,
}

impl State {
    pub fn new(role: Role, port: u16, dir: String, dbfilename: String) -> Self {
        Self {
            role,
            slaves: Vec::new(),
            offset: 0,
            port,
            processed: 0,
            dir,
            dbfilename,
        }
    }

    pub fn add_slave(&mut self, r: Replica) {
        self.slaves.push(r);
    }

    pub fn is_slave(&self) -> bool {
        self.role == Role::Slave
    }
}

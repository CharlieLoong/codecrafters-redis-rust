#[allow(dead_code)]
mod command;
mod rdb;
mod redis;
mod resp;

use std::{
    env::args,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
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
    sync::{mpsc, Mutex},
};

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
            _ => {}
        }
    }

    println!("Logs from your program will appear here!");
    //let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();

    let redis = if role == Role::Master {
        redis::Redis::new(port)
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
            tokio::spawn(async move {
                let ping = Value::Array(vec![Value::BulkString("PING".to_string())]);
                let replconf1 = Value::Array(vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("listening-port".to_string()),
                    Value::BulkString(port.to_string()),
                ]);
                let replconf2 = Value::Array(vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("capa".to_string()),
                    Value::BulkString("psync2".to_string()),
                ]);
                let psync = Value::Array(vec![
                    Value::BulkString("PSYNC".to_string()),
                    Value::BulkString("?".to_string()),
                    Value::BulkString("-1".to_string()),
                ]);
                let mut handler = resp::RespHandler::new(master_stream);
                handler.write_value(ping).await.unwrap();
                handler.read_values().await.unwrap(); // PONG
                handler.write_value(replconf1).await.unwrap();
                handler.read_values().await.unwrap(); // OK
                handler.write_value(replconf2).await.unwrap();
                handler.read_values().await.unwrap(); // OK
                handler.write_value(psync).await.unwrap();
                // let sync = handler.read_values().await; // FULLRSYNC
                // println!("FULLRESYNC_command: {:?}", sync);
                //let rdb = handler.read_file().await.unwrap(); // rdb file
                //println!("got rdb file: {:?}", String::from_utf8_lossy(&rdb.unwrap()));
                handle_stream(handler.stream, redis_clone).await
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
                // let shared_stream = Arc::new(Mutex::new(_stream));
                tokio::spawn(async move { handle_stream(_stream, redis_clone).await });
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
    // slaves: Mutex<Vec<Replica>>,
) -> Result<()> {
    //let mut handler = resp::RespHandler::new(stream);

    let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();
    let mut handler = resp::RespHandler::new(stream);
    let mut offset = 0;

    loop {
        // let value = handler.read_value().await.unwrap();
        tokio::select! {
            values = handler.read_values() => {
                //let mut responses = vec![];
                if let Ok(Some(v)) = values {
                    println!("[receive values]: {:?}", v);
                    for value in v {
                        let (command, args, len) = extract_command(value.clone()).unwrap_or(("Extract Failed".to_owned(), vec![], 0));
                        offset += len;
                        let response = match command.to_lowercase().as_str() {
                            "ping" => Value::SimpleString("PONG".to_string()),
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
                                let res = if redis_clone.lock().await.is_slave() {
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
                            "info" => Value::BulkString(redis_clone.lock().await.info()),
                            "replconf" => {
                                let res = match args[0].clone().decode().to_lowercase().as_str() {
                                "listening-port" => {
                                    // slaves.lock().await.push(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                                    // let framed = Framed::new(handler.stream, LengthDelimitedCodec::new());
                                    redis_clone.lock().await.add_slave(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                                    Value::SimpleString("OK".to_string())

                                }
                                "getack" => {
                                    if redis_clone.lock().await.is_slave() {
                                        Value::Array(vec![
                                            Value::BulkString("REPLCONF".to_string()),
                                            Value::BulkString("ACK".to_string()),
                                            Value::BulkString(offset.to_string()),
                                        ])
                                    } else {
                                        Value::Empty
                                    }
                                }
                                _ => unimplemented!()
                            };
                                res
                            }
                            "psync" => {
                                handler
                                    .write_value(Value::SimpleString(format!(
                                        "FULLRESYNC {} 0",
                                        redis_clone.lock().await.master_replid.clone().unwrap()
                                    )))
                                    .await
                                    .unwrap();
                                handler
                                    .write_bytes(format!("${}\r\n", empty_rdb().len()).as_bytes())
                                    .await
                                    .unwrap();
                                handler.write_bytes(&empty_rdb()).await.unwrap();
                                Value::Empty
                                //Value::SYNC(redis_clone.lock().await.master_replid.clone().unwrap())
                            }

                            _ => {
                                println!("unknown command, {} : {:?}", command, args);
                                Value::Empty
                            },
                        };
                        //responses.push(response);
                        handler.write_value(response).await.unwrap();
                    };
                    // for response in responses {
                    // }
                    println!("response finished");
            };


            }
            cmd = rx.recv() => {
                    if let Some(cmd) = cmd {
                        println!("receiving cmd from master, {:?}", cmd);
                        handler.stream.write_all(cmd.as_ref()).await?;
                        handler.stream.flush().await?;
                        offset += cmd.len();
                        // handler.write_bytes(cmd.as_ref()).await.unwrap();
                    }

            }
        }
    }
}

async fn _handle_stream_to_master(
    stream: TcpStream,
    redis_clone: Arc<Mutex<redis::Redis>>,
    // slaves: Mutex<Vec<Replica>>,
) -> Result<()> {
    //let mut handler = resp::RespHandler::new(stream);

    let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();
    let mut handler = resp::RespHandler::new(stream);

    loop {
        // let value = handler.read_value().await.unwrap();
        tokio::select! {
            values = handler.read_values() => {
                //let mut responses = vec![];
                if let Ok(Some(v)) = values {
                    println!("values: {:?}", v);
                    for value in v {
                        let (command, args) = extract_command(value.clone()).unwrap_or(("".to_owned(), vec![]));
                        let response = match command.to_lowercase().as_str() {
                            "ping" => Value::SimpleString("PONG".to_string()),
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
                                let res = if redis_clone.lock().await.is_slave() {
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
                            "info" => Value::BulkString(redis_clone.lock().await.info()),
                            "replconf" => {
                                let res = match args[0].clone().decode().to_lowercase().as_str() {
                                "listening-port" => {
                                    // slaves.lock().await.push(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                                    // let framed = Framed::new(handler.stream, LengthDelimitedCodec::new());
                                    redis_clone.lock().await.add_slave(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                                    Value::SimpleString("OK".to_string())

                                }
                                "getack" => {
                                    if redis_clone.lock().await.is_slave() {
                                        Value::Array(vec![
                                            Value::BulkString("REPLCONF".to_string()),
                                            Value::BulkString("ACK".to_string()),
                                            Value::BulkString("0".to_string()),
                                        ])
                                    } else {
                                        Value::Empty
                                    }
                                }
                                _ => unimplemented!()
                            };
                                res
                            }
                            "psync" => {
                                // handler
                                //     .write_value(Value::SimpleString(format!(
                                //         "FULLRESYNC {} 0",
                                //         redis_clone.lock().await.master_replid.clone().unwrap()
                                //     )))
                                //     .await
                                //     .unwrap();
                                // handler
                                //     .write_bytes(format!("${}\r\n", empty_rdb().len()).as_bytes())
                                //     .await
                                //     .unwrap();
                                // handler.write_bytes(&empty_rdb()).await.unwrap();

                                Value::SYNC(redis_clone.lock().await.master_replid.clone().unwrap())
                            }

                            _ => Value::Empty,
                        };
                        handler.write_value(response).await.unwrap();
                        //responses.push(response);
                    };
                    // for response in responses {
                    // }
                    println!("response finished");
            };


            }
            cmd = rx.recv() => {
                    if let Some(cmd) = cmd {
                        println!("receiving cmd from master, {:?}", cmd);
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

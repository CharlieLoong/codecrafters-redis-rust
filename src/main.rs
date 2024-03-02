#[allow(dead_code)]
mod command;
mod rdb;
mod redis;
mod resp;

use std::{
    env::args,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use rdb::empty_rdb;
use redis::Replica;
// use clap::Parser;
use resp::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};


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
        if let Ok(mut master_stream) = TcpStream::connect(SocketAddrV4::new(
            master_host_ipv4.unwrap(),
            master_port.unwrap(),
        ))
        .await
        {
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
            handler.write_value(replconf1).await.unwrap();
            handler.write_value(replconf2).await.unwrap();
            handler.write_value(psync).await.unwrap();
            let redis_clone = Arc::clone(&redis);
            tokio::spawn(async move { handle_stream(handler.stream, redis_clone).await });
            // while let Some(_) = handler.read_bytes().await.unwrap() {}
            // let _ = handler.stream.shutdown();
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

// async fn _handshake_to_master(master_socket: SocketAddr, port: u16) {
//     if let Ok(mut stream) = TcpStream::connect(master_socket).await {
//         let ping = Value::Array(vec![Value::BulkString("PING".to_string())]);
//         let replconf1 = Value::Array(vec![
//             Value::BulkString("REPLCONF".to_string()),
//             Value::BulkString("listening-port".to_string()),
//             Value::BulkString(port.to_string()),
//         ]);
//         let replconf2 = Value::Array(vec![
//             Value::BulkString("REPLCONF".to_string()),
//             Value::BulkString("capa".to_string()),
//             Value::BulkString("psync2".to_string()),
//         ]);
//         let psync = Value::Array(vec![
//             Value::BulkString("PSYNC".to_string()),
//             Value::BulkString("?".to_string()),
//             Value::BulkString("-1".to_string()),
//         ]);
//         let mut handler = resp::RespHandler::new(stream, );
//         handler.write_value(ping).await.unwrap();
//         handler.write_value(replconf1).await.unwrap();
//         handler.write_value(replconf2).await.unwrap();
//         handler.write_value(psync).await.unwrap();
//         while let Some(_) = handler.read_bytes().await.unwrap() {}
//     } else {
//         eprintln!("Could not connect to replication master");
//     }
// }

async fn handle_stream(
    stream: TcpStream,
    redis_clone: Arc<Mutex<redis::Redis>>,
    // slaves: Mutex<Vec<Replica>>,
) -> Result<()> {
    //let mut handler = resp::RespHandler::new(stream);

    let (tx, mut rx) = mpsc::unbounded_channel::<BytesMut>();

    //let shared_stream = Arc::new(Mutex::new(stream.));
    let mut handler = resp::RespHandler::new(stream);
    loop {
        // let value = handler.read_value().await.unwrap();
        tokio::select! {
            value = handler.read_value() => {
                let response: Value = if let Ok(Some(v)) = value {
                println!("value: {:?}", v);
                let (command, args) = extract_command(v.clone()).unwrap_or(("".to_owned(), vec![]));
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_string()),
                    "echo" => args.first().unwrap().clone(),
                    "set" => {
                        println!("{:?}",args);
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
                        // let res = if redis_clone.lock().await.master_port.unwrap_or(0) == handler.stream.peer_addr().unwrap().port() {
                        //     Value::Empty
                        // } else {
                        //     Value::SimpleString("OK".to_string())
                        // };

                        Value::SimpleString("OK".to_string())
                    }
                    "get" => {
                        let key = unpack_bulk_str(args[0].clone()).unwrap();
                        match redis_clone.lock().await.get(key) {
                            Some(val) => Value::BulkString(val),
                            None => Value::BulkString("".to_string()),
                        }
                    }
                    "info" => Value::BulkString(redis_clone.lock().await.info()),
                    "replconf" => {
                        if unpack_bulk_str(args[0].clone()).unwrap() == "listening-port" {
                            // slaves.lock().await.push(Replica { port: args[1].clone().decode() , channel: tx.clone() });
                            // let framed = Framed::new(handler.stream, LengthDelimitedCodec::new());
                            redis_clone.lock().await.add_slave(Replica { port: args[1].clone().decode() , channel: tx.clone() })
                        }
                        Value::SimpleString("OK".to_string())
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
                    }

                    _ => Value::Empty,
                }
            } else {
                Value::Empty
            };
            handler.write_value(response).await.unwrap();

            }
            cmd = rx.recv() => {
                    if let Some(cmd) = cmd {
                        println!("receiving cmd from master, {:?}", cmd);
                        handler.stream.write_all(cmd.as_ref()).await.unwrap();
                        handler.stream.flush().await?;
                        // handler.write_bytes(cmd.as_ref()).await.unwrap();
                    }

            }
        }

        // let response = if let Some(v) = value {
        //     let (command, args) = extract_command(v.clone()).unwrap();
        //     match command.to_lowercase().as_str() {
        //         "ping" => Value::SimpleString("PONG".to_string()),
        //         "echo" => args.first().unwrap().clone(),
        //         "set" => {
        //             let key = unpack_bulk_str(args[0].clone()).unwrap();
        //             let val = unpack_bulk_str(args[1].clone()).unwrap();
        //             let expr = if args.len() >= 4 {
        //                 Some(Duration::from_millis(
        //                     unpack_bulk_str(args[3].clone())
        //                         .unwrap()
        //                         .parse()
        //                         .expect("invalid expr format"),
        //                 ))
        //             } else {
        //                 None
        //             };
        //             redis_clone
        //                 .lock()
        //                 .await
        //                 .set(key, redis::RedisValue::String(val), expr)
        //                 .await;
        //             for replica in slaves.iter() {
        //                 let _ = replica.channel.send(Bytes::from(v.to_owned().serialize()));
        //             }
        //             Value::SimpleString("OK".to_string())
        //         }
        //         "get" => {
        //             let key = unpack_bulk_str(args[0].clone()).unwrap();
        //             match redis_clone.lock().await.get(key) {
        //                 Some(val) => Value::BulkString(val),
        //                 None => Value::BulkString("".to_string()),
        //             }
        //         }
        //         "info" => Value::BulkString(redis_clone.lock().await.info()),
        //         "replconf" => {
        //             if unpack_bulk_str(args[0].clone()).unwrap() == "listening-port" {
        //                 slaves.push(Replica {
        //                     port: args[1].clone().decode(),
        //                     channel: tx.clone(),
        //                 });
        //                 redis_clone.lock().await.add_slave(Replica {
        //                     port: args[1].clone().decode(),
        //                     channel: tx.clone(),
        //                 })
        //             }
        //             Value::SimpleString("OK".to_string())
        //         }
        //         "psync" => {
        //             handler
        //                 .write_value(Value::SimpleString(format!(
        //                     "FULLRESYNC {} 0",
        //                     redis_clone.lock().await.master_replid.clone().unwrap()
        //                 )))
        //                 .await
        //                 .unwrap();
        //             handler
        //                 .write_bytes(format!("${}\r\n", empty_rdb().len()).as_bytes())
        //                 .await
        //                 .unwrap();
        //             handler.write_bytes(&empty_rdb()).await.unwrap();

        //             Value::Empty
        //         }

        //         _ => panic!("Unknown command"),
        //     }
        // } else {
        //     break;
        // };
        // handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
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

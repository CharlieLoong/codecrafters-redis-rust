mod redis;
mod resp;

use std::{
    env::args,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Result};
// use clap::Parser;
use resp::Value;
use tokio::net::{TcpListener, TcpStream};

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
    let mut master_port = None;

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

    let redis = if role == Role::Master {
        redis::Redis::new()
    } else {
        redis::Redis::slave(format!("{}:{}", master_host.unwrap(), master_port.unwrap()))
    };
    let redis = Arc::new(Mutex::new(redis));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to bind");
    println!("listening on port {}", port);

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut _stream, _)) => {
                println!("accepted new connection");
                //handle_stream(&mut _stream);
                let redis_clone = Arc::clone(&redis);
                tokio::spawn(async move { handle_stream(_stream, redis_clone).await });
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

async fn handle_stream(stream: TcpStream, redis_clone: Arc<Mutex<redis::Redis>>) {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_value().await.unwrap();

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => {
                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    let value = unpack_bulk_str(args[1].clone()).unwrap();
                    let expr = if args.len() >= 4 {
                        Some(Duration::from_millis(
                            unpack_bulk_str(args[3].clone()).unwrap().parse().unwrap(),
                        ))
                    } else {
                        None
                    };
                    redis_clone
                        .lock()
                        .unwrap()
                        .set(key, redis::RedisValue::String(value), expr);
                    Value::SimpleString("OK".to_string())
                }
                "get" => {
                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    match redis_clone.lock().unwrap().get(key) {
                        Some(val) => Value::BulkString(val),
                        None => Value::BulkString("".to_string()),
                    }
                }
                "info" => Value::BulkString(redis_clone.lock().unwrap().info()),
                _ => panic!("Unknown command"),
            }
        } else {
            break;
        };
        handler.write_value(response).await.unwrap();
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

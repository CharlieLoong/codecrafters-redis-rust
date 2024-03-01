// pub enum Command {
//     Ping,
//     Echo(String),
//     Set(String, String),
//     Get(String),
//     Info,
//     Replconf(String),
// }

use std::time::Duration;

use anyhow::{anyhow, Result};

use crate::resp::Value;

// impl Command {
//     pub fn to_string(self) -> String {
//         match self {
//             Command::Ping => format!("PING"),
//             Command::Echo(s) => format!("ECHO {}", s),
//             Command::Set(k, v) => format!("SET {} {}", k, v),
//             Command::Get(k) => format!("GET {}", k),
//             Command::Info => format!("INFO"),
//             Command::Replconf(s) => format!("REPLCONF {}", s),
//         }
//     }
// }
pub struct SET {
    key: String,
    value: String,
    expr: Option<Duration>,
}
#[allow(dead_code)]
impl SET {
    pub fn new(key: String, value: String, expr: Option<Duration>) -> SET {
        SET { key, value, expr }
    }

    pub fn parse(args: Vec<Value>) -> SET {
        let key = unpack_bulk_str(args[0].clone()).unwrap();
        let value = unpack_bulk_str(args[1].clone()).unwrap();
        let expr = if args.len() >= 4 {
            Some(Duration::from_millis(
                unpack_bulk_str(args[3].clone()).unwrap().parse().unwrap(),
            ))
        } else {
            None
        };
        SET { key, value, expr }
    }

    pub fn serialize(self) -> Value {
        let mut arr = vec![
            Value::BulkString("SET".into()),
            Value::BulkString(self.key.into()),
            Value::BulkString(self.value.into()),
        ];
        match self.expr {
            Some(expr) => {
                arr.push(Value::BulkString("EX".into()));
                arr.push(Value::BulkString(expr.as_millis().to_string()));
            }
            None => {}
        }
        Value::Array(arr)
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow!("Expect bulk string")),
    }
}

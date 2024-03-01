use anyhow::Result;
use anyhow::{anyhow, Ok};
#[allow(dead_code)]
use bytes::BytesMut;
use bytes::{BufMut, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct RespHandler {
    pub stream: TcpStream,
    buffer: BytesMut,
    // pub receiver: UnboundedReceiver<BytesMut>,
}

#[derive(Debug, Clone)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    File(Vec<u8>),
    Multiple(Vec<Value>),
    Empty,
}
impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => match s.len() {
                0 => format!("$-1\r\n"),
                _ => format!("${}\r\n{}\r\n", s.chars().count(), s),
            },
            Value::Array(a) => {
                let mut s = String::new();
                for v in &a {
                    s.push_str(&<Value as Clone>::clone(&v).serialize());
                }
                format!("*{}\r\n{}", a.len(), s)
            }
            Value::File(f) => {
                format!("${}\r\n{}", f.len(), hex::encode(&f))
            }
            Value::Multiple(mut values) => {
                values.insert(0, Value::BulkString("Multiple".to_string()));
                values
                    .iter()
                    .map(|v| <Value as Clone>::clone(&v).serialize())
                    .collect()
            }
            _ => "".to_string(),
        }
    }
    pub fn decode(self) -> String {
        match self {
            Value::BulkString(s) => s,
            Value::SimpleString(s) => s,
            _ => unimplemented!(),
        }
    }
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(512),
            // receiver,
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = if !self.buffer.is_empty() { self.buffer.len() } else { self.stream.read_buf(&mut self.buffer).await? };
        if bytes_read == 0 {
            return Ok(None);
        }
        let bytes = self.buffer.split();
        let (v, consumed) = parse_message(bytes.clone())?;
        println!("consumed: {}, read: {}", consumed, bytes_read);
        if bytes_read - consumed > 0 {
            self.buffer.put(&bytes[consumed..]);
        }
        // bytes_read -= consumed;
        // if bytes_read == 0 {
        //     return Ok(Some(v));
        // }
        // let mut vec = vec![v];
        // while bytes_read > 0 {
        //     let (v, consumed) = parse_message(self.buffer.split())?;
        //     bytes_read -= consumed;
        //     vec.push(v);
        // }
        // Ok(Some(Value::Multiple(vec)))
        Ok(Some(v))
    }

    pub async fn read_bytes(&mut self) -> Result<Option<usize>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        Ok(Some(bytes_read))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write(bytes).await?;
        Ok(())
    }

    async fn handshake(&mut self, port: u16) {
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
        self.write_value(ping).await.unwrap();
        self.write_value(replconf1).await.unwrap();
        self.write_value(replconf2).await.unwrap();
        self.write_value(psync).await.unwrap();
    }
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    const CR: u8 = b'\r';
    const LF: u8 = b'\n';
    for i in 1..buffer.len() {
        if buffer[i - 1] == CR && buffer[i] == LF {
            return Some((&buffer[..(i - 1)], i + 1));
        }
    }

    None
}

fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow!("Invalid first byte".to_string())),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        return Ok((Value::SimpleString(string), len + 1));
    }
    Err(anyhow!("Invalid string".to_string()))
}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (array_length, mut bytes_consumed) =
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let array_length = parse_int(line)?;

            (array_length, len + 1)
        } else {
            return Err(anyhow!("Invalid array format".to_string()));
        };
    let mut items = vec![];
    for _ in 0..array_length {
        let (item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(item);
        bytes_consumed += len;
    }
    Ok((Value::Array(items), bytes_consumed))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let array_length = parse_int(line)?;

        (array_length, len + 1)
    } else {
        return Err(anyhow!("Invalid array format".to_string()));
    };
    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((
        Value::BulkString(String::from_utf8(
            buffer[bytes_consumed..end_of_bulk_str].to_vec(),
        )?),
        total_parsed,
    ))
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

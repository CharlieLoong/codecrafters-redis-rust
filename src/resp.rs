use std::time::Duration;

use anyhow::anyhow;
#[allow(dead_code, unused)]
use anyhow::Result;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use crate::rdb::empty_rdb;

pub struct RespHandler {
    pub stream: TcpStream,
    buffer: BytesMut,
    // pub receiver: UnboundedReceiver<BytesMut>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    File(Vec<u8>),
    Multiple(Vec<Value>),
    Empty,
    Null,
    SYNC(String),
    Integers(i64),
    Error(String)
}
impl Value {
    pub fn serialize(&self) -> BytesMut {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s).as_bytes().into(),
            Value::BulkString(s) => match s.len() {
                0 => format!("$-1\r\n").as_bytes().into(),
                _ => format!("${}\r\n{}\r\n", s.chars().count(), s)
                    .as_bytes()
                    .into(),
            },
            Value::Array(a) => {
                let mut s = vec![];
                for v in a {
                    s.push(v.serialize());
                }
                format!("*{}\r\n{}", a.len(), String::from_utf8(s.concat()).unwrap())
                    .as_bytes()
                    .into()
            }
            Value::File(f) => [format!("${}\r\n", f.len()).as_bytes(), f].concat()[..].into(),
            Value::Null => format!("$-1\r\n").as_bytes().into(),
            Value::Empty => BytesMut::new(),
            Value::SYNC(id) => [
                Value::SimpleString(format!("FULLRESYNC {} 0", id)).serialize(),
                format!("${}\r\n", empty_rdb().len()).as_bytes().into(),
                BytesMut::from(&empty_rdb()[..]),
            ]
            .concat()[..]
                .into(),
            Value::Multiple(_) => todo!(),
            Value::Integers(i) => format!(":{}\r\n", i).as_bytes().into(),
            Value::Error(e) => format!("-Error {}\r\n", e).as_bytes().into(),
            _ => unimplemented!(),
        }
    }
    pub fn decode(self) -> String {
        match self {
            Value::BulkString(s) => s,
            Value::SimpleString(s) => s,
            Value::Null => "nil".to_owned(),
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

    pub async fn read_values(&mut self) -> Result<Option<Vec<Value>>> {
        let mut bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let mut bytes = self.buffer.split();
        let mut values = vec![];
        while bytes_read > 0 {
            let (v, consumed) = parse_message(bytes.clone())?;
            //println!("consumed: {}, read: {}", consumed, bytes_read);
            values.push(v);
            bytes.advance(consumed);
            bytes_read -= consumed;
        }
        //println!("read finished");
        Ok(Some(values))
    }

    pub async fn read_bytes(&mut self) -> Result<Option<BytesMut>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        Ok(Some(self.buffer.clone()))
    }

    pub async fn read_file(&mut self) -> Result<Option<BytesMut>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (file_len, bytes_consumed) =
            if let Some((line, len)) = read_until_crlf(&self.buffer[1..]) {
                let file_len = parse_int(line)?;

                (file_len, len + 1)
            } else {
                return Err(anyhow!("Invalid file format".to_string()));
            };
        Ok(Some(
            self.buffer[bytes_consumed..bytes_consumed + file_len as usize].into(),
        ))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        if value.clone().serialize().len() == 0 {
            return Ok(());
        }
        println!("write: {:?}", value.clone().serialize());
        self.stream.write(&value.serialize()).await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write(bytes).await?;
        Ok(())
    }

    pub async fn _handshake(&mut self, port: u16) {
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
        //sleep(Duration::from_millis(2500)).await;
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

fn parse_integer(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let integer = parse_int(line)?;
        return Ok((Value::Integers(integer), len + 1));
    }
    Err(anyhow!("Invalid integer format".to_string()))
}
fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        ':' => parse_integer(buffer),
        _ => Err(anyhow!("Invalid first byte".to_string())),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        //println!("string: {}", string);
        // if string.starts_with("FULLRESYNC") {
        //     let args: Vec<_> = string.split(' ').collect();
        //     let id = args[1];
        //     if let Ok((_file, file_len)) = parse_file(buffer.split_off(len)) {
        //         return Ok((Value::SYNC(id.to_owned()), file_len + 1));
        //     } else {
        //         return Err(anyhow!("parse file failed".to_string()));
        //     }
        // }
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
    if bulk_str_len < 0 {
        return Ok((Value::Null, bytes_consumed + 2));
    }
    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    //println!("{:?}", &buffer[end_of_bulk_str..end_of_bulk_str + 2]);
    if buffer.len() >= end_of_bulk_str + 2
        && &buffer[end_of_bulk_str..end_of_bulk_str + 2] == b"\r\n"
    {
        Ok((
            Value::BulkString(String::from_utf8(
                buffer[bytes_consumed..end_of_bulk_str].to_vec(),
            )?),
            end_of_bulk_str + 2,
        ))
    } else {
        Ok((
            Value::File(buffer[bytes_consumed..end_of_bulk_str].to_vec()),
            end_of_bulk_str,
        ))
    }
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

pub fn _parse_file(buffer: BytesMut) -> Result<(BytesMut, usize)> {
    println!("parse file: {:?}", String::from_utf8_lossy(&buffer));
    let (file_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let file_len = parse_int(line)?;

        (file_len, len + 1)
    } else {
        return Err(anyhow!("Invalid file format".to_string()));
    };

    Ok((
        buffer[bytes_consumed..bytes_consumed + file_len as usize].into(),
        file_len as usize,
    ))
}

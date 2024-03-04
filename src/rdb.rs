//use hex;

use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::Path,
    time::{Duration, SystemTime},
};

use crate::redis::{Item, RedisValue};


const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty_rdb() -> Vec<u8> {
    hex::decode(EMPTY_RDB).unwrap()
}

pub struct RdbReader {
    _comments: String,
    _storage: HashMap<String, Item>,
    _db_selector: usize,
}


impl RdbReader {
    pub fn read_header(&mut self, s: &[u8]) -> Option<usize> {
        let mut probe = 0;
        while s[probe] != 0xFE && probe < s.len() {
            probe += 1
        }
        println!("skipping {} bytes", probe);
        // TODO: Implementing Boundary/Validity check
        self._db_selector = s[probe + 1] as usize;
        Some(probe + 5)
    }
    pub fn read_data(&mut self, s: &[u8], index: usize) -> Option<usize> {
        if index >= s.len() {
            return None;
        }
        // TODO: Implement parser for other type of data
        if s[index] == 0xFF {
            return None;
        }
        let mut index = index + 1;
        let key;
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            key = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new key {}", key);
            index = nindex + length;
        } else {
            return None;
        }
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            let value = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new value {}", value);
            self._storage.insert(
                key,
                Item {
                    value: RedisValue::String(value),
                    expire: SystemTime::now() + Duration::from_secs(60)
                },
            );
            Some(nindex + length)
        } else {
            None
        }
    }

    pub fn parse_length_encoding(&mut self, s: &[u8], index: usize) -> Option<(usize, usize)> {
        if index >= s.len() {
            return None;
        }
        println!("Reading length {}", s[index]);
        if s[index] == 0xFF {
            return None;
        }
        if s[index] < 64 {
            return Some((index + 1, s[index] as usize));
        }
        // TODO: Rightnow only implementing length-coding case one
        //if s[0] < 128 {
        //    return Some((&s[2..], (s[0] % 64 * 256 + s[1]) as usize));
        //}
        None
    }

    pub fn read_from_file(file_path: String) -> HashMap<String, Item> {
        let path = Path::new(&file_path);
        println!("{}", path.display());
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(_err) => {
                return HashMap::new()
            }
        };
        let mut data = vec![];
        if file.read_to_end(&mut data).is_ok() {
            println!("{}", &data.len());
        } else {
            panic!("Cannot open file");
        }
        let data: &[u8] = &data;
        let mut rdb_reader = RdbReader {
            _comments: file_path.to_string(),
            _storage: HashMap::new(),
            _db_selector: 0,
        };
        let mut res = rdb_reader.read_header(data);
        while let Some(index) = res {
            res = rdb_reader.read_data(data, index);
        }
        rdb_reader._storage
    }
}

// pub enum Command {
//     Ping,
//     Echo(String),
//     Set(String, String),
//     Get(String),
//     Info,
//     Replconf(String),
// }

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

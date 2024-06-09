use std::error::Error;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::types::Message;

#[derive(Debug)]
pub enum Command {
    CreateTopic(Bytes),
    DeleteTopic(Bytes),
    SelectTopic(Bytes),
    SetReadOffset(usize),
    ReadMessage,
    PublishMessage(Message),
    InvalidCommand,
}


#[derive(Debug)]
pub struct Connection {
    stream: BufReader<TcpStream>,
}


#[derive(Debug)]
pub enum Response {
    Positive(String),
    Negative(String),
}


impl From<Bytes> for Command {
    fn from(value: Bytes) -> Self {
        if value.len() < 2 {
            return Self::InvalidCommand;
        }
        
        let action_byte = *value.get(0).unwrap();
        let data_end = value.len() - 1;

        if value.get(data_end).unwrap() != &b'\n' {
            return Self::InvalidCommand;
        }


        let data = value.slice(1..data_end);

        match action_byte {
            b'#' => Self::CreateTopic(data),
            b'!' => Self::DeleteTopic(data),
            b'@' => Self::SelectTopic(data),
            b'>' => Self::PublishMessage(Message::from(value.slice(1..))),
            b'$' => {
                if let Ok(offset) = String::from_utf8_lossy(&data.to_vec()).parse::<usize>() {
                    Self::SetReadOffset(offset)
                } else {
                    Self::InvalidCommand
                }
            },
            b'<' if data.len() == 0 => Self::ReadMessage,
            _ => Self::InvalidCommand,
        }
    }
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream: BufReader::new(stream) }
    }

    pub async fn read_command(&mut self) -> Result<Command, Box<dyn Error>> {
        let mut line = String::new();
        self.stream.read_line(&mut line).await?;

        Ok(Command::from(Bytes::from(line)))
    }

    pub async fn write_response(&mut self, response: Response) -> Result<(), Box<dyn Error>> {
        self.stream.write_all(&response.as_vec_of_u8()).await?;

        Ok(())
    }
}

impl Response {
    pub fn as_vec_of_u8(self) -> Vec<u8> {
        let mut ans = vec![];
        match self {
            Self::Negative(data) => {
                ans.push(b'-');
                ans.extend_from_slice(data.as_bytes());
            },
            Self::Positive(data) => {
                ans.push(b'+');
                ans.extend_from_slice(data.as_bytes());
            },
        }

        if !ans.ends_with(&[b'\n']) {
            ans.push(b'\n');
        }

        ans
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topic_command_from_bytes_test() {
        let data = Bytes::from_static(b"#foo\n");
        let cmd = Command::from(data);

        if let Command::CreateTopic(topic_name) = cmd {
            assert_eq!(topic_name, Bytes::from_static(b"foo"));
        } else {
            panic!("command should have been parsed as Command::CreateTopic");
        }
    }

    #[test]
    fn delete_topic_command_from_bytes_test() {
        let data = Bytes::from_static(b"!foo\n");
        let cmd = Command::from(data);

        if let Command::DeleteTopic(topic_name) = cmd {
            assert_eq!(topic_name, Bytes::from_static(b"foo"));
        } else {
            panic!("command should have been parsed as Command::DeleteTopic");
        }
    }

    #[test]
    fn select_topic_command_from_bytes_test() {
        let data = Bytes::from_static(b"@foo\n");
        let cmd = Command::from(data);

        if let Command::SelectTopic(topic_name) = cmd {
            assert_eq!(topic_name, Bytes::from_static(b"foo"));
        } else {
            panic!("command should have been parsed as Command::SelectTopic");
        }
    }

    #[test]
    fn set_read_offset_topic_command_from_bytes_test() {
        let data = Bytes::from_static(b"$1001\n");
        let cmd = Command::from(data);

        if let Command::SetReadOffset(offset) = cmd {
            assert_eq!(offset, 1001);
        } else {
            panic!("command should have been parsed as Command::SetReadOffset");
        }
    }

    #[test]
    fn read_message_command_from_bytes_test() {
        let data = Bytes::from_static(b"<\n");
        let cmd = Command::from(data);

        if let Command::ReadMessage = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::ReadMessage");
        }
    }

    #[test]
    fn publish_message_command_from_bytes_test() {
        let data = Bytes::from_static(b">hello world\n");
        let cmd = Command::from(data);

        if let Command::PublishMessage(msg) = cmd {
            assert_eq!(msg, Message::from(Bytes::from_static(b"hello world\n")));
        } else {
            panic!("command should have been parsed as Command::PublishMessage");
        }
    }

    #[test]
    fn invalid_command_test_01() {
        let data = Bytes::from_static(b"inavlid_bytes");

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn invalid_command_test_02() {
        let data = Bytes::from_static(b"#inavlid_bytes");

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn invalid_command_test_03() {
        let data = Bytes::from_static(b"inavlid_bytes\n");

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn invalid_command_test_04() {
        let data = Bytes::from_static(b"$inavlid_offset\n");

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn invalid_command_test_05() {
        let data = Bytes::from_static(b"$123O5\n"); // it's an O not zero

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn invalid_command_test_06() {
        let data = Bytes::from_static(b"<inavlid_read_command\n");

        let cmd = Command::from(data);

        if let Command::InvalidCommand = cmd {
            // pass
        } else {
            panic!("command should have been parsed as Command::InvalidCommand");
        }
    }

    #[test]
    fn test_positive_response() {
        let res = Response::Positive("hello".to_owned());
        assert_eq!(res.as_vec_of_u8(), b"+hello\n");
    }

    #[test]
    fn test_negative_response() {
        let res = Response::Negative("hello".to_owned());
        assert_eq!(res.as_vec_of_u8(), b"-hello\n");
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RespValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub(crate) fn serialize(&self) -> String {
        match self {
            Self::SimpleString(s) => format!("+{}\r\n", s),
            Self::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Self::Array(list) => format!(
                "*{}\r\n{}",
                list.len(),
                list.iter()
                    .map(|elem| elem.serialize())
                    .collect::<Vec<_>>()
                    .join("")
            ),
        }
    }

    pub(crate) fn as_string(&self) -> Option<&String> {
        match self {
            Self::BulkString(s) | Self::SimpleString(s) => Some(s),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::resp::RespValue;

    #[test]
    fn test_simple_string() {
        assert_eq!(
            "+OK\r\n".to_string(),
            RespValue::SimpleString("OK".to_string()).serialize()
        );
    }

    #[test]
    fn test_bulk_string() {
        assert_eq!(
            "$5\r\nhello\r\n".to_string(),
            RespValue::BulkString("hello".to_string()).serialize()
        );
    }

    #[test]
    fn test_array() {
        assert_eq!(
            "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_string(),
            RespValue::Array(vec![
                RespValue::BulkString("hello".to_string()),
                RespValue::BulkString("world".to_string())
            ])
            .serialize()
        );
    }
}

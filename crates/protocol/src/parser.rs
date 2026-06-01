use std::fmt;
use crate::Command;

pub enum ParseError {
    NotAnArray,
    InvalidArrayLength,
    InvalidBulkStringLength,
    InvalidUtf8,   
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::NotAnArray => write!(f, "not an array"),
            ParseError::InvalidArrayLength => write!(f, "invalid array length"),
            ParseError::InvalidBulkStringLength => write!(f, "invalid bulk string length"),
            ParseError::InvalidUtf8 => write!(f, "invalid utf8"),
        }
    }
}

pub enum ParseResult {
    Complete(Command, usize),
    Incomplete,
    Error(ParseError),
}

pub fn parse(buf: &[u8]) -> ParseResult {
    match buf.first() {
        None => return ParseResult::Incomplete,
        Some(b) if b != &b'*' => return ParseResult::Error(ParseError::NotAnArray),
        _ => (),
    } 

    // *<num-elements>\r\n<element-1>...<element-n>
    let mut pos: usize = 1;
    let Some(crlf) = find_crlf(&buf[pos..]) else {
        return ParseResult::Incomplete;
    };
    let num_elements = match parse_usize(&buf[pos..(pos+crlf)]) {
        Ok(n) if n > 0 => n,
        _ => return ParseResult::Error(ParseError::InvalidArrayLength),
    };
    pos += crlf + 2;

    let mut elements: Vec<String> = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        if buf.get(pos) != Some(&b'$') {
            return ParseResult::Incomplete;
        }
        pos += 1;

        let Some(crlf) = find_crlf(&buf[pos..]) else {
            return ParseResult::Incomplete;
        };
        let Ok(str_len) = parse_usize(&buf[pos..(pos + crlf)]) else {
            return ParseResult::Error(ParseError::InvalidBulkStringLength);
        };
        pos += crlf + 2;

        // Do we have str_len + 2 (\r\n) bytes?
        if buf.len() < pos + str_len + 2 {
            return ParseResult::Incomplete;
        }

        let s = match std::str::from_utf8(&buf[pos..(pos + str_len)]) {
            Ok(s) => s.to_string(),
            Err(_) => return ParseResult::Error(ParseError::InvalidUtf8),
        };
        pos += str_len + 2;

        elements.push(s);
    }

    // Convert to Command
    let mut iter = elements.into_iter();
    let name = iter.next().unwrap().to_uppercase();
    let args: Vec<String> = iter.collect();

    ParseResult::Complete(Command { name, args }, pos)
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|bs| bs == b"\r\n")
}

fn parse_usize(buf: &[u8]) -> Result<usize, ()> {
    std::str::from_utf8(buf)
        .map_err(|_| ())?
        .parse::<usize>()
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to build a RESP bulk string
    fn bulk(s: &str) -> Vec<u8> {
        format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
    }

    // Helper to build RESP array
    fn array(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend(bulk(arg));
        }
        out
    }

    #[test]
    fn should_parse_complete_command() {
        let input = array(&["GET", "shorty"]);

        let ParseResult::Complete(cmd, to_consume) = parse(&input) else {
            panic!("Expected a valid GET command");
        };

        assert_eq!(cmd.name, String::from("GET"));
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0], String::from("shorty"));
        assert_eq!(to_consume, input.len());
    }

    #[test]
    fn should_parse_first_of_two_commands() {
        let first = array(&["SET", "foo", "bar"]);
        let second = array(&["GET", "foo"]);
        let mut input = first.clone();
        input.extend(&second);

        let ParseResult::Complete(cmd, to_consume) = parse(&input) else {
            panic!("Expected a valid SET command");
        };

        assert_eq!(cmd.name, String::from("SET"));
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(to_consume, first.len());
    }

    #[test]
    fn should_be_incomplete_mid_bulk_header() {
        // array header + first bulk string header, no data
        let ParseResult::Incomplete = parse(b"*2\r\n$3\r\n") else {
            panic!("Expected Incomplete");
        };
    }

    #[test]
    fn should_be_incomplete_with_first_element_only() {
        // array claims 2 elements, only first is present
        let ParseResult::Incomplete = parse(b"*2\r\n$3\r\nGET\r\n") else {
            panic!("Expected Incomplete");
        };
    }

    #[test]
    fn should_be_incomplete_with_partial_bulk_string_data() {
        // second element header present but data truncated
        let ParseResult::Incomplete = parse(b"*2\r\n$3\r\nGET\r\n$6\r\nshor") else {
            panic!("Expected Incomplete");
        };
    }

    #[test]
    fn should_see_empty_input_as_incomplete() {
        let ParseResult::Incomplete = parse(b"") else {
            panic!("Expected Incomplete on empty input");
        };
    }

    #[test]
    fn should_err_on_invalid_resp() {
        // not a RESP array — plain text like old protocol
        let res = parse(b"GET foo\r\n");
        assert!(matches!(res, ParseResult::Error(ParseError::NotAnArray)));
    }

    #[test]
    fn should_err_on_zero_element_array() {
        // *0 is technically valid RESP but not a valid command
        let res = parse(b"*0\r\n");
        assert!(matches!(res, ParseResult::Error(ParseError::InvalidArrayLength)));
    }

    #[test]
    fn should_err_on_bad_bulk_string_length() {
        let res = parse(b"*1\r\n$-1\r\nPING\r\n");
        assert!(matches!(res, ParseResult::Error(ParseError::InvalidBulkStringLength)));
    }

    #[test]
    fn should_err_on_invalid_utf8() {
        let res = parse(b"*1\r\n$1\r\n\xFF\r\n");
        assert!(matches!(res, ParseResult::Error(ParseError::InvalidUtf8)));
    }

    #[test]
    fn should_parse_unknown_command() {
        // Parser accepts any well-formed array of bulk strings.
        // Semantic validation is Database's responsibility, not the parser's.
        let input = array(&["DRINK", "tea", "water"]);
        let ParseResult::Complete(cmd, to_consume) = parse(&input) else {
            panic!("Expected Complete for unknown but well-formed command");
        };
        assert_eq!(cmd.name, "DRINK");
        assert_eq!(cmd.args, vec!["tea", "water"]);
        assert_eq!(to_consume, input.len());
    }
}
use crate::Command;

//pub enum ParseError {
//    
//}

pub enum ParseResult {
    Complete(Command, usize),
    Incomplete,
    Error(String), // TODO: Use ParseError?
}

pub fn parse(buf: &[u8]) -> ParseResult {
    if let Some(pos) = buf.iter().position(|b| *b == b'\n') {
        let line = &buf[..pos];

        let line = match std::str::from_utf8(line) {
            Ok(s) => s.trim(),
            Err(e) => {
                return ParseResult::Error(e.to_string());
            },
        };

        let mut parts = line.split_whitespace();

        let Some(name) = parts.next() else {
            return ParseResult::Error("Empty Command".to_string());
        };

        let cmd = Command {
            name: name.to_uppercase(),
            args: parts.map(str::to_string).collect(),
        };

        ParseResult::Complete(cmd, pos + 1)
    } else {
        ParseResult::Incomplete
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_as_commands() {
        let input = b"GET shorty\nDRINK tea and\nTHEN\nUSE toil";

        let ParseResult::Complete(cmd, to_consume) = parse(input) else {
            panic!("Expected a valid GET command");
        };

        assert_eq!(cmd.name, String::from("GET"));
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0], String::from("shorty"));
        assert_eq!(to_consume, 11);

        let input = b"DRINK tea and\nTHEN\nUSE toil";

        let ParseResult::Complete(cmd, to_consume) = parse(input) else {
            panic!("Expected a valid invalid DRINK command");
        };

        assert_eq!(cmd.name, String::from("DRINK"));
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args, vec!["tea".to_string(), "and".to_string()]);
        assert_eq!(to_consume, 14);

        let input = b"THEN\nUSE toil";

        let ParseResult::Complete(cmd, to_consume) = parse(input) else {
            panic!("Expected a valid invalid THEN command");
        };

        assert_eq!(cmd.name, String::from("THEN"));
        assert_eq!(cmd.args.len(), 0);
        assert_eq!(to_consume, 5);

        let input = b"USE toil";

        let ParseResult::Incomplete = parse(input) else {
            panic!("Expected an Incomplete command");
        };
    }

    #[test]
    fn should_err_on_empty_command() {
        let ParseResult::Error(_) = parse(b"\n") else {
            panic!("Expected an empty command error.");
        };

        let ParseResult::Error(_) = parse(b"  \n") else {
            panic!("Expected an empty command error.");
        };
    }
}
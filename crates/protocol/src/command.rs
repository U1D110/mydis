#[derive(Debug)]
pub struct Command {
    pub name: String,
    pub args: Vec<String>,
}

impl Command {
    pub fn to_resp_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // Start with array length
        bytes.extend_from_slice(format!("*{}\r\n", self.args.len() + 1).as_bytes());

        // Append bulk strings for each array element
        for element in std::iter::once(&self.name).chain(self.args.iter()) {
            bytes.extend_from_slice(format!("${}\r\n", element.len()).as_bytes());
            bytes.extend_from_slice(element.as_bytes());
            bytes.extend_from_slice(b"\r\n");
        }

        bytes
    }
}

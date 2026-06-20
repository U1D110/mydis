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

        // TODO: normalize all expirations to Absolute so the down time before replay is not
        //      counted against Relative expirations.
        //      But then how should we go about handling a key that has expired since that down
        //      time began?

        // Append bulk strings for each array element
        for element in std::iter::once(&self.name).chain(self.args.iter()) {
            bytes.extend_from_slice(format!("${}\r\n", element.len()).as_bytes());
            bytes.extend_from_slice(element.as_bytes());
            bytes.extend_from_slice(b"\r\n");
        }

        bytes
    }
}

use crate::header::Header;
use numext_fixed_uint::U256;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Tip {
    pub header: Header,
    pub total_difficulty: U256,
}

impl Tip {
    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn total_difficulty(&self) -> &U256 {
        &self.total_difficulty
    }
}

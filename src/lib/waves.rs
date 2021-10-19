use bytes::{BufMut, BytesMut};
use sha3::Digest;

pub struct Address( pub String);
pub struct RawPublicKey(pub Vec<u8>);
pub struct RawAddress(pub Vec<u8>);

impl From<(RawAddress, u8)> for Address {
    fn from(data: (RawAddress, u8)) -> Self {
        let (RawAddress(address), chain_id) = data;

        let mut addr = BytesMut::with_capacity(26);

        addr.put_u8(1);
        addr.put_u8(chain_id);
        addr.put_slice(&address[..]);

        let chks = &keccak256(&blake2b256(&addr[..22]))[..4];

        addr.put_slice(chks);

        Address(bs58::encode(addr).into_string())
    }
}

impl From<(RawPublicKey, u8)> for Address {
    fn from(data: (RawPublicKey, u8)) -> Self {
        let (RawPublicKey(pk), chain_id) = data;

        let pkh = keccak256(&blake2b256(&pk));

        let mut addr = BytesMut::with_capacity(26); // VERSION + CHAIN_ID + PKH + checksum

        addr.put_u8(1); // address version is always 1
        addr.put_u8(chain_id);
        addr.put_slice(&pkh[..20]);

        let chks = &keccak256(&blake2b256(&addr[..22]))[..4];

        addr.put_slice(chks);

        Address(bs58::encode(addr).into_string())
    }
}

fn keccak256(message: &[u8]) -> [u8; 32] {
    use sha3::Keccak256;

    let mut hasher = Keccak256::new();

    hasher.input(message);

    hasher.result().into()
}

fn blake2b256(message: &[u8]) -> [u8; 32] {
    use blake2::{
        digest::{Input, VariableOutput},
        VarBlake2b,
    };
    use std::convert::TryInto;

    let mut hasher = VarBlake2b::new(32).unwrap();

    hasher.input(message);

    let mut arr = [0u8; 32];

    hasher.variable_result(|res| arr = res.try_into().unwrap());

    arr
}
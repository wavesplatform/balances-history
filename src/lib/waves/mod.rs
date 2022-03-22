use bytes::{BufMut, BytesMut};
use postgres_derive::{FromSql, ToSql};
use sha3::Digest;
use std::fmt;
use waves_protobuf_schemas::waves::{
    block::Header,
    events::{
        blockchain_updated::{
            append::{BlockAppend, Body, MicroBlockAppend},
            Append, Rollback, Update,
        },
        grpc::SubscribeEvent,
        StateUpdate, TransactionMetadata,
    },
    SignedTransaction,
};

pub struct Address(pub String);
pub struct RawPublicKey(pub Vec<u8>);
pub struct RawAddress(pub Vec<u8>);

pub mod bu;

#[derive(Clone, Debug, PartialEq, ToSql, FromSql)]
#[postgres(name = "blocks_microblocks_block_type")]
pub enum BlockType {
    #[postgres(name = "block")]
    Block,
    #[postgres(name = "microblock")]
    MicroBlock,
    #[postgres(name = "rollback")]
    Rollback,
    EMPTY,
}

impl fmt::Display for BlockType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct BlockchainUpdateInfo {
    pub uid: Option<i64>,
    pub height: Option<u32>,
    pub id: Option<String>,
    pub timestamp: Option<i64>,
    pub reference_block_id: Option<String>,
    pub state_updates: Option<StateUpdate>,
    pub transactions: Vec<SignedTransaction>,
    pub transaction_ids: Vec<Vec<u8>>,
    pub transactions_metadata: Vec<TransactionMetadata>,
    pub transaction_state_updates: Vec<StateUpdate>,
    pub block_type: BlockType,
    pub rollback_data: Option<Rollback>,
}

impl Default for BlockchainUpdateInfo {
    fn default() -> Self {
        Self {
            uid: None,
            height: None,
            id: None,
            timestamp: None,
            reference_block_id: None,
            block_type: BlockType::EMPTY,
            state_updates: None,
            transactions: vec![],
            transaction_ids: vec![],
            transactions_metadata: vec![],
            transaction_state_updates: vec![],
            rollback_data: None,
        }
    }
}

impl From<Option<SubscribeEvent>> for BlockchainUpdateInfo {
    fn from(event: Option<SubscribeEvent>) -> Self {
        let mut block_data = BlockchainUpdateInfo::default();

        match event {
            Some(SubscribeEvent { update: Some(bu) }) => {
                block_data.height = Some(bu.height as u32);
                block_data.id = Some(bs58::encode(&bu.id).into_string());

                match bu.update {
                    Some(Update::Append(Append {
                        state_update,
                        body,
                        transaction_ids,
                        transactions_metadata,
                        transaction_state_updates,
                    })) => {
                        block_data.state_updates = state_update;
                        block_data.transaction_ids = transaction_ids;
                        block_data.transactions_metadata = transactions_metadata;
                        block_data.transaction_state_updates = transaction_state_updates;

                        match body {
                            Some(Body::Block(BlockAppend { block, .. })) => {
                                block_data.block_type = BlockType::Block;

                                let block = block.unwrap();

                                match &block.header {
                                    Some(Header {
                                        reference,
                                        timestamp,
                                        ..
                                    }) => {
                                        block_data.timestamp = Some(*timestamp);
                                        block_data.reference_block_id =
                                            Some(bs58::encode(&reference).into_string());
                                    }
                                    None => {}
                                }

                                block_data.transactions = block.transactions;
                            }
                            Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                                block_data.block_type = BlockType::MicroBlock;

                                let micro_block = micro_block.unwrap();
                                block_data.id =
                                    Some(bs58::encode(&micro_block.total_block_id).into_string());

                                block_data.transactions =
                                    micro_block.micro_block.unwrap().transactions;
                            }
                            _ => {}
                        }
                    }
                    Some(Update::Rollback(r)) => {
                        block_data.rollback_data = Some(r);
                        block_data.block_type = BlockType::Rollback;
                    }
                    None => {}
                }
            }
            _ => {}
        };

        block_data
    }
}

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

    let mut hasher = VarBlake2b::new(32).unwrap();

    hasher.input(message);

    let mut arr = [0u8; 32];

    hasher.variable_result(|res| arr = res.try_into().unwrap());

    arr
}

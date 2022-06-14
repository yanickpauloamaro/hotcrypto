use bytes::Bytes;
use crypto::{PublicKey, Signature, Hash as Digestable, Digest};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::hash::Hash;
use std::cmp::{Ord, Ordering};

use move_core_types::{
    account_address::AccountAddress,
    value::MoveValue,
    transaction_argument::TransactionArgument
};

pub type Currency = u64;
pub const CONST_INITIAL_BALANCE: Currency = 5_000_000;
#[cfg(feature = "benchmark")]
pub const NORMAL_TX_AMOUNT: Currency = 1;
#[cfg(feature = "benchmark")]
pub const SAMPLE_TX_AMOUNT: Currency = 2;

// ------------------------------------------------------------------------
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct Account {
    pub public_key: PublicKey,
    pub address: AccountAddress
}

impl Account {
    pub const PUBLIC_KEY_LENGTH: usize = 32;
    pub const ACCOUNT_ADDRESS_LENGTH: usize = 16;

    pub fn new(public_key: PublicKey) -> Self {
        let mut array = [0u8; AccountAddress::LENGTH];
        array.copy_from_slice(&public_key.0[Account::ACCOUNT_ADDRESS_LENGTH..]);
        let address = AccountAddress::new(array);

        Self { public_key, address }
    }
}

impl AsRef<[u8]> for Account {
    fn as_ref(&self) -> &[u8] {
        self.public_key.as_ref()
    }
}

// ------------------------------------------------------------------------
pub type Nonce = u64;
pub trait Nonceable {
    fn get_nonce(&self) -> u64;
}

// ------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Hash, Debug)]
pub enum RequestType {
    Balance
}

impl RequestType {
    fn ordinal(&self) -> u8 {
        return match self {
            RequestType::Balance => 0u8
        }
    }

    fn to_bytes(&self) -> [u8; 1] {
        return self.ordinal().to_le_bytes();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub source: Account,
    pub tpe: RequestType,
    pub nonce: u64
}

impl Digestable for Request {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.source);
        hasher.update(self.tpe.to_bytes());
        hasher.update(self.nonce.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl Nonceable for Request {
    fn get_nonce(&self) -> u64 {
        return self.nonce.clone();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedRequest {
    pub request: Request,
    pub signature: Signature
}

// ------------------------------------------------------------------------
#[derive(Debug, Clone, Eq, Serialize, Deserialize, Hash)]
pub struct Transaction {
    pub source: Account,    // 32B
    pub payload: Vec<u8>,
    pub args: Vec<TransactionArgument>,
    pub nonce: u64          // 8B
}

fn to_single_ref<'a>(tx_args: Vec<TransactionArgument>) -> Vec<u8> {
    tx_args.iter()
        .flat_map(|arg| {
            MoveValue::from(arg.clone())
                .simple_serialize()
                .expect("transaction arguments must serialize")
        })
        .collect()
}

impl Digestable for Transaction {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        // hasher.update(self.source);
        hasher.update(self.payload.clone());
        hasher.update(to_single_ref(self.args.clone()));
        hasher.update(self.nonce.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl Nonceable for Transaction {
    fn get_nonce(&self) -> u64 {
        return self.nonce.clone();
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nonce.cmp(&other.nonce)
    }
}

impl PartialOrd for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.nonce == other.nonce
    }
}

// ------------------------------------------------------------------------
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedTransaction {
    pub content: Transaction,
    pub signature: Signature    // signed hash of transaction
}

impl From<SignedTransaction> for Bytes {
    fn from(tx: SignedTransaction) -> Bytes {
        let serialized = bincode::serialize(&tx)
            .expect("Failed to serialize a transaction");

        return Bytes::from(serialized);
    }
}

impl SignedTransaction {
    pub fn from(bytes: Bytes) -> SignedTransaction {

        let tx = bincode::deserialize(&bytes)
            .expect("Failed to deserialize a transaction");

        return tx;
    }

    pub fn from_vec(vec: &Vec<u8>) -> SignedTransaction {

        if cfg!(feature = "benchmark") {
            // NOTE: During benchmark there are 9 bytes prepended to the serialized
            return SignedTransaction::from_slice(&vec[9..]);
        }
        
        let tx = bincode::deserialize(&vec)
            .expect("Failed to deserialize a transaction");

        return tx;
    }

    pub fn from_slice(slice: &[u8]) -> SignedTransaction {

        let tx = bincode::deserialize(&slice)
            .expect("Failed to deserialize a transaction");

        return tx;
    }
}

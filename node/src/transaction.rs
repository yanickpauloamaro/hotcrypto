use crate::config::{Export, ConfigError};

use crypto::{PublicKey, Signature, Hash as Digestable, Digest};
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use std::hash::Hash;
use ed25519_dalek::Digest as _;
use std::convert::TryInto;

use move_core_types::{
    account_address::AccountAddress,
    value::MoveValue,
    transaction_argument::TransactionArgument
};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

pub type Currency = u64;
pub const CONST_INITIAL_BALANCE: Currency = 100;
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
#[derive(Clone, Serialize, Deserialize)]
pub struct Register {
    pub accounts: Vec<Account>
}

impl Export for Register {
    fn read(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            let keys: Vec<PublicKey> = serde_json::from_slice(data.as_slice())?;
            let accounts = keys.iter().map(|key| Account::new(key.clone())).collect();
            Ok(Self { accounts })
        };
        reader().map_err(|e| ConfigError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let to_write: Vec<_> = self.accounts.iter().map(|account| account.public_key).collect();
            let data = serde_json::to_string_pretty(&to_write).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
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
#[derive(Debug, Serialize, Deserialize, Hash)]
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

        let tx = bincode::deserialize(&vec)
            .expect("Failed to deserialize a transaction");

        return tx;
    }
}

use crypto::{PublicKey, Signature, Hash as Digestable, Digest};
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use std::hash::Hash;
use ed25519_dalek::Digest as _;
use std::convert::TryInto;

pub type Currency = u64;
pub const CONST_INITIAL_BALANCE: Currency = 100;

pub type Account = PublicKey;

pub type Nonce = u64;
pub trait Nonceable {
    fn get_nonce(&self) -> u64;
}

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

/* 
Une transaction doit contenir à minima:
  - l'adresse de l'émetteur (disons 32 bytes)
  - la quantité à transférer (8 bytes)
  - l'adresse du recepteur (32 bytes)
  - une signature (48 bytes)
donc 120 bytes
*/
#[derive(Debug, Serialize, Deserialize, Hash)]
pub struct Transaction {
    pub source: Account,    // 32B
    pub dest: Account,      // 32B
    pub amount: Currency,   // 8B
    pub nonce: u64          // 8B
}

impl Digestable for Transaction {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.source);
        hasher.update(self.dest);
        hasher.update(self.amount.to_le_bytes());
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
        // ##TODO: does this guarantee a consistent size?
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
}
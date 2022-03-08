use crypto::{PublicKey, Signature};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use bytes::Bytes;

#[derive(Serialize, Deserialize)]
pub enum Request {
    Balance
}

pub type Currency = u64;
pub const CONST_INITIAL_BALANCE: Currency = 100;

pub type Account = PublicKey;

/* 
Une transaction doit contenir à minima:
  - l'adresse de l'émetteur (disons 32 bytes)
  - la quantité à transférer (8 bytes)
  - l'adresse du recepteur (32 bytes)
  - une signature (48 bytes)
donc 120 bytes
*/
#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    source: Account,    // 32B
    dest: Account,      // 32B
    amount: Currency,   // 8B
    nonce: u64          // 8B
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SignedTransaction {
    content: Transaction,
    signature: Signature
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
    pub fn from(bytes: Bytes) -> Transaction {
        
        let tx = bincode::deserialize(&bytes)
            .expect("Failed to deserialize a transaction");
        
        return tx
    }
}
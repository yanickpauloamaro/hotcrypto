use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::currency::*;

use consensus::{Block, Consensus};
use crypto::{SignatureService, Digest};
use log::{info, warn};
use mempool::{MempoolMessage, Mempool};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use std::collections::HashMap;
use tokio::sync::oneshot;
use futures::sink::SinkExt as _;
use crypto::{Signature, Hash as Digestable};
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::StoreCommand;
#[cfg(feature = "parallel")]
use futures::future::join_all;
#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Block>,
    pub request: Receiver<(SignedRequest, oneshot::Sender<Currency>)>,
    pub store: Store,
    pub accounts: HashMap<Account, Currency>,
    pub nonces: HashMap<Account, Nonce>
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
        port: &str
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_request, rx_request) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        let port = port.parse::<u16>()
            .expect("Failed to parse node port");

        let mut address = committee.mempool
            .transactions_address(&name)
            .expect("Our public key is not in the committee")
            .clone();

        // ##TODO Do I need to set the ip?
        address.set_ip("0.0.0.0".parse().unwrap());
        address.set_port(port);
        info!("Listening to requests at {}", address);

        NetworkReceiver::spawn(
            address,
            /* handler */ RequestReceiverHandler { tx_request },
        );

        // Make a new mempool.
        Mempool::spawn(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store.clone(),
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit,
                request: rx_request,
                store: store,
                accounts: HashMap::new(),
                nonces: HashMap::new()})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    fn get_balance(&self, account: &Account) -> Currency {
        return self.accounts.get(&account).unwrap_or(&CONST_INITIAL_BALANCE).clone();
    }

    fn get_nonce(&self, account: &Account) -> u64 {
        return self.nonces.get(&account).unwrap_or(&0u64).clone();
    }

    fn increment_nonce(&mut self, account: &Account) {
        let nonce = self.nonces.entry(*account).or_insert(0);
        *nonce += 1;
    }

    fn verify_nonce<M>(&self, msg: &M, source: &Account) -> bool
        where M: Nonceable
    {
        return msg.get_nonce() == self.get_nonce(source);
    }

    fn verify_signature<M>(&self, msg: &M, source: &Account, signature: &Signature) -> bool
        where M: Digestable
    {
        return signature.verify(&msg.digest(), source).is_ok();
    }

    fn transfer(&mut self, source: Account, dest: Account, amount: Currency) {
        let source_balance = self.get_balance(&source);
        let dest_balance = self.get_balance(&dest);

        if source_balance >= amount {
            self.accounts.insert(source, source_balance - amount);
            self.accounts.insert(dest, dest_balance + amount);
            // info!("Transfered {} from {} to {}... ", amount, source, dest);
            // info!("Resulting balance for {} is {}$:", source, source_balance - amount);
        }
    }

    async fn verify_batch(&self, store: Sender<StoreCommand>, key: &Digest) -> Vec<Transaction> {

        let (sender, receiver) = oneshot::channel();

        if let Err(e) = store.send(StoreCommand::Read(key.to_vec(), sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        let serialized = receiver
            .await
            .expect("Failed to receive reply to Read command from store")
            .expect("Failed to get batch from storage")
            .expect("Batch was not in storage");

        info!("Deserializing stored batch...");
        let mempool_message = bincode::deserialize(&serialized)
            .expect("Failed to deserialize batch");

        match mempool_message {
            MempoolMessage::Batch(batch) => {

                #[cfg(feature = "parallel")]
                let iter: rayon::slice::Iter<Vec<u8>> = batch.par_iter();
                
                #[cfg(not(feature = "parallel"))]
                let iter: std::slice::Iter<Vec<u8>> = batch.iter();
                
                let verified_batch: Vec<Transaction> = iter
                    .filter_map(|tx_vec| {
                        let SignedTransaction{content: tx, signature} = SignedTransaction::from_vec(tx_vec);
                        if self.verify_signature(&tx, &tx.source, &signature) {
                            Some(tx)
                        } else {
                            None
                        }
                    }).collect();

                return verified_batch;
            },
            MempoolMessage::BatchRequest(_, _) => {
                warn!("A batch request was stored!");
                return Vec::new();
            }
        }
    }

    async fn verify_block(&self, block: &Block) -> Vec<Vec<Transaction>> {
        
        let store = self.store.command_channel();

        let mut verified_block = Vec::with_capacity(block.payload.len());

        #[cfg(feature = "parallel")] {
            block.payload.par_iter().map(|digest| {
                return self.verify_batch(store.clone(), digest);
            }).collect_into_vec(&mut verified_block);

            return join_all(verified_block).await;
        }

        #[cfg(not(feature = "parallel"))] {
            for digest in &block.payload {
                verified_block.push(self.verify_batch(store.clone(), &digest).await);
            }

            return verified_block;
        }
    }

    pub async fn analyze_block(&mut self) {
        info!("Starting analyze loop");

        #[cfg(feature = "parallel")]
        info!("Transaction signatures will be checked in parallel");

        #[cfg(not(feature = "parallel"))]
        info!("Transaction signatures will be checked sequentially");

        loop {
            tokio::select! {
                Some((request, tx_response)) = self.request.recv() => {
                    // info!("Received request from {}", request.request.source);

                    // Verify request signature and send response
                    let SignedRequest{request, signature} = request;

                    if self.verify_signature(&request, &request.source, &signature)
                        && self.verify_nonce(&request, &request.source) {

                        self.increment_nonce(&request.source);

                        // There is only one type of request for now
                        let balance = self.get_balance(&request.source);
                        let _err = tx_response.send(balance);
                    }
                },
                Some(block) = self.commit.recv() => {
                    
                    let verified_block = self.verify_block(&block).await;
                    let zipped = block.payload.iter().zip(verified_block);
                    
                    for (_digest, batch) in zipped {
                        for tx in &batch {
                            if self.verify_nonce(tx, &tx.source) {
                                self.increment_nonce(&tx.source);
                                self.transfer(tx.source, tx.dest, tx.amount);

                                #[cfg(feature = "benchmark")]
                                if tx.amount == SAMPLE_TX_AMOUNT {
                                    // NOTE: This log entry is used to compute performance.
                                    info!("Processed sample transaction {} from {:?}", tx.nonce, tx.source);
                                }
                            }
                        }

                        #[cfg(feature = "benchmark")]
                        {
                            // NOTE: This is one extra hash that is only needed to print the following log entries.
                            let digest = Digest(
                                Sha512::digest(&_digest.to_vec()).as_slice()[..32]
                                    .try_into()
                                    .unwrap(),
                            );
                            // NOTE: This log entry is used to compute performance.
                            info!("Batch {:?} contains {} currency tx", digest, batch.len());
                        }
                    }
                }
            }
        }
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct RequestReceiverHandler {
    tx_request: Sender<(SignedRequest, oneshot::Sender<Currency>)>
}

#[async_trait]
impl MessageHandler for RequestReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {

        let (tx_response, rx_response) = oneshot::channel();

        let request = bincode::deserialize(&message)
            .expect("Failed to deserialize a request");

        // Send the request to the node.
        if let Err(e) = self.tx_request.send((request, tx_response)).await {
            panic!("Failed to send request to node: {:?}", e);
        }

        let response: Currency = rx_response
            .await
            .expect("Failed to receive response from node");

        let serialized = bincode::serialize(&response)
            .expect("Failed to serialize response");

        let _ = writer.send(Bytes::from(serialized)).await;

        // Give the chance to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

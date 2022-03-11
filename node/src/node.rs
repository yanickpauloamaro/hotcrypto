use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::currency::{Account, Nonceable, Nonce, SignedRequest, Currency, SignedTransaction, CONST_INITIAL_BALANCE};

use consensus::{Block, Consensus};
use crypto::{SignatureService, Digest};
use log::{info, debug, warn};
use mempool::{MempoolMessage, Mempool};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;
use futures::sink::SinkExt as _;
use crypto::{Signature, Hash as Digestable};

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

    async fn store_read(&mut self, hash: &Digest) -> Option<Vec<u8>> {
        let duration_ms = 100;

        let timer = sleep(Duration::from_millis(duration_ms));
        tokio::pin!(timer);
        tokio::select!{
            res = self.store.read(hash.to_vec()) => {
                return res.unwrap_or(None);
            },
            () = &mut timer => {
                warn!("Store took more than {} to respond!", duration_ms);
                return None;
            }
        }
    }

    fn get_balance(&self, account: &Account) -> Currency {
        return self.accounts.get(&account).unwrap_or(&CONST_INITIAL_BALANCE).clone();
    }

    fn get_nonce(&self, account: &Account) -> u64 {
        return self.nonces.get(&account).unwrap_or(&0u64).clone();
    }

    fn increment_nonce(&mut self, account: &Account) {
        let nonce = self.nonces.entry(*account).or_insert(0);
        info!("Incrementing nonce for {0}: {1} -> {2}", account, *nonce, 1+*nonce);
        *nonce += 1;
    }

    fn verify<M>(&self, msg: &M, source: &Account, signature: &Signature) -> bool 
        where M: Digestable, M: Nonceable
    {
        info!("Verifiying transaction/request from {}", source);
        let signature_check = signature.verify(&msg.digest(), source).is_ok();
        let nonce_check = msg.get_nonce() == self.get_nonce(source);

        return signature_check && nonce_check;
    }

    fn transfer(&mut self, source: Account, dest: Account, amount: Currency) {
        let source_balance = self.get_balance(&source);
        let dest_balance = self.get_balance(&dest);

        if source_balance >= amount {
            self.accounts.insert(source, source_balance - amount);
            self.accounts.insert(dest, dest_balance + amount);
            info!("Transfered {} from {} to {}... ", amount, source, dest);
        }
    }

    pub async fn analyze_block(&mut self) {
        info!("Starting analyze loop");
        loop {
            tokio::select! {
                Some((request, tx_response)) = self.request.recv() => {
                    info!("Received request from {}", request.request.source);

                    // Verify request signature and send response
                    let SignedRequest{request, signature} = request;

                    if self.verify(&request, &request.source, &signature) {
                        info!("Request is valid");
                        self.increment_nonce(&request.source);

                        // There is only one type of request for now
                        let balance = self.get_balance(&request.source);
                        let _err = tx_response.send(balance);
                    }
                },
                Some(block) = self.commit.recv() => {
                    // Verify transaction signatures and update accounts for each transaction
                    if !block.payload.is_empty() {
                        debug!("Received block!");
                    }
                    
                    for digest in &block.payload {
                        let serialized = self.store_read(digest)
                            .await
                            .expect("Failed to get object from storage");

                        info!("Deserializing stored batch...");
                        let mempool_message = bincode::deserialize(&serialized)
                            .expect("Failed to deserialize batch");

                        match mempool_message {
                            MempoolMessage::Batch(batch) => {
                                for tx_vec in batch {
                                    let SignedTransaction{content: tx, signature} = SignedTransaction::from_vec(tx_vec);
            
                                    if self.verify(&tx, &tx.source, &signature) {
                                        info!("Transaction is valid");
                                        self.increment_nonce(&tx.source);
                                        self.transfer(tx.source, tx.dest, tx.amount);
                                    }

                                    #[cfg(feature = "benchmark")]
                                    if tx.amount == 2 {
                                        // ##TODO check that feature=benchmark works
                                        // NOTE: This log entry is used to compute performance.
                                        info!("Processed sample transaction {} from {:?}", tx.nonce, tx.source);
                                    }
                                }
                            },
                            MempoolMessage::BatchRequest(_, _) => {
                                warn!("A batch request was stored!");
                            }
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

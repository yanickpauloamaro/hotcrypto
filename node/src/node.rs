use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::currency::{Request, Currency, SignedTransaction, CONST_INITIAL_BALANCE};

use consensus::{Block, Consensus};
use crypto::{SignatureService, Digest};
use log::{info, debug, warn};
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use tokio::net::{TcpListener, TcpStream};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use std::net::SocketAddr;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;
use std::fmt;
use futures::sink::SinkExt as _;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Block>,
    pub request: Receiver<(Request, oneshot::Sender<Currency>)>,
    pub store: Store
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

        let port = port.parse::<u16>().expect("Failed to parse node port");
        let mut address = committee.mempool
            .transactions_address(&name)
            .expect("Our public key is not in the committee")
            .clone();

        address.set_ip("0.0.0.0".parse().unwrap());
        address.set_port(port);
        println!("Listening for requests on {}", address);

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
        Ok(Self { commit: rx_commit, request: rx_request, store: store})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    async fn fetch(&mut self, hash: &Digest) -> Option<Vec<u8>> {
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

    pub async fn analyze_block(&mut self) {

        let accounts: HashMap<Digest, Currency> = HashMap::new();

        loop {
            tokio::select! {
                Some((request, tx_response)) = self.request.recv() => {
                    // ##TODO: Should have signature to show that the client owns the account
                    let account: Digest;
                    let balance = accounts.get(&account).unwrap_or(&CONST_INITIAL_BALANCE).clone();

                    tx_response.send(balance);
                },
                Some(block) = self.commit.recv() => {
                    // verify transactions and update accounts
                    for x in &block.payload {
                        let option = self.fetch(x).await;
                        if option.is_none() {
                            continue;
                        }

                        let bytes = Bytes::from(option.unwrap());
                        let transaction = SignedTransaction::from(bytes);

                        // ##TODO: verify signature
                    }
                }
            }
        }
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct RequestReceiverHandler {
    tx_request: Sender<(Request, oneshot::Sender<Currency>)>
}

#[async_trait]
impl MessageHandler for RequestReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {

        let (tx_response, rx_response) = oneshot::channel();

        let request = bincode::deserialize(&message)
            .expect("Failed to deserialize a request");

        // Send the request to the node.
        self.tx_request
            .send((request, tx_response))
            .await;

        let response = rx_response.await;

        // ##TODO Send response back to client
        let _ = writer.send(Bytes::from("Ack")).await;

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

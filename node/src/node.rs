use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::transaction::*;
use crate::compiler::Compiler;

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
use crypto::{PublicKey, Signature, Hash as Digestable};
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::StoreCommand;
use rayon::prelude::*;

use anyhow::Result;

use move_vm_runtime::move_vm::MoveVM;
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas_schedule::GasStatus;
use move_core_types::{
    value::MoveValue,
    account_address::AccountAddress
};

#[cfg(feature = "benchmark")]
use move_core_types::transaction_argument::TransactionArgument;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;
const BATCH_JOB_COUNT: usize = 3;

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

    fn verify_signature<M>(&self, msg: &M, source: &PublicKey, signature: &Signature) -> bool
        where M: Digestable
    {
        return signature.verify(&msg.digest(), source).is_ok();
    }

    // fn transfer(&mut self, source: Account, dest: Account, amount: Currency) {
    //     let source_balance = self.get_balance(&source);
    //     let dest_balance = self.get_balance(&dest);

    //     if source_balance >= amount {
    //         self.accounts.insert(source, source_balance - amount);
    //         self.accounts.insert(dest, dest_balance + amount);
    //         // info!("Transfered {} from {} to {}... ", amount, source, dest);
    //         // info!("Resulting balance for {} is {}$:", source, source_balance - amount);
    //     }
    // }

    fn verify_transactions(&self, transactions: Vec<Transaction>, proofs: Vec<(PublicKey, Signature)>) -> Vec<Transaction> {
        transactions.into_iter().zip(proofs.iter())
            .filter_map(move |(tx, (key, signature))| {
                if self.verify_signature(&tx, key, signature) {
                    Some(tx)
                } else {
                    None
                }
            }).collect()
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
                if batch.len() == 0 {
                    return Vec::new();
                }

                let chunk_size = (batch.len() / BATCH_JOB_COUNT) + (batch.len() % BATCH_JOB_COUNT != 0) as usize;

                let jobs: rayon::slice::Chunks<Vec<u8>> = batch.par_chunks(chunk_size);

                let verified_batch: Vec<Transaction> = jobs
                    .flat_map(|job| {
                        let job_size = job.len();
                        let mut transactions = Vec::with_capacity(job_size);
                        let mut digests = Vec::with_capacity(job_size);
                        let mut proofs = Vec::with_capacity(job_size);

                        for tx_vec in job {
                            let SignedTransaction{content: tx, signature} = SignedTransaction::from_vec(tx_vec);
                            proofs.push((tx.source.public_key, signature));
                            digests.push(tx.digest());
                            transactions.push(tx);
                        }

                        if Signature::verify_many(&digests, &proofs).is_ok() {
                            transactions
                        } else {
                            warn!("Some transactions are invalid. Checking them one by one...");
                            self.verify_transactions(transactions, proofs)
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

        let mut verified_block = Vec::with_capacity(block.payload.len());

        for digest in &block.payload {
            verified_block.push(self.verify_batch(self.store.command_channel(), &digest).await);
        }

        return verified_block;
    }

    pub async fn analyze_block(&mut self) {
        
        info!("Setting up MoveVM...");
        let mut storage = InMemoryStorage::new();

        let mut gas_status = GasStatus::new_unmetered();

        let compiler = Compiler::new().expect("Failed to create compiler");

        let vm = MoveVM::new(move_stdlib::natives::all_natives(
            AccountAddress::from_hex_literal("0x1").unwrap(),
        ))
        .unwrap();

        for module in compiler.dependencies() {
            info!("Publishing and loading module {}", module.self_id().short_str_lossless());
            
            let mut blob = vec![];
            module.serialize(&mut blob).unwrap();

            storage.publish_or_overwrite_module(module.self_id(), blob);

            vm.load_module(&module.self_id(), &storage).expect("Failed to load module");
        }

        info!("Creating accounts for all clients...");
        // TODOTODO Create accounts and mint CONST_INITIAL_BALANCE for each

        info!("Starting analyze loop");

        loop {
            tokio::select! {
                Some((request, tx_response)) = self.request.recv() => {
                    // info!("Received request from {}", request.request.source);

                    // Verify request signature and send response
                    let SignedRequest{request, signature} = request;

                    if self.verify_signature(&request, &request.source.public_key, &signature)
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

                                #[cfg(feature = "benchmark")]
                                let is_sample = match tx.args.last() {
                                    Some(TransactionArgument::U64(SAMPLE_TX_AMOUNT)) => true,
                                    _ => false
                                };

                                let signer = MoveValue::Signer(tx.source.address);
                                let mut tx_args: Vec<MoveValue> = tx.args.iter()
                                    .map(|arg| MoveValue::from(arg.clone()))
                                    .collect();

                                tx_args.insert(0, signer);

                                // self.transfer(tx.source, tx.dest, tx.amount);
                                let mut sess = vm.new_session(&storage);
                                let err = sess.execute_script(
                                    tx.payload.clone(),
                                    vec![], // ty_args: Vec<TypeTag>
                                    tx_args.iter()
                                        .map(|arg| arg.simple_serialize().unwrap())
                                        .collect(),
                                    &mut gas_status,
                                )
                                .map(|_| ());
                                
                                // match err {
                                //     Ok(_) => warn!("VM output ok"),
                                //     Err(other) => {
                                //         warn!("VM output: {}", other);
                                //         panic!("VMErr")
                                //     }
                                // };
                                
                                err.unwrap();

                                let (changeset, _) = sess
                                    .finish()
                                    .unwrap(); // TODO What about errors?
                                storage.apply(changeset).unwrap();


                                #[cfg(feature = "benchmark")]
                                if is_sample {
                                    // NOTE: This log entry is used to compute performance.
                                    info!("Processed sample transaction {} from {:?}", tx.nonce, tx.source.address);
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

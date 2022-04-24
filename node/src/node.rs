#![allow(unused)]
use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret, Register};
use crate::transaction::*;
use crate::utils::{Mode, init_vm};

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
use std::collections::BinaryHeap;
use std::cmp::{Reverse, Ordering};
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

use move_core_types::transaction_argument::TransactionArgument;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;
const BATCH_JOB_COUNT: usize = 3;
type Batch = Vec<Transaction>;

pub struct Node {
    pub commit: Receiver<Vec<(Digest, Batch)>>,
    pub request: Receiver<(SignedRequest, oneshot::Sender<Currency>)>,
    pub accounts: HashMap<AccountAddress, Currency>,
    pub nonces: HashMap<AccountAddress, Nonce>,
    backlogs: HashMap<AccountAddress, BinaryHeap<Reverse<Transaction>>>,
    mode: Mode,
    storage: InMemoryStorage,
    vm: MoveVM
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
        port: &str,
        register_file: &str,
        mode: &str
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_request, rx_request) = channel(CHANNEL_CAPACITY);
        let (tx_verified, rx_verified) = channel(CHANNEL_CAPACITY);

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

        let register = Register::read(register_file)?;
        
        let (vm, storage) = init_vm(&register)
        .expect("Unable initialize move vm");
        
        let balances = register.accounts
        .iter().map(|account| (account.address, CONST_INITIAL_BALANCE));
        
        let nonces = register.accounts
        .iter().map(|account| (account.address, 0));
        
        let backlogs = register.accounts
        .iter().map(|account| (account.address, BinaryHeap::new()));
        
        let mode = mode.parse::<Mode>()?;
        CryptoVerifier::spawn(
            mode.clone(),
            store,
            rx_commit,
            tx_verified
        );

        info!("Node {} successfully booted", name);
        Ok(Self {
                commit: rx_verified,
                request: rx_request,
                accounts: balances.collect(),
                nonces: nonces.collect(),
                backlogs: backlogs.collect(),
                mode,
                storage,
                vm,
            })
    }

    fn get_balance(&self, account: &AccountAddress) -> Currency {
        return self.accounts.get(&account).unwrap_or(&CONST_INITIAL_BALANCE).clone();
    }

    fn get_nonce(&self, account: &AccountAddress) -> u64 {
        return self.nonces.get(&account).unwrap_or(&0u64).clone();
    }

    fn increment_nonce(&mut self, account: &AccountAddress) {
        let nonce = self.nonces.entry(*account).or_insert(0);
        *nonce += 1;
    }

    fn verify_nonce<M>(&self, msg: &M, source: &AccountAddress) -> bool
        where M: Nonceable
    {
        return msg.get_nonce() == self.get_nonce(source);
    }

    fn compare_nonce<M>(&self, msg: &M, source: &AccountAddress) -> Ordering
        where M: Nonceable
    {
        return msg.get_nonce().cmp(&self.get_nonce(source));
    }

    fn transfer(&mut self, source: AccountAddress, dest: AccountAddress, amount: Currency) {
        let source_balance = self.get_balance(&source);
        let dest_balance = self.get_balance(&dest);

        if source_balance >= amount {
            self.accounts.insert(source, source_balance - amount);
            self.accounts.insert(dest, dest_balance + amount);
            // info!("Transfered {} from {} to {}... ", amount, source, dest);
            // info!("Resulting balance for {} is {}$:", source, source_balance - amount);
        }
    }    

    fn get_arguments(&self, args: &Vec<TransactionArgument>) -> Result<(AccountAddress, Currency)> {
        if args.len() < 2 {
            panic!("Not enough arguments of transaction");
        }
        match (&args[0], &args[1]) {
            (TransactionArgument::Address(dest), TransactionArgument::U64(amount)) => Ok((*dest, *amount)),
            _ => panic!("Transaction arguments have incorrect types")
        }
    }

    pub async fn analyze_block(&mut self) {
        
        info!("Starting analyze loop");
        let mut gas_status = GasStatus::new_unmetered();

        loop {
            tokio::select! {
                Some((request, tx_response)) = self.request.recv() => {
                    // info!("Received request from {}", request.request.source);

                    // Verify request signature and send response
                    let SignedRequest{request, signature} = request;

                    if CryptoVerifier::verify_signature(&request, &request.source.public_key, &signature)
                        && self.verify_nonce(&request, &request.source.address) {

                        self.increment_nonce(&request.source.address);

                        // There is only one type of request for now
                        let balance = self.get_balance(&request.source.address);
                        let _err = tx_response.send(balance);
                    }
                },
                Some(verified_block) = self.commit.recv() => {

                    for (_digest, batch) in verified_block {
                        for tx in &batch {
                            match self.compare_nonce(tx, &tx.source.address) {
                                Ordering::Equal => {
                                    self.increment_nonce(&tx.source.address);
                                    self.execute_transaction(tx, &mut gas_status);

                                    while self.has_progress(&tx.source.address) {
                                        let backlog = self.backlogs.get_mut(&tx.source.address).unwrap();
                                        let Reverse(next) = backlog.pop().unwrap();

                                        // warn!("Processing transaction {} from {}'s backlog...", next.nonce, tx.source.address);
                                        self.increment_nonce(&tx.source.address);
                                        self.execute_transaction(&next, &mut gas_status);
                                    }
                                }
                                Ordering::Greater => {
                                    // warn!("Transaction from {} too new. Got {} instead of {}", tx.source.address, tx.nonce, self.get_nonce(&tx.source.address));
                                    let backlog = self.backlogs.get_mut(&tx.source.address).unwrap();
                                    let clone: Transaction = (*tx).clone();
                                    backlog.push(Reverse(clone));
                                }
                                Ordering::Less => {
                                    // warn!("Transaction from {} too old. Got {} instead of {}", tx.source.address, tx.nonce, self.get_nonce(&tx.source.address));
                                }
                            }
                        }

                        // NOTE: This log entry is used to compute performance.
                        #[cfg(feature = "benchmark")]
                        info!("Batch {:?} contains {} currency tx", _digest, batch.len());
                    }
                }
            }
        }
    }

    fn has_progress(&self, address: &AccountAddress) -> bool {
        let backlog = self.backlogs.get(address).unwrap();

        if let Some(Reverse(next)) = backlog.peek() {
            self.verify_nonce(next, address)
        } else {
            false
        }

    }

    fn execute_transaction(&mut self, tx: &Transaction, gas_status: &mut GasStatus) {
        #[cfg(feature = "benchmark")]
        let is_sample = match tx.args.last() {
            Some(TransactionArgument::U64(SAMPLE_TX_AMOUNT)) => true,
            _ => false
        };

        match &self.mode {
            Mode::HotCrypto => {
                let (dest, amount) = self.get_arguments(&tx.args).unwrap();
                self.transfer(tx.source.address, dest, amount);
            }
            Mode::HotMove => {
                let signer = MoveValue::Signer(tx.source.address);
                let mut tx_args: Vec<MoveValue> = tx.args.iter()
                    .map(|arg| MoveValue::from(arg.clone()))
                    .collect();

                tx_args.insert(0, signer);

                let mut sess = self.vm.new_session(&self.storage);
                let err = sess.execute_script(
                    tx.payload.clone(),
                    vec![], // ty_args: Vec<TypeTag>
                    tx_args.iter()
                        .map(|arg| arg.simple_serialize().unwrap())
                        .collect(),
                    gas_status,
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
                self.storage.apply(changeset).unwrap();
            }
            mode => panic!("Unknown mode: {:?}", mode)
        }


        #[cfg(feature = "benchmark")]
        if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Processed sample transaction {} from {:?}", tx.nonce, tx.source.address);
        }
    }
}

// ------------------------------------------------------------------------
struct CryptoVerifier {
    pub store: Store,
    pub to_verify: Receiver<Block>,
    pub verified: Sender<Vec<(Digest, Batch)>>,
}

impl CryptoVerifier {
    fn spawn(
        mode: Mode,
        store: Store,
        to_verify: Receiver<Block>,
        verified: Sender<Vec<(Digest, Batch)>>) {

        let verifier = CryptoVerifier{
            store,
            to_verify,
            verified,
        };
        
        tokio::spawn(async move {
            match mode {
                Mode::HotStuff => verifier.do_nothing().await,
                Mode::HotCrypto | Mode::HotMove => verifier.verify_blocks().await,
                Mode::MoveVM => ()
            }
        });
    }

    async fn do_nothing(mut self) {
        while let Some(block) = self.to_verify.recv().await {

        }
    }

    async fn verify_blocks(mut self) {
        while let Some(block) = self.to_verify.recv().await {
            let mut verified_block = Vec::with_capacity(block.payload.len());

            for digest in &block.payload {
                // info!("Block {} contains batch {:?}", block, digest);
                let verified = Self::verify_batch(self.store.command_channel(), &digest).await;
                verified_block.push((digest.clone(), verified));
            }
    
            self.verified.send(verified_block).await;
        }
    }

    fn verify_signature<M>(msg: &M, source: &PublicKey, signature: &Signature) -> bool
        where M: Digestable
    {
        return signature.verify(&msg.digest(), source).is_ok();
    }

    fn verify_signatures(transactions: Vec<Transaction>, proofs: Vec<(PublicKey, Signature)>) -> Vec<Transaction> {
        transactions.into_iter().zip(proofs.iter())
            .filter_map(move |(tx, (key, signature))| {
                if Self::verify_signature(&tx, key, signature) {
                    Some(tx)
                } else {
                    None
                }
            }).collect()
    }

    async fn verify_batch(store: Sender<StoreCommand>, key: &Digest) -> Vec<Transaction> {

        let (sender, receiver) = oneshot::channel();

        if let Err(e) = store.send(StoreCommand::Read(key.to_vec(), sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        let serialized = receiver
            .await
            .expect("Failed to receive reply to Read command from store")
            .expect("Failed to get batch from storage")
            .expect("Batch was not in storage");

        info!("Deserializing stored batch {:?}...", key);
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
                            Self::verify_signatures(transactions, proofs)
                        }
                    }).collect();

                #[cfg(feature = "benchmark")]
                for tx in &verified_batch {
                    match tx.args.last() {
                        Some(TransactionArgument::U64(SAMPLE_TX_AMOUNT)) => {
                            // NOTE: This log entry is used to compute performance.
                            info!("Verified sample transaction {} from {:?}", tx.nonce, tx.source.address);
                        },
                        _ => ()
                    };
                }

                // NOTE: This log entry is used to compute performance. TODOTODO
                #[cfg(feature = "benchmark")]
                info!("Batch {:?} contains {} crypto tx", key, verified_batch.len());

                return verified_batch;
            },
            MempoolMessage::BatchRequest(_, _) => {
                warn!("A batch request was stored!");
                return Vec::new();
            }
        }
    }
}

// ------------------------------------------------------------------------
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

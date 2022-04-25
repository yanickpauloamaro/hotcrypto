#![allow(unused)]
mod node;
mod config;
mod compiler;
mod transaction;
mod utils;

use crate::compiler::{Compiler, currency_named_addresses};
use crate::config::Export as _;
use crate::config::{Secret, Register};
use crate::transaction::*;
use crate::utils::*;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use bytes::BufMut as _;
use clap::{crate_name, crate_version, App, AppSettings, SubCommand, ArgMatches};
use crypto::{SecretKey, Signature, Hash as Digestable};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::{info, warn, error};
use rand::{
    Rng,
    rngs::ThreadRng,
    seq::SliceRandom,
};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Sender, Receiver, channel};
use tokio::time::{interval, sleep, Duration, Instant};

use move_core_types::{
    transaction_argument::TransactionArgument,
    value::MoveValue,
};
use move_vm_runtime::move_vm::MoveVM;
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas_schedule::GasStatus;

extern crate num_cpus;

#[tokio::main]
async fn main() {

    let matches = App::new(crate_name!())
        .version(crate_version!())
        .subcommand(
            SubCommand::with_name("keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        ).subcommand(
            SubCommand::with_name("run")
                .about("Benchmark client for nodes.")
                .arg_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
                .arg_from_usage("--duration=<INT> 'The duration (s) the benchmark will last'")
                .arg_from_usage("--timeout=<INT> 'The nodes timeout value'")
                .subcommand(
                    SubCommand::with_name("hotstuff")
                        .about("Benchmark HotStuff")
                        .arg_from_usage(SocketAddr::usage())
                        .arg_from_usage(Vec::<SocketAddr>::usage())
                        .arg_from_usage("--size=<INT> 'The size of each transaction in bytes'")
                        /* timeout, target, nodes, size */
                )
                .subcommand(
                    SubCommand::with_name("hotmove")
                        .about("Benchmark HotMove")
                        .arg_from_usage(SocketAddr::usage())
                        .arg_from_usage(Vec::<SocketAddr>::usage())
                        .arg_from_usage(Secret::usage())
                        .arg_from_usage(Register::usage())
                        /* timeout, target, nodes, key_file, register_file */
                ).subcommand(
                    SubCommand::with_name("hotcrypto")
                        .about("Benchmark HotCrypto")
                        .arg_from_usage(SocketAddr::usage())
                        .arg_from_usage(Vec::<SocketAddr>::usage())
                        .arg_from_usage(Secret::usage())
                        .arg_from_usage(Register::usage())
                        /* timeout, target, nodes, key_file, register_file */
                ).subcommand(
                    SubCommand::with_name("movevm")
                        .about("Benchmark MoveVM")
                        .arg_from_usage(Secret::usage())
                        .arg_from_usage(Register::usage())
                        /* timeout, key_file, register_file, rate */
                )
        )
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
    .format_timestamp_millis()
    .init();

    match matches.subcommand() {
        ("keys", Some(subm)) => {
            let filename = subm.value_of("filename").unwrap();
            if let Err(e) = print_key_file(filename) {
                error!("{}", e);
            }
        }
        ("run", Some(subm)) => {
            let _ = run(subm).await;
        }
        _ => unreachable!()
    }
}

async fn run<'a>(matches: &ArgMatches<'_>) -> Result<()> {

    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;

    let duration = matches
        .value_of("duration")
        .unwrap()
        .parse::<u64>()
        .context("The duration of the benchmark must be a non-negative integer")?;

    let timeout = matches
        .value_of("timeout")
        .unwrap()
        .parse::<u64>()
        .context("The timeout value must be a non-negative integer")?;

    info!("Creating {} client", matches.subcommand().0);

    let boxed: Box<dyn Client> = match matches.subcommand() {
        ("hotstuff", Some(subm)) =>     HotStuffClient::from(subm, timeout).await?,
        ("hotcrypto", Some(subm)) =>    CryptoClient::<HotCrypto>::from(subm, timeout, HotCrypto{}).await?,
        ("hotmove", Some(subm)) =>      CryptoClient::<HotMove>::from(subm, timeout, HotMove::new()?).await?,
        ("movevm", Some(subm)) =>       MoveClient::from(subm, timeout, rate, duration).await?,
        _ => unreachable!()
    };

    let cores = num_cpus::get();
    info!("Benchmarking {}", boxed.to_string());
    info!("Number of cores: {}", cores);
    info!("Transactions size: {} B", boxed.transaction_size());
    info!("Transactions rate: {} tx/s", rate);

    benchmark(boxed, rate, duration).await
}

// ------------------------------------------------------------------------
async fn benchmark(mut client: Box<dyn Client>, rate: u64, duration: u64) -> Result<()> {

    const PRECISION: u64 = 20; // Sample precision.
    const BURST_DURATION: u64 = 1000 / PRECISION;

    // Submit all transactions.
    let burst = rate / PRECISION;
    let mut counter = 0;
    let mut nonce = 0;

    let interval = interval(Duration::from_millis(BURST_DURATION));
    tokio::pin!(interval);

    // NOTE: This log entry is used to compute performance.
    info!("Start sending transactions");
    let start = Instant::now();

    'main: loop {
        interval.as_mut().tick().await;
        let now = Instant::now();

        for x in 0..burst {
            let is_sample = x == counter % burst;
            
            match client.send(is_sample, nonce, counter) {
                Wrapper::Local(sender, msg) =>
                    sender.send(msg)
                        .await
                        .context("Failed to send transaction")?,
                Wrapper::Remote(transport, bytes) =>
                    transport.send(bytes)
                        .await
                        .context("Failed to send transaction")?
            };

            nonce += 1;
        }
        
        if now.elapsed().as_millis() > BURST_DURATION as u128 {
            // NOTE: This log entry is used to compute performance.
            warn!("Transaction rate too high for this client");
        }

        if start.elapsed().as_millis() > (duration * 1000) as u128 {
            // NOTE: This log entry is used to compute performance.
            info!("Client: Reached end of benchmark");
            info!("Stop sending transactions");
            break 'main;
        }

        counter += 1;
    }

    Ok(())
}

type LocalMsg = (Vec<MoveValue>, bool);
pub enum Wrapper<'a> {
    Local(&'a tokio::sync::mpsc::Sender<LocalMsg>, LocalMsg),
    Remote(&'a mut Transport, bytes::Bytes)
}

// ------------------------------------------------------------------------
pub trait Client {
    fn send(&mut self, is_sample: bool, nonce: u64, counter: u64) -> Wrapper;

    fn transaction_size(&self) -> usize;

    fn to_string(&self) -> String;
}

// ------------------------------------------------------------------------
struct HotStuffClient {
    transport: Transport,
    size: usize,
    r: u64,
    buffer: BytesMut
}

impl HotStuffClient {
    pub async fn new(
        target: SocketAddr,
        nodes: Vec<SocketAddr>,
        timeout: u64,
        size: usize,
    ) -> Result<Self> {

        wait(nodes, timeout).await;
        
        let transport = connect(target).await?;
        info!("Node address: {}", target);

        let r = rand::thread_rng().gen();
        let buffer = BytesMut::with_capacity(size);

        let client = Self{transport, size, r, buffer};
        Ok(client)
    }

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64) -> Result<Box<dyn Client>> {
        
        let target: SocketAddr = SocketAddr::from_matches(matches)?;
        let nodes: Vec<SocketAddr> = Vec::<SocketAddr>::from_matches(matches)?;

        let size = matches
            .value_of("size")
            .unwrap()
            .parse::<usize>()
            .context("The size of transactions must be a non-negative integer")?;
        
        let client = Self::new(target, nodes, timeout, size).await?;
        Ok(Box::new(client))
    }
}

impl Client for HotStuffClient {
    fn send(&mut self, is_sample: bool, _nonce: u64, counter: u64) -> Wrapper {

        if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Sending sample transaction {}", counter);

            self.buffer.put_u8(0u8); // Sample txs start with 0.
            self.buffer.put_u64(counter); // This counter identifies the tx.
        } else {
            self.r += 1;

            self.buffer.put_u8(1u8); // Standard txs start with 1.
            self.buffer.put_u64(self.r); // Ensures all clients send different txs.
        };
        self.buffer.resize(self.size, 0u8);

        let bytes = self.buffer.split().freeze();

        Wrapper::Remote(&mut self.transport, bytes)
    }

    fn transaction_size(&self) -> usize {
        return self.size;
    }

    fn to_string(&self) -> String {
        "HotStuff".to_string()
    }
}

// ------------------------------------------------------------------------
trait CryptoCurrency {
    fn transaction_payload(&self) -> Vec<u8>;
    fn to_string(&self) -> String;
}

// ------------------------------------------------------------------------
struct HotCrypto;
impl CryptoCurrency for HotCrypto {
    fn transaction_payload(&self) -> Vec<u8> {
        Vec::new()  // TODOTODO make payload the same size as hotmove for fair comparison?
    }

    fn to_string(&self) -> String {
        "HotCrypto".to_string()
    }
}

// ------------------------------------------------------------------------
struct HotMove{
    script: Vec<u8>
}
impl HotMove {
    pub fn new() -> Result<Self> {
        let script_code = format!("
                import {}.BasicCoin;

                main(account: signer, to: address, amount: u64) {{
                    label b0:
                        BasicCoin.transfer(&account, move(to), move(amount));
                        return;
                }}
            ",
            currency_named_addresses().get("Currency").unwrap()
        );

        let compiler = Compiler::new()?;

        let script = compiler.into_script_blob(&script_code)?;

        Ok(Self { script })
    }
}
impl CryptoCurrency for HotMove {
    fn transaction_payload(&self) -> Vec<u8> {
        self.script.clone()
    }

    fn to_string(&self) -> String {
        "HotMove".to_string()
    }
}

// ------------------------------------------------------------------------
struct CryptoClient<T>
    where T: CryptoCurrency
{
    transport: Transport,
    secret_key: SecretKey,
    account: Account,
    register: Register,
    r: ThreadRng,
    currency: T,
}

impl<T> CryptoClient<T>
    where T: CryptoCurrency
{
    pub async fn new(
        target: SocketAddr,
        nodes: Vec<SocketAddr>,
        timeout: u64,
        secret: Secret,
        register: Register,
        currency: T,
    ) -> Result<Self> {
        
        wait(nodes, timeout).await;
        
        let transport = connect(target).await?;
        info!("Node address: {}", target);
        
        let r = rand::thread_rng();
        let account = Account::new(secret.name);
        info!("Account {:?}", account);
        info!("Other accounts {:?}", register.accounts);

        let client = Self{
            transport,
            secret_key: secret.secret,
            account,
            register,
            r,
            currency
        };
        
        Ok(client)
    }

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64, currency: T) -> Result<Box<CryptoClient<T>>> {
        
        let target = SocketAddr::from_matches(matches)?;
        let nodes = Vec::<SocketAddr>::from_matches(matches)?;
        
        let secret = Secret::from_matches(matches)?;
        let mut register = Register::from_matches(matches)?;
        register.accounts.retain(|account| account.public_key != secret.name);

        Some(register.accounts.len())
            .filter(|s| *s > 0)
            .expect("There must be at least one other account");
        
        let client = Self::new(target, nodes, timeout, secret, register, currency).await?;
        Ok(Box::new(client))
    }

    fn transaction(&self, dest: Account, amount: Currency, nonce: u64) -> Transaction {
        let args = vec![
            TransactionArgument::Address(dest.address),
            TransactionArgument::U64(amount),
        ];

        Transaction{
            source: self.account,
            payload: self.currency.transaction_payload(),
            args: args,
            nonce: nonce,
        }
    }

    fn signed_transaction(&self, dest: Account, amount: Currency, nonce: u64) -> SignedTransaction {

        let tx = self.transaction(dest, amount, nonce);

        let signature = Signature::new(&tx.digest(), &self.secret_key);
        
        SignedTransaction {
            content: tx,
            signature
        }
    }
}

impl<T> Client for CryptoClient<T>
    where T: CryptoCurrency
{
    fn send(
        &mut self,
        is_sample: bool, nonce: u64, counter: u64,
    ) -> Wrapper {

        let dest = *self.register.accounts.choose(&mut self.r).unwrap();
        
        let signed = if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Sending sample transaction {} from {:?}", nonce, self.account.address);
            self.signed_transaction(dest, SAMPLE_TX_AMOUNT, nonce)
        } else {
            self.signed_transaction(dest, NORMAL_TX_AMOUNT, nonce)
        };
        
        // let bytes = Bytes::from(signed);
        // Wrapper::Remote(&mut self.transport, bytes)

        let mut serialized: Vec<u8> = bincode::serialize(&signed)
        .expect("Failed to serialize a transaction");
        
        // NOTE: These bytes are used by the node to copute performance
        let mut msg = vec![!is_sample as u8];
        msg.extend_from_slice(&nonce.to_be_bytes());
        msg.extend_from_slice(&serialized);
        
        Wrapper::Remote(&mut self.transport, Bytes::from(msg))
    }

    fn transaction_size(&self) -> usize {

        let signed = self.signed_transaction(self.account, SAMPLE_TX_AMOUNT, 0);

        let size = bincode::serialized_size(&signed)
            .expect("Failed to serialize signed transaction");

        return 9 + size as usize;
    }

    fn to_string(&self) -> String {
        self.currency.to_string()
    }
}

// ------------------------------------------------------------------------
struct MoveClient {
    account: Account,
    register: Register,
    tx_send: Sender<(Vec<MoveValue>, bool)>,
}

impl MoveClient {
    pub async fn new(
        timeout: u64,
        secret: Secret,
        mut register: Register,
        rate: u64,
        duration: u64,
    ) -> Result<Self> {
        
        // 10 seconds of margin
        let channel_capacity = 10 * rate as usize;
        info!("Channel capacity: {}", channel_capacity);
        let (tx_send, tx_receive) = channel(channel_capacity);
        
        let node = MoveNode::new(tx_receive, &register);
        tokio::spawn(node.receive(duration));

        info!("Waiting for the node to be ready...");
        sleep(Duration::from_millis(2 * timeout)).await;
        
        // Make sure we don't make transfers to ourself
        register.accounts.retain(|account| account.public_key != secret.name);

        let account = Account::new(secret.name);
        info!("Account {:?}", account);
        info!("Other accounts {:?}", register.accounts);

        let client = Self{
            account,
            register,
            tx_send
        };
        
        Ok(client)
    }

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64, rate: u64, duration: u64) -> Result<Box<dyn Client>> {
        
        let secret = Secret::from_matches(matches)?;
        let register = Register::from_matches(matches)?;

        Some(register.accounts.len())
            .filter(|s| *s > 1)
            .expect("There must be at least one other account");

        let client = Self::new(timeout, secret, register, rate, duration).await?;
        Ok(Box::new(client))
    }
}

impl Client for MoveClient {
    fn send(&mut self, is_sample: bool, nonce: u64, _counter: u64) -> Wrapper {

        let mut amount = NORMAL_TX_AMOUNT;
        let mut from = self.account.address;
        let mut dest = self.register.accounts[0].address;
        if nonce % 2 == 0 {
            std::mem::swap(&mut from, &mut dest);
        }

        if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Sending sample input {} from {:?}", nonce, from);
            amount = SAMPLE_TX_AMOUNT; // This amount identifies sample transactions
        };
        
        let args = vec![
            MoveValue::Signer(from),
            MoveValue::Address(dest),
            MoveValue::U64(amount),
        ];

        Wrapper::Local(&self.tx_send, (args, is_sample))
    }

    fn transaction_size(&self) -> usize {
        return 0;
    }

    fn to_string(&self) -> String {
        "MoveVM".to_string()
    }
}

// ------------------------------------------------------------------------
struct MoveNode {
    tx_receive: Receiver<(Vec<MoveValue>, bool)>,
    vm: MoveVM,
    storage: InMemoryStorage,
    register: Register
}

impl MoveNode {

    pub fn new(tx_receive: Receiver<(Vec<MoveValue>, bool)>, register: &Register) -> Self {

        let (vm, storage) = init_vm(register)
            .expect("Unable initialize move vm");
        
        Self {
            tx_receive,
            vm,
            storage,
            register: register.clone()
        }
    }

    pub async fn receive(mut self, duration: u64) {
        
        let mut gas_status = GasStatus::new_unmetered();
        
        let script = HotMove::new()
            .expect("Unable create script")
            .transaction_payload();
        
        info!("Start processing transactions");
        let start = Instant::now();
        let mut nonce = 0;

        while let Some((args, is_sample)) = self.tx_receive.recv().await {

            let s = Instant::now();
            let mut sess = self.vm.new_session(&self.storage);
            let err = sess.execute_script(
                script.clone(),
                vec![],
                args.iter()
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
                .unwrap();
            self.storage.apply(changeset).unwrap();
    
            if is_sample {
                // NOTE: This log entry is used to compute performance.
                info!("Processed sample input {} (took {} ms)", nonce, s.elapsed().as_millis());
            }
    
            if start.elapsed().as_millis() > (duration * 1000) as u128 {
                info!("Move Node: Reached end of benchmark");
                break;
            }
            nonce += 1;
        }
    
        info!("Stop processing transactions");
        info!("MoveVM processed {} inputs", nonce);
    }
}
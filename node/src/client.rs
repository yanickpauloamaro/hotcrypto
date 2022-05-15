#![allow(unused)]
mod node;
mod config;
mod compiler;
mod transaction;
mod utils;
use crate::compiler::{Compiler, currency_named_addresses, currency_module_file};
use crate::config::{Secret, Register};
use crate::transaction::*;
use crate::utils::*;

extern crate num_cpus;

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
    seq::SliceRandom,
};
use std::cmp::max;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::mpsc::{Sender, Receiver, channel};
use tokio::task::{JoinHandle};
use tokio::time::{interval, Duration, Instant};

// MoveVM
use move_core_types::{
    transaction_argument::TransactionArgument,
    value::MoveValue,
};
use move_vm_runtime::move_vm::MoveVM;
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas_schedule::GasStatus;

// Diem
use language_e2e_tests::{
    account::AccountData, compile::compile_script, current_function_name, executor::FakeExecutor,
};
use move_ir_compiler_01::Compiler as MoveIRCompiler;
use move_binary_format_01::CompiledModule as CompModule;
use move_bytecode_verifier_01::verify_module;
use move_compiler_01::{
    compiled_unit::AnnotatedCompiledUnit,
    shared::NumericalAddress,
};

use diem_types::{
    transaction::{
        Module, 
        SignedTransaction as DiemSignedTransaction,
        TransactionStatus, 
        Script},
    vm_status::KeptVMStatus,
};

use move_core_types_01::{
    transaction_argument::TransactionArgument as DiemArgument,
};

macro_rules! lap {
    ( $chrono:expr, $acc:expr ) => {
        $acc = $acc.saturating_add($chrono.elapsed());
        $chrono = Instant::now();
    };
}

const PRECISION: u64 = 20; // Sample precision.
const BURST_DURATION: u64 = 1000 / PRECISION;

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
                ).subcommand(
                    SubCommand::with_name("diemvm")
                        .about("Benchmark DiemVM")
                        .arg_from_usage("--with_signature=<BOOL> 'Whether to compute a signature for each transaction'")
                        /* timeout, rate */
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

    let client: ClientWrapper = match matches.subcommand() {
        ("hotstuff", Some(subm)) =>     ClientWrapper::HotStuff(HotStuffClient::from(subm, timeout).await?),
        ("hotcrypto", Some(subm)) =>    ClientWrapper::HotCrypto(CryptoClient::<HotCrypto>::from(subm, timeout, HotCrypto{}).await?),
        ("hotmove", Some(subm)) =>      ClientWrapper::HotMove(CryptoClient::<HotMove>::from(subm, timeout, HotMove::new()?).await?),
        ("movevm", Some(subm)) =>       ClientWrapper::MoveVM(MoveClient::from(subm, timeout, rate, duration).await?),
        ("diemvm", Some(subm)) => {
            let with_signature = subm
                .value_of("with_signature")
                .unwrap()
                .parse::<bool>()
                .context("must be a boolean")?;
            return ClientWrapper::benchmark_diem(rate, duration, with_signature).await;
        }
        _ => unreachable!()
    };

    let cores = num_cpus::get();
    info!("Benchmarking {}", client.to_string());
    info!("Number of cores: {}", cores);
    info!("Transactions size: {} B", client.transaction_size());
    info!("Transactions rate: {} tx/s", rate);

    benchmark(client, rate, duration).await.expect("Benchmark failed");

    Ok(())
}

// ------------------------------------------------------------------------
async fn benchmark(mut client: ClientWrapper, rate: u64, duration: u64) -> Result<()> {
    
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
                Wrapper::Remote(transport, bytes) =>
                    transport.send(bytes)
                        .await
                        .context("Failed to send transaction")?,
                Wrapper::Local(sender, msg) =>
                    sender.send(msg)
                        .await
                        .context("Failed to send transaction")?,
                Wrapper::DiemLocal(sender, msg) =>
                    sender.send(msg)
                        .await
                        .context("Failed to send transaction")?,
                Wrapper::Blank => ()
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

    client.conclude().await
}

type MoveMsg = (Vec<MoveValue>, bool);
type DiemMsg = (Vec<DiemSignedTransaction>, bool);
pub enum Wrapper<'a> {
    Remote(&'a mut Transport, bytes::Bytes),
    Local(&'a tokio::sync::mpsc::Sender<MoveMsg>, MoveMsg),
    DiemLocal(&'a tokio::sync::mpsc::Sender<DiemMsg>, DiemMsg),
    Blank
}

enum ClientWrapper {
    HotStuff(HotStuffClient),
    HotCrypto(CryptoClient::<HotCrypto>),
    HotMove(CryptoClient::<HotMove>),
    MoveVM(MoveClient),
    DiemVM(DiemClient),
}

impl ClientWrapper {
    async fn benchmark_diem(rate: u64, duration: u64, with_signature: bool) -> Result<()> {
        // 10 seconds of margin
        let channel_capacity = 10 * rate as usize;
        info!("Channel capacity: {}", channel_capacity);
        let (tx_send, tx_receive) = channel(channel_capacity);
        let (info_send, info_receive) = channel(1);

        tokio::spawn(async move {
            let cores = num_cpus::get();
            let client = DiemClient::new(rate, tx_send, info_receive, with_signature).await?;


            let wrapper = ClientWrapper::DiemVM(client);
            info!("Benchmarking {}", wrapper.to_string());
            info!("Number of cores: {}", cores);
            info!("Transactions size: {} B", wrapper.transaction_size());
            info!("Transactions rate: {} tx/s", rate);
            benchmark(wrapper, rate, duration).await
        });
        
        let mut node = DiemNode::new(tx_receive, info_send).await;
        node.receive(duration).await;
        
        Ok(())
    }

    fn send(&mut self, is_sample: bool, nonce: u64, counter: u64) -> Wrapper {
        match self {
            ClientWrapper::HotStuff(client) => client.send(is_sample, nonce, counter),
            ClientWrapper::HotCrypto(client) => client.send(is_sample, nonce, counter),
            ClientWrapper::HotMove(client) => client.send(is_sample, nonce, counter),
            ClientWrapper::MoveVM(client) => client.send(is_sample, nonce, counter),
            ClientWrapper::DiemVM(client) => client.send(is_sample, nonce, counter),
        }
    }

    fn transaction_size(&self) -> usize {
        match self {
            ClientWrapper::HotStuff(client) => client.transaction_size(),
            ClientWrapper::HotCrypto(client) => client.transaction_size(),
            ClientWrapper::HotMove(client) => client.transaction_size(),
            ClientWrapper::MoveVM(client) => client.transaction_size(),
            ClientWrapper::DiemVM(client) => client.transaction_size(),
        }
    }

    fn to_string(&self) -> String {
        match self {
            ClientWrapper::HotStuff(client) => client.to_string(),
            ClientWrapper::HotCrypto(client) => client.to_string(),
            ClientWrapper::HotMove(client) => client.to_string(),
            ClientWrapper::MoveVM(client) => client.to_string(),
            ClientWrapper::DiemVM(client) => client.to_string(),
        }
    }

    async fn conclude(self) -> Result<()> {
        match self {
            ClientWrapper::MoveVM(client) => client.conclude().await,
            _ => Ok(())
        }
    }
}

// ------------------------------------------------------------------------

pub trait ClientTrait {
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

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64) -> Result<Self> {
        
        let target: SocketAddr = SocketAddr::from_matches(matches)?;
        let nodes: Vec<SocketAddr> = Vec::<SocketAddr>::from_matches(matches)?;

        let size = matches
            .value_of("size")
            .unwrap()
            .parse::<usize>()
            .context("The size of transactions must be a non-negative integer")?;
            
        Ok(Self::new(target, nodes, timeout, size).await?)
    }
}

impl ClientTrait for HotStuffClient {
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
        Vec::new()
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
        
        let account = Account::new(secret.name);
        info!("Account {:?}", account);
        info!("Other accounts {:?}", register.accounts);

        let client = Self{
            transport,
            secret_key: secret.secret,
            account,
            register,
            currency
        };
        
        Ok(client)
    }

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64, currency: T) -> Result<CryptoClient<T>> {
        
        let target = SocketAddr::from_matches(matches)?;
        let nodes = Vec::<SocketAddr>::from_matches(matches)?;
        
        let secret = Secret::from_matches(matches)?;
        let mut register = Register::from_matches(matches)?;
        register.accounts.retain(|account| account.public_key != secret.name);

        Some(register.accounts.len())
            .filter(|s| *s > 0)
            .expect("There must be at least one other account");
        
        Ok(Self::new(target, nodes, timeout, secret, register, currency).await?)
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

impl<T> ClientTrait for CryptoClient<T>
    where T: CryptoCurrency
{
    fn send(
        &mut self,
        is_sample: bool, nonce: u64, _counter: u64,
    ) -> Wrapper {
        
        let mut r = rand::thread_rng();
        let dest = *self.register.accounts.choose(&mut r).unwrap();
        
        let signed = if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Sending sample transaction {} from {:?}", nonce, self.account.address);
            self.signed_transaction(dest, SAMPLE_TX_AMOUNT, nonce)
        } else {
            self.signed_transaction(dest, NORMAL_TX_AMOUNT, nonce)
        };
        
        // let bytes = Bytes::from(signed);
        // Wrapper::Remote(&mut self.transport, bytes)

        let serialized: Vec<u8> = bincode::serialize(&signed)
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
    node_handle: JoinHandle<()>
}

impl MoveClient {
    pub async fn new(
        _timeout: u64,
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
        let node_handle = tokio::spawn(node.receive(duration));

        // info!("Waiting for the node to be ready...");
        // sleep(Duration::from_millis(2 * timeout)).await;
        
        // Make sure we don't make transfers to ourself
        register.accounts.retain(|account| account.public_key != secret.name);

        let account = Account::new(secret.name);
        info!("Account {:?}", account);
        info!("Other accounts {:?}", register.accounts);

        let client = Self{
            account,
            register,
            tx_send,
            node_handle
        };
        
        Ok(client)
    }

    pub async fn from(matches: &ArgMatches<'_>, timeout: u64, rate: u64, duration: u64) -> Result<Self> {
        
        let secret = Secret::from_matches(matches)?;
        let register = Register::from_matches(matches)?;

        Some(register.accounts.len())
            .filter(|s| *s > 1)
            .expect("There must be at least one other account");

        Ok(Self::new(timeout, secret, register, rate, duration).await?)
    }

    pub async fn conclude(self) -> Result<()> {
        info!("Waiting for MoveNode to finish");
        self.node_handle.await.context("Failed to wait for MoveNode")
    }
}

impl ClientTrait for MoveClient {
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
    tx_receive: Receiver<MoveMsg>,
    vm: MoveVM,
    storage: InMemoryStorage,
    register: Register
}

impl MoveNode {

    pub fn new(tx_receive: Receiver<MoveMsg>, register: &Register) -> Self {

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
        
        let mut nonce: u128 = 0;
        let mut new_session_acc: Duration = Duration::ZERO;
        let mut execute_script_acc: Duration = Duration::ZERO;
        let mut finish_session_acc: Duration = Duration::ZERO;
        let mut apply_changeset_acc: Duration = Duration::ZERO;

        info!("Start processing transactions");
        let timer = interval(Duration::from_secs(duration));
        tokio::pin!(timer);
        timer.tick().await; // Interval ticks immediately

        loop {
            tokio::select! {
                _s = timer.tick() => {
                    info!("Move Node: Reached end of benchmark");
                    break;
                }
                Some((args, is_sample)) = self.tx_receive.recv() => {

                    let mut chrono = Instant::now();
                    let mut sess = self.vm.new_session(&self.storage);
                    lap!(chrono, new_session_acc);
        
                    sess.execute_script(
                        script.clone(),
                        vec![],
                        args.iter()
                            .map(|arg| arg.simple_serialize().unwrap())
                            .collect(),
                        &mut gas_status,
                    )
                    .map(|_| ())
                    .unwrap();
                    lap!(chrono, execute_script_acc);
            
                    let (changeset, _) = sess
                        .finish()
                        .unwrap();
                    lap!(chrono, finish_session_acc);
        
                    self.storage.apply(changeset).unwrap();
                    lap!(chrono, apply_changeset_acc);
            
                    if is_sample {
                        // NOTE: This log entry is used to compute performance.
                        info!("Processed sample input {}", nonce);
                    }

                    nonce += 1;
                }
            }
        }
        
        info!("Stop processing transactions");
        self.print_stats(
            nonce,
            new_session_acc,
            execute_script_acc,
            finish_session_acc,
            apply_changeset_acc
        );
    }
    
    fn print_stats(
        &self,
        nonce: u128,
        new_session_acc: Duration,
        execute_script_acc: Duration,
        finish_session_acc: Duration,
        apply_changeset_acc: Duration,
    ) {

        let mut total_duration = Duration::ZERO;
        total_duration = total_duration.saturating_add(new_session_acc);
        total_duration = total_duration.saturating_add(execute_script_acc);
        total_duration = total_duration.saturating_add(finish_session_acc);
        total_duration = total_duration.saturating_add(apply_changeset_acc);
        
        let mut total_width = 0;
        let mut avg_width = 0;

        let v = vec![
            new_session_acc,
            execute_script_acc,
            finish_session_acc,
            apply_changeset_acc,
            total_duration
        ];

        let mut res = Vec::with_capacity(v.len());
        for acc in v {
            let total = acc.as_millis();
            let avg = acc.as_nanos() / (1000 * nonce);
            
            total_width = max(total_width, total.to_string().len());
            avg_width = max(avg_width, avg.to_string().len());
            res.push((total, avg));
        }

        info!("MoveVM processed {} inputs", nonce);
        info!("MoveVM session creation:  total {total:>total_width$} ms, avg {avg:>avg_width$} μs",
            total = res[0].0, total_width = total_width,
            avg = res[0].1, avg_width = avg_width,
        );
        info!("MoveVM script execution:  total {total:>total_width$} ms, avg {avg:>avg_width$} μs",
            total = res[1].0, total_width = total_width,
            avg = res[1].1, avg_width = avg_width,
        );
        info!("MoveVM closing session:   total {total:>total_width$} ms, avg {avg:>avg_width$} μs",
            total = res[2].0, total_width = total_width,
            avg = res[2].1, avg_width = avg_width,
        );
        info!("MoveVM applying changset: total {total:>total_width$} ms, avg {avg:>avg_width$} μs",
            total = res[3].0, total_width = total_width,
            avg = res[3].1, avg_width = avg_width,
        );
        info!("MoveVM execution time:    total {total:>total_width$} ms, avg {avg:>avg_width$} μs",
            total = res[4].0, total_width = total_width,
            avg = res[4].1, avg_width = avg_width,
        ); 
    }
}

// ------------------------------------------------------------------------
struct DiemClient {
    account: AccountData,

    add_tx: DiemSignedTransaction,
    remove_tx: DiemSignedTransaction,
    txs: Vec<DiemSignedTransaction>,
    
    tx_block: Vec<DiemSignedTransaction>,
    tx_send: Sender<DiemMsg>,

    with_signature: bool,
}

impl DiemClient {
    pub async fn new(
        rate: u64,
        tx_send: Sender<DiemMsg>,
        mut info_receive: Receiver<(DiemSignedTransaction, DiemSignedTransaction, AccountData)>,
        with_signature: bool,
    ) -> Result<Self> {

        let (add_tx, remove_tx, account) = info_receive.recv().await.unwrap();

        let burst = rate / PRECISION;
        info!("Burst: {}", burst);
        
        let client = Self{
            account,
            add_tx: add_tx.clone(),
            remove_tx: remove_tx.clone(),
            txs: vec![add_tx, remove_tx],
            tx_block: Vec::with_capacity(burst as usize),
            tx_send,

            with_signature,
        };
        
        Ok(client)
    }
}

impl ClientTrait for DiemClient {
    fn send(&mut self, is_sample: bool, nonce: u64, counter: u64) -> Wrapper {
        if self.with_signature {
            // Dummy signing to see performance difference
            let tx = transfer_tx(&self.account, &self.account, &self.account);
        }

        let (mut tx, nonce) = if nonce % 2 == 0 {   // TODO Check == 1 or == 0
            (self.add_tx.clone(), nonce/2) // Alice tx
        } else {
            (self.remove_tx.clone(), nonce/2) // Bob tx
        };

        tx.raw_txn.sequence_number += nonce;

        // Send each burst as a block of transactions (should be beneficial for DiemVM performance)
        self.tx_block.push(tx);

        if self.tx_block.len() == self.tx_block.capacity() {
            let to_send = self.tx_block.clone();
            self.tx_block.clear();

            // Each burst contains a single sample transaction
            info!("Sending sample input {} from {}", counter, self.account.address());
            Wrapper::DiemLocal(&self.tx_send, (to_send, true))
        } else {
            Wrapper::Blank
        }
        
        // One by one ---------------------------
        // let to_send = vec![tx];
        // if is_sample {
        //     // info!("Signing transaction took {} ms", start.elapsed().as_millis());
        //     // info!("Sending sample input {} from {:?}", counter, self.alice);
        //     info!("Sending sample input {} from someone", counter);
        // }
        // Wrapper::DiemLocal(&self.tx_send, (to_send, is_sample))
    }

    fn transaction_size(&self) -> usize {
        return 0;
    }

    fn to_string(&self) -> String {
        // TODO Add DiemVM to utils.py::Mode and logs
        "DiemVM".to_string()
    }
}

// ------------------------------------------------------------------------
struct DiemNode {
    tx_receive: Receiver<DiemMsg>,
    executor: FakeExecutor,
}

impl DiemNode {

    pub async fn new(
        tx_receive: Receiver<DiemMsg>,
        info_send: Sender<(DiemSignedTransaction, DiemSignedTransaction, AccountData)>)
    -> Self {
        let mut executor = FakeExecutor::from_genesis_file();
        let owner = executor.create_raw_account_data(CONST_INITIAL_BALANCE, 10);
        let alice = executor.create_raw_account_data(CONST_INITIAL_BALANCE, 0);
        let bob = executor.create_raw_account_data(CONST_INITIAL_BALANCE, 0);

        executor.add_account_data(&owner);
        executor.add_account_data(&alice);
        executor.add_account_data(&bob);

        let mut nonce = 10;
        // Add resource/remove resource ----------------------------------------
        // publish module with add and remove resource
        // let (module, txn) = resource_module_tx(&owner, nonce);
        // nonce += 1;
        // execute_and_apply(txn, &mut executor, false);

        // // TODO use actual diem transfer script
        // let add = add_resource_script(&owner, vec![module.clone()]);
        // let remove = remove_resource_script(&owner, vec![module.clone()]);

        // let add_tx = owner
        //     .account()
        //     .transaction()
        //     .script(add)
        //     .sequence_number(nonce)
        //     .sign();
        // let remove_tx = owner
        //     .account()
        //     .transaction()
        //     .script(remove)
        //     .sequence_number(nonce)
        //     .sign();

        // Transfer BasicCoins -------------------------------------------------
        info!("Compiling and executing currency module...");
        let (_module, txn) = currency_module_tx(&owner, nonce);
        nonce += 1;
        execute_and_apply(txn, &mut executor, false);

        info!("Compiling and executing publish script (alice)...");
        let tx = publish_tx(&owner, &alice, nonce);
        nonce += 1;
        execute_and_apply(tx, &mut executor, false);

        let tx = publish_tx(&owner, &bob, nonce);
        nonce += 1;
        execute_and_apply(tx, &mut executor, false);

        let alice_tx = transfer_tx(&owner, &alice, &bob);

        let bob_tx = transfer_tx(&owner, &bob, &alice);

        info_send.send((alice_tx, bob_tx, owner)).await;

        Self {
            tx_receive,
            executor,
        }
    }

    pub async fn receive(&mut self, duration: u64) {

        info!("Start processing transactions");
        let timer = interval(Duration::from_secs(duration));
        tokio::pin!(timer);
        timer.tick().await; // Interval ticks immediately

        let mut counter = 0;
        let mut nonce = 0;
        loop {
            tokio::select!{
                _s = timer.tick() => {
                    info!("Diem Node: Reached end of benchmark");
                    break;
                }
                Some((tx_block, is_sample)) = self.tx_receive.recv() => {
                    if is_sample {
                        info!("Processing block containing sample {}", counter);
                    }
                    nonce += tx_block.len();
                    execute_and_apply_many(tx_block, &mut self.executor, false);
                    // execute_and_apply_many(tx_block, &mut self.executor, true);
                    if is_sample {
                        // NOTE: This log entry is used to compute performance.
                        info!("Processed sample input {}", counter);
                        counter += 1;
                    }
                }
            }
        }
        info!("Stop processing transactions");
        info!("DiemVM processed {} inputs", nonce);
    }
}

fn execute_and_apply(tx: DiemSignedTransaction, executor: &mut FakeExecutor, parallel: bool) {

    let tx_block = vec![tx];

    let output = executor.execute(tx_block, parallel);

    info!("{:?}", output[0].status());

    assert_eq!(
        output[0].status(),
        &TransactionStatus::Keep(KeptVMStatus::Executed)
    );

    executor.apply_write_set(output[0].write_set());
}

fn execute_and_apply_many(tx_block: Vec<DiemSignedTransaction>, executor: &mut FakeExecutor, parallel: bool) {
    let copy = tx_block.clone();
    let output = executor.execute(tx_block, parallel);

    for (out, _tx) in output.into_iter().zip(copy) {
        // warn!("Status output: {:?}\n\tfrom tx {:?}", out, tx);
        assert_eq!(
            out.status(),
            &TransactionStatus::Keep(KeptVMStatus::Executed)
        );
        executor.apply_write_set(out.write_set());
    }
}

fn publish_script_file() -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/publish_script.move".to_string());
    path.to_str().unwrap().to_string()
}

fn transaction_script_file() -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/transaction_script.move".to_string());
    path.to_str().unwrap().to_string()
}

fn publish_tx(owner: &AccountData, account: &AccountData, nonce: u64) -> DiemSignedTransaction {
    let args = vec![
        DiemArgument::Address(*account.address()),
        // DiemArgument::U64(CONST_INITIAL_BALANCE),
        DiemArgument::U64(1_000_000),
        ];
    let script = compile_currency_script(&owner, publish_script_file(), args);

    owner
        .account()
        .transaction()
        .script(script)
        .secondary_signers(vec![account.account().clone()])
        .sequence_number(nonce)
        .sign_multi_agent()
}

fn transfer_tx(owner: &AccountData, from: &AccountData, to: &AccountData) -> DiemSignedTransaction {
    let args = vec![
        DiemArgument::Address(*to.address()),
        DiemArgument::U64(NORMAL_TX_AMOUNT),
        ];
    let script = compile_currency_script(&owner, transaction_script_file(), args);

    from
        .account()
        .transaction()
        .script(script)
        .sequence_number(0)
        .sign()
}

fn compile_currency_script(owner: &AccountData, script_file: String, args: Vec<DiemArgument>) -> Script {
    let mut deps = diem_framework::diem_stdlib_files();
    deps.push(currency_module_file());

    let (_files, mut compiled_program) =
    move_compiler_01::Compiler::new(&[script_file], &deps)
            .set_flags(move_compiler_01::Flags::empty().set_sources_shadow_deps(false))
            .set_named_address_values(make_named_addresses(owner))
            .build_and_report()
            .unwrap();
    assert!(compiled_program.len() == 1);
    let script = match compiled_program.pop().unwrap() {
        AnnotatedCompiledUnit::Module(_) => panic!("Unexpected module when compiling script"),
        AnnotatedCompiledUnit::Script(annot_script) => annot_script.named_script.script,
    };
    
    let mut blob = Vec::<u8>::new();
        script.serialize(&mut blob)
        .expect("Script compilation failed");

    Script::new(
        blob,
        vec![],
        args,
    )
}

fn compile_currency_module(owner: &AccountData) -> CompModule {
    // c.f. admin_script_builder
    let (_files, mut compiled_program) =
    move_compiler_01::Compiler::new(&[currency_module_file()], &diem_framework::diem_stdlib_files())
            .set_flags(move_compiler_01::Flags::empty().set_sources_shadow_deps(false))
            .set_named_address_values(make_named_addresses(owner))
            .build_and_report()
            .unwrap();
    assert!(compiled_program.len() == 1);
    match compiled_program.pop().unwrap() {
        AnnotatedCompiledUnit::Module(annot_module) => annot_module.named_module.module,
        _x @ AnnotatedCompiledUnit::Script(_) => panic!("Unexpected script when compiling module"),
    }
}

fn make_named_addresses(owner: &AccountData) -> BTreeMap<String, NumericalAddress> {
    let pairs = [("Currency", owner.address().to_hex_literal())];

    let mut mapping: BTreeMap<String, NumericalAddress> = pairs
        .iter()
        .map(|(name, addr)| (name.to_string(), NumericalAddress::parse_str(addr).unwrap()))
        .collect();

    let mut std_mapping = diem_framework::diem_framework_named_addresses();
    mapping.append(&mut std_mapping);

    mapping
}

fn currency_module_tx(owner: &AccountData, seq_num: u64) -> (CompModule, DiemSignedTransaction) {

    let module = compile_currency_module(&owner);

    let mut module_blob = vec![];
    module
        .serialize(&mut module_blob)
        .expect("Module must serialize");
    verify_module(&module).expect("Module must verify");
    // info!("Module compiled successfully");
    (
        module,
        owner
            .account()
            .transaction()
            .module(Module::new(module_blob))
            .sequence_number(seq_num)
            .sign(),
    )
}

// ----------------------------------------------------
fn resource_module_tx(sender: &AccountData, seq_num: u64) -> (CompModule, DiemSignedTransaction) {
    let module_code = format!(
        "
        module 0x{}.M {{
            import 0x1.Signer;
            struct T1 has key {{ v: u64 }}

            public borrow_t1(account: &signer) acquires T1 {{
                let t1: &Self.T1;
            label b0:
                t1 = borrow_global<T1>(Signer.address_of(move(account)));
                return;
            }}

            public change_t1(account: &signer, v: u64) acquires T1 {{
                let t1: &mut Self.T1;
            label b0:
                t1 = borrow_global_mut<T1>(Signer.address_of(move(account)));
                *&mut move(t1).T1::v = move(v);
                return;
            }}

            public remove_t1(account: &signer) acquires T1 {{
                let v: u64;
            label b0:
                T1 {{ v }} = move_from<T1>(Signer.address_of(move(account)));
                return;
            }}

            public publish_t1(account: &signer) {{
            label b0:
                move_to<T1>(move(account), T1 {{ v: 3 }});
                return;
            }}
        }}
        ",
        sender.address(),
    );

    let compiler = MoveIRCompiler {
        deps: diem_framework_releases::current_modules().iter().collect(),
    };
    let module = compiler
        .into_compiled_module(module_code.as_str())
        .expect("Module compilation failed");

    let mut module_blob = vec![];
    module
        .serialize(&mut module_blob)
        .expect("Module must serialize");
    verify_module(&module).expect("Module must verify");
    (
        module,
        sender
            .account()
            .transaction()
            .module(Module::new(module_blob))
            .sequence_number(seq_num)
            .sign(),
    )
}

fn add_resource_script(sender: &AccountData, extra_deps: Vec<CompModule>) -> Script {
    let program = format!(
        "
            import 0x{}.M;

            main(account: signer) {{
            label b0:
                M.publish_t1(&account);
                return;
            }}
        ",
        sender.address(),
    );

    let s = Instant::now();
    info!("Compiling {} program", current_function_name!());
    let script = compile_script(&program, extra_deps);
    info!("Compilation of {} took {} ms", current_function_name!(), s.elapsed().as_millis());

    script
}

fn remove_resource_script(sender: &AccountData, extra_deps: Vec<CompModule>) -> Script {
    let program = format!(
        "
            import 0x{}.M;

            main(account: signer) {{
            label b0:
                M.remove_t1(&account);
                return;
            }}
        ",
        sender.address(),
    );

    let s = Instant::now();
    info!("Compiling {} program", current_function_name!());
    let script = compile_script(&program, extra_deps);
    info!("Compilation of {} took {} ms", current_function_name!(), s.elapsed().as_millis());

    script
}
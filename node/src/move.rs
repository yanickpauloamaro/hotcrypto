#![allow(unused)]
mod node;
mod config;
mod compiler;
mod transaction;

use crate::config::Export as _;
use crate::config::{ConfigError, Secret};
use crate::compiler::{Compiler, currency_named_addresses};
use crate::transaction::{SignedTransaction, Transaction, Account, Register};
use crate::node::Node;

use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, SubCommand, ArgMatches};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn, error};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crypto::{SecretKey, Signature, Hash as Digestable};
use rand::seq::SliceRandom;
use bytes::Bytes;

use move_core_types::transaction_argument::TransactionArgument;
use move_vm_runtime::move_vm::MoveVM;
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas_schedule::GasStatus;
use move_core_types::{
    value::MoveValue,
    account_address::AccountAddress
};

use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::oneshot;

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
                .about("Benchmark client for HotStuff nodes.")
                .args_from_usage("--rate=[INT] 'The rate (txs/s) at which to send the transactions'")
                .args_from_usage("--duration=[INT] 'The duration (s) the benchmark will last'")
        )
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
    .format_timestamp_millis()
    .init();

    match matches.subcommand() {
        ("keys", Some(subm)) => {
            let filename = subm.value_of("filename").unwrap();
            if let Err(e) = Client::print_key_file(filename) {
                error!("{}", e);
            }
        }
        ("run", Some(subm)) => {
            let _ = run(subm).await;
        }
        _ => unreachable!()
    }
}

// const DEFAULT_DURATION: u64 = 10;
// const DEFAULT_RATE: u64 = 100;

// fn get_duration() -> u64 {
//     env::var("BENCHMARK_DURATION").ok()
//     .map(|s| s.parse::<u64>().ok()).flatten()
//     .unwrap_or(DEFAULT_DURATION)
// }

// fn get_rate() -> u64 {
//     env::var("BENCHMARK_RATE").ok()
//         .map(|s| s.parse::<u64>().ok()).flatten()
//         .unwrap_or(DEFAULT_RATE)
// }

async fn run<'a>(matches: &ArgMatches<'_>) -> Result<()> {

    // let duration = get_duration();
    // let rate = get_rate();
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

    let client = Client::new().expect("Failed to create client");


    benchmark(rate, duration, client).await
}

async fn transaction_sender(
    rate: u64,
    duration: u64,
    // tx_send: Sender<(SignedTransaction, bool)>,
    tx_send: Sender<(Vec<u8>, Vec<MoveValue>, bool)>,
    client: Client
) -> Result<()> {

    const PRECISION: u64 = 20; // Sample precision.
    const BURST_DURATION: u64 = 1000 / PRECISION;

    let burst = rate / PRECISION;
    info!("Transactions rate: {} tx/s", rate);
    info!("Benchmark duration: {} s", duration);
    info!("Burst: {}", burst);

    let interval = interval(Duration::from_millis(BURST_DURATION));
    tokio::pin!(interval);

    let script_blob = Client::script_blob().expect("Failed to create transfer blob");
    // let sample_blob = Client::sample_blob().expect("Failed to create sample blob");

    // NOTE: This log entry is used to compute performance.
    info!("Start sending transactions");
    let start = Instant::now();

    let mut nonce = 0;
    let mut counter = 0;
    // let mut r = rand::thread_rng();
    'main: loop {
        interval.as_mut().tick().await;
        let now = Instant::now();
        
        for x in 0..burst {

            let mut amount = 1;
            let mut source = client.account.clone();
            let mut other = client.register.accounts[0];
            let (from, dest) = if x % 2 == 0 {
                (source, other)
            } else {
                (other, source)
            };

            let mut script = script_blob.clone();
            let is_sample = x == counter % burst;
            if is_sample {
                // NOTE: This log entry is used to compute performance.
                info!("Sending sample transaction {} from {:?}", nonce, from.address);
                // amount = SAMPLE_TX_AMOUNT; // This amount identifies sample transactions
                amount = 2; // This amount identifies sample transactions
                // script = sample_blob.clone();
            };
            
            let mut args = vec![
                MoveValue::Signer(from.address),
                MoveValue::Address(dest.address),
                MoveValue::U64(amount),
            ];

            tx_send.send((script.clone(), args, is_sample)).await;

            nonce += 1;
        }

        let took = now.elapsed().as_millis();
        if took > BURST_DURATION as u128 {
            // NOTE: This log entry is used to compute performance.
            warn!("Transaction rate too high for this client (took {} ms)", took);
        }

        if start.elapsed().as_millis() > (duration * 1000) as u128 {
            // NOTE: This log entry is used to compute performance.
            info!("Stop sending transactions");
            return Ok(());
        }

        counter += 1;
    }
}

async fn benchmark(rate: u64, duration: u64, client: Client) -> Result<()> {
    // Prepare vm preparation -------------------------------------------------------------------------
    let (vm, mut storage) = Node::init_vm(".register.json")
            .expect("Unable initialize move vm");

    // Link to transaction sender ---------------------------------------------------
    let channel_capacity = 100 * rate as usize;
    info!("Channel capacity: {}", channel_capacity);
    let (tx_send, mut tx_receive) = channel(channel_capacity);

    tokio::spawn(transaction_sender(rate, duration, tx_send, client));

    // Process transactions -------------------------------
    info!("Start processing transactions");
    let start = Instant::now();
    let mut nonce = 0;
    let mut gas_status = GasStatus::new_unmetered();
    // while let Some((tx, is_sample)) = tx_receive.recv().await {
    while let Some((script, args, is_sample)) = tx_receive.recv().await {

        let s = Instant::now();
        let mut sess = vm.new_session(&storage);
        let err = sess.execute_script(
            script,
            vec![], // ty_args: Vec<TypeTag>
            args.iter()
                .map(|arg| arg.simple_serialize().unwrap())
                .collect(),
            &mut gas_status,
        )
        // .map(|_| ());
        .map(|res| {
            for (blob, ty) in res.return_values {
                let move_value = MoveValue::simple_deserialize(&blob, &ty);
                info!("VM output = {:?}", move_value);
            }
        });

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
        storage.apply(changeset).unwrap();

        if is_sample {
            // NOTE: This log entry is used to compute performance.
            info!("Processed sample transaction {} (took {} ms)", nonce, s.elapsed().as_millis());
        }
        // else {
        //     info!("Processed NORMAL transaction {} (took {} ms)", nonce, s.elapsed().as_millis());
        // }

        if start.elapsed().as_millis() > (duration * 1000) as u128 {
            info!("Ran for long enough");
            break;
        }
        nonce += 1;
    }

    info!("Stop processing transactions");
    Ok(())
}

struct Client {
    account: Account,
    register: Register
}

impl Client {
    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub fn new() -> Result<Self, ConfigError> {

        let secret = Secret::read(".client-0.json")?;
        let mut register = Register::read(".register.json")?;
        register.accounts.retain(|account| account.public_key != secret.name);

        info!("Account {:?}", secret.name);

        Some(register.accounts.len())
            .filter(|s| *s > 0)
            .expect("There must be at least one other account");

        info!("Other accounts {:?}", register.accounts);

        Ok(Self{
            account: Account::new(secret.name),
            register
        })
    }

    pub fn script_blob() -> Result<Vec<u8>> {

        let script_code = format!("
            import {}.BasicCoin;

            main(account: signer, to: address, amount: u64) {{
                label b0:
                    BasicCoin.transfer(&account, move(to), move(amount));
                    return;
            }}
        ",
        currency_named_addresses().get("Currency").unwrap());

        let compiler = Compiler::new()?;

        compiler.into_script_blob(&script_code)
    }

    pub fn sample_blob() -> Result<Vec<u8>> {

        let script_code = format!("
            import {}.BasicCoin;
            import {}.Debug;

            main(account: signer) {{
                let res: u64;
                label b0:
                    res = BasicCoin.balance_of(&account);
                    Debug.print(&res);
                    return;
            }}
        ",
        currency_named_addresses().get("Currency").unwrap(),
        currency_named_addresses().get("Std").unwrap());

        let compiler = Compiler::new()?;

        compiler.into_script_blob(&script_code)
    }
}
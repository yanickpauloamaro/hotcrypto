#![allow(unused)]
mod currency;
mod node;
mod config;

use crate::config::Export as _;
use crate::config::{ConfigError, Secret};

use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
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

use currency::{SignedTransaction, Transaction, Account, Register, SAMPLE_TX_AMOUNT};
use crypto::{SecretKey, Signature, Hash as Digestable};
use rand::seq::SliceRandom;
use bytes::Bytes;

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
                .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
                .args_from_usage("--timeout=<INT> 'The nodes timeout value'")
                .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
                .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
                .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--accounts=[FILE] 'The file containing accounts addresses'")
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

async fn run<'a>(matches: &ArgMatches<'_>) -> Result<()> {
    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let timeout = matches
        .value_of("timeout")
        .unwrap()
        .parse::<u64>()
        .context("The timeout value must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    let key_file = matches.value_of("keys").unwrap();
    let register_file = matches.value_of("accounts").unwrap();

    let client = Client::new(
        target,
        // size,
        rate,
        timeout,
        nodes,
        key_file,
        register_file
    ).context("Failed to create client")?;
    
    info!("Node address: {}", target);
    info!("Transactions size: {} B", client.transaction_size());
    info!("Transactions rate: {} tx/s", rate);

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    // size: usize,
    rate: u64,
    timeout: u64,
    nodes: Vec<SocketAddr>,
    secret_key: SecretKey,
    account: Account,
    register: Register
}

impl Client {
    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub fn new(
        target: SocketAddr,
        // size: usize,
        rate: u64,
        timeout: u64,
        nodes: Vec<SocketAddr>,
        key_file: &str,
        register_file: &str,
    ) -> Result<Self, ConfigError> {

        let secret = Secret::read(key_file)?;
        let mut register = Register::read(register_file)?;
        register.accounts.retain(|account| *account != secret.name);

        info!("Account {:?}", secret.name);

        Some(register.accounts.len())
            .filter(|s| *s > 0)
            .expect("There must be at least one other account");

        info!("Other accounts {:?}", register.accounts);

        Ok(Self{
            target,
            // size,
            rate,
            timeout,
            nodes,
            secret_key: secret.secret,
            account: secret.name,
            register
        })
    }

    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut counter = 0;
        let mut nonce = 0;
        let mut r = rand::thread_rng();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {

                let mut amount = 1;
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {} from {:?}", nonce, self.account);
                    amount = SAMPLE_TX_AMOUNT; // This amount identifies sample transactions
                };

                let dest = *self.register.accounts.choose(&mut r).unwrap();

                let transaction = Transaction{
                    source: self.account,
                    dest: dest,
                    amount: amount,
                    nonce: nonce
                };

                let signature = Signature::new(&transaction.digest(), &self.secret_key);
                let signed = SignedTransaction{
                    content: transaction,
                    signature
                };

                let bytes = Bytes::from(signed);

                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                };

                nonce += 1;
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    fn transaction_size(&self) -> u64 {
        let transaction = Transaction {
            source: self.account,
            dest: self.account,
            amount: 0,
            nonce: 0
        };

        let signature = Signature::new(&transaction.digest(), &self.secret_key);
        let signed = SignedTransaction {
            content: transaction,
            signature
        };

        let size = bincode::serialized_size(&signed)
            .expect("Failed to serialize signed transaction");

        return size;
    }

    pub async fn wait(&self) {
        // First wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;

        // Then wait for the nodes to be synchronized.
        info!("Waiting for all nodes to be synchronized...");
        sleep(Duration::from_millis(2 * self.timeout)).await;
    }
}

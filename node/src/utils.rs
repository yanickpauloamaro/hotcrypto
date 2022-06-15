use crate::compiler::{Compiler, currency_named_addresses};
use crate::config::{ConfigError, Secret, Register};
use crate::config::Export as _;
use crate::transaction::CONST_INITIAL_BALANCE;

pub use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::ArgMatches;
use log::info;
use futures::future::join_all;
use std::str::FromStr;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use move_core_types::{
    value::MoveValue,
    account_address::AccountAddress
};
use move_vm_runtime::move_vm::MoveVM;
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas_schedule::GasStatus;

pub trait Argument{
    type Output;

    const USAGE: &'static str;
    fn usage() -> &'static str {
        Self::USAGE
    }
    fn from_matches(matches: &ArgMatches<'_>) -> Result<Self::Output>;
}

impl Argument for SocketAddr {
    type Output = Self;
    const USAGE: &'static str = "
        <ADDR> 'The network address of the node where to send txs'
    ";

    fn from_matches(matches: &ArgMatches<'_>) -> Result<Self::Output> {
        matches
            .value_of("ADDR")
            .unwrap()
            .parse::<SocketAddr>()
            .context("Invalid socket address format")
    }
}

impl Argument for Vec<SocketAddr> {
    type Output = Self;

    const USAGE: &'static str = "
        --nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'
    ";

    fn from_matches(matches: &ArgMatches<'_>) -> Result<Self::Output> {
        matches
            .values_of("nodes")
            .unwrap_or_default()
            .into_iter()
            .map(|x| x.parse::<SocketAddr>())
            .collect::<Result<Vec<_>, _>>()
            .context("Invalid socket address format")
    }
}

#[derive(Debug, Clone)]
pub enum Mode {
    HotStuff,
    HotCrypto,
    HotMove,
    MoveVM
}

impl FromStr for Mode {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s {
            "hotstuff" =>   Mode::HotStuff,
            "hotcrypto" =>  Mode::HotCrypto,
            "hotmove" =>    Mode::HotMove,
            "movevm" =>     Mode::MoveVM,
            _ =>            Mode::HotMove, 
        };

        Ok(res)
    }
}



pub type Transport = Framed<tokio::net::TcpStream, LengthDelimitedCodec>;
pub async fn connect(target: SocketAddr) -> Result<Transport> {
    // Connect to the mempool.
    let stream = TcpStream::connect(target)
        .await
        .context(format!("failed to connect to {}", target))?;

    let transport = Framed::new(stream, LengthDelimitedCodec::new());

    Ok(transport)
}

pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
    Secret::new().write(filename)
}

pub async fn wait(nodes: Vec<SocketAddr>, timeout: u64) {
    // First wait for all nodes to be online.
    info!("Waiting for all nodes to be online...");
    join_all(nodes.iter().cloned().map(|address| {
        tokio::spawn(async move {
            while TcpStream::connect(address).await.is_err() {
                sleep(Duration::from_millis(10)).await;
            }
        })
    }))
    .await;

    // Then wait for the nodes to be synchronized.
    info!("Waiting for all nodes to be synchronized...");
    sleep(Duration::from_millis(2 * timeout)).await;
}

pub fn init_vm(register: &Register) -> Result<(MoveVM, InMemoryStorage)> {

    info!("Setting up MoveVM...");
    let mut gas_status = GasStatus::new_unmetered();

    let compiler = Compiler::new().expect("Failed to create compiler");

    let vm = MoveVM::new(move_stdlib::natives::all_natives(
        AccountAddress::from_hex_literal("0x1").unwrap(),
    ))
    .unwrap();

    let mut storage = InMemoryStorage::new();

    for module in compiler.dependencies() {
        info!("Publishing and loading module {}", module.self_id().short_str_lossless());

        let mut blob = vec![];
        module.serialize(&mut blob).unwrap();

        storage.publish_or_overwrite_module(module.self_id(), blob);

        vm.load_module(&module.self_id(), &storage).expect("Failed to load module");
    }

    info!("Creating accounts for all clients...");
    let mut sess = vm.new_session(&storage);

    let module_address = AccountAddress::from_hex_literal("0xCAFE").unwrap();
    let module_signer = MoveValue::Signer(module_address);

    let publish_and_mint = publish_and_mint()?;

    for account in &register.accounts {
        info!("Creating account {:?}", account.address);
        
        let mut args = vec![module_signer.clone()];
        args.push(MoveValue::Signer(account.address));
        args.push(MoveValue::Address(account.address));
        args.push(MoveValue::U64(CONST_INITIAL_BALANCE));
        let err = sess.execute_script(
            publish_and_mint.clone(),
            vec![], // ty_args: Vec<TypeTag>
            args.iter()
                .map(|arg| arg.simple_serialize().unwrap())
                .collect(),
            &mut gas_status,
        )
        .map(|_| ());

        err.unwrap();
    }

    let (changeset, _) = sess
        .finish()
        .unwrap();
    storage.apply(changeset).unwrap();

    Ok((vm, storage))
}

pub fn publish_and_mint() -> Result<Vec<u8>> {

    let script_code = format!("
            import {}.BasicCoin;

            main(owner: signer, account: signer, account_addr: address, amount: u64) {{
                label b0:
                    BasicCoin.publish_balance(&account);
                    BasicCoin.mint(&owner, move(account_addr), move(amount));
                    return;
            }}
        ",
        currency_named_addresses().get("Currency").unwrap()
    );

    let compiler = Compiler::new()?;

    compiler.into_script_blob(&script_code)
}
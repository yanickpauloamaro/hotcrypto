#![allow(unused)]
mod config;
mod node;
mod transaction;
mod compiler;
mod utils;

use crate::config::Export as _;
use crate::config::{Committee, Secret, Register};
use crate::node::Node;
use crate::transaction::{Account};
use crate::utils::print_key_file;

use clap::{crate_name, crate_version, App, AppSettings, SubCommand};
use consensus::Committee as ConsensusCommittee;
use env_logger::Env;
use futures::future::join_all;
use log::{warn, error};
use mempool::Committee as MempoolCommittee;
use std::fs;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of the HostStuff protocol.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Runs a single node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .args_from_usage("--port=<PORT> 'The port from which nodes will listen for requests'")
                .args_from_usage("--accounts=[FILE] 'The file containing accounts addresses'")
                .args_from_usage("[MODE] 'One of: hotstuff, hotcrypto, hotmove, movevm (default is hotstuff)'"),
        )
        .subcommand(
            SubCommand::with_name("deploy")
                .about("Deploys a network of nodes locally")
                .args_from_usage("--nodes=<INT> 'The number of nodes to deploy'")
                .args_from_usage("[MODE] 'One of: hotstuff, hotcrypto, hotmove, movevm (default is hotstuff)'"),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("keys", Some(subm)) => {
            let filename = subm.value_of("filename").unwrap();
            if let Err(e) = print_key_file(filename) {
                error!("{}", e);
            }
        }
        ("run", Some(subm)) => {
            let key_file = subm.value_of("keys").unwrap();
            let committee_file = subm.value_of("committee").unwrap();
            let parameters_file = subm.value_of("parameters");
            let store_path = subm.value_of("store").unwrap();
            let port = subm.value_of("port").unwrap();
            let register_file = subm.value_of("accounts").unwrap();
            let mode = subm.value_of("MODE").unwrap_or_default();

            match Node::new(committee_file, key_file, store_path, parameters_file, port, register_file, mode).await {
                Ok(mut node) => {
                    tokio::spawn(async move {
                        node.analyze_block().await;
                    })
                    .await
                    .expect("Failed to analyze committed blocks");
                }
                Err(e) => error!("{}", e),
            }
        }
        ("deploy", Some(subm)) => {
            let nodes = subm.value_of("nodes").unwrap();
            let mode = subm.value_of("MODE").unwrap_or_default();
            match nodes.parse::<usize>() {
                Ok(nodes) if nodes > 1 => match deploy_testbed(nodes, mode) {
                    Ok(handles) => {
                        let _ = join_all(handles).await;
                    }
                    Err(e) => error!("Failed to deploy testbed: {}", e),
                },
                _ => error!("The number of nodes must be a positive integer"),
            }
        }
        _ => unreachable!(),
    }
}

fn deploy_testbed(nodes: usize, mode: &str) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {
    let keys: Vec<_> = (0..nodes).map(|_| Secret::new()).collect();

    // Print the committee file.
    let epoch = 1;
    let mempool_committee = MempoolCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = 1;
                let front = format!("127.0.0.1:{}", 25_000 + i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", 25_100 + i).parse().unwrap();
                (name, stake, front, mempool)
            })
            .collect(),
        epoch,
    );
    let consensus_committee = ConsensusCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = 1;
                let addresses = format!("127.0.0.1:{}", 25_200 + i).parse().unwrap();
                (name, stake, addresses)
            })
            .collect(),
        epoch,
    );
    let committee_file = "committee.json";
    let _ = fs::remove_file(committee_file);
    Committee {
        mempool: mempool_committee,
        consensus: consensus_committee,
    }
    .write(committee_file)?;

    // Print register file
    let accounts: Vec<_> = keys.iter().map(|secret| Account::new(secret.name)).collect();
    let register_file = "register.json";
    let _ = fs::remove_file(register_file);
    Register{
        accounts
    }
    .write(register_file)?;

    // Write the key files and spawn all nodes.
    keys.iter()
        .enumerate()
        .map(|(i, keypair)| {
            let key_file = format!("node_{}.json", i);
            let _ = fs::remove_file(&key_file);
            keypair.write(&key_file)?;

            let store_path = format!("db_{}", i);
            let _ = fs::remove_dir_all(&store_path);

            let port = format!("{}", 25_300 + i);

            let mode = mode.to_string();

            Ok(tokio::spawn(async move {
                match Node::new(committee_file, &key_file, &store_path, None, &port, register_file, &mode).await {
                    Ok(mut node) => {
                        // Sink the commit channel.
                        while node.commit.recv().await.is_some() {}
                    }
                    Err(e) => error!("{}", e),
                }
            }))
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()
}

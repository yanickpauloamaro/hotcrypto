> **Note to readers:** This repo is part of my Master semester project at EPFL. I use code from the [Diem foundation](https://github.com/diem) and from Alberto Sonnino's implementation of [HotStuff](https://github.com/asonnino/hotstuff). This README contains the abstract of my project as well as a modified version of the README from HotStuff.

# Towards efficient and safe blockchain

Blockchain is an innovative technology with promising uses in finance, decentralized computing and market platforms. These applications require high performance to be able to reach a truly global scale.

Unfortunately, current blockchains do not reach the tens of thousands of transactions per second necessary for this goal. This project explores bottlenecks in blockchains by building a simple smart-contract system using the same components as Diem, a state of the art blockchain.

We show that consensus is the best performing component of blockchain systems and that cryptography and virtual machines are bottlenecks. We also show that our simplistic system is able to sustain much higher throughput that Diem while using the same components, indicating that the architecture of blockchains have a large impact on performance, independently of the components used.

This repo provides a minimal implementation of a smart-contract system based on HotStuff consensus and the [Move virtual machine](https://github.com/diem/move). The codebase has been designed to be small, efficient, and easy to benchmark and modify. It has not been designed to run in production but uses real cryptography ([dalek](https://doc.dalek.rs/ed25519_dalek)), networking ([tokio](https://docs.rs/tokio)), and storage ([rocksdb](https://docs.rs/rocksdb)).

## Quick Start
This system is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:
```
$ git clone https://github.com/yanickpauloamaro/hotcrypto
$ cd hotstuff/benchmark
$ pip install -r requirements.txt
```
You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:
```
$ fab local
```
This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.
```
-----------------------------------------
 SUMMARY: HotStuff 
-----------------------------------------
 + CONFIG:
 Faults: 0 nodes
 Committee size: 3 nodes
 Transaction size: 128 B

 Consensus timeout delay: 1,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool GC depth: 50 rounds
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 15,000 B
 Mempool max batch delay: 10 ms

-----------------------------------------
 + INFO:
 Input rate: 300 tx/s
 Number of cores: 4
 Number of accounts: 3
 Execution time: 20 s
 Benchmark start: 15-06-2022 10:05:48
 Benchmark end:   15-06-2022 10:06:08
-----------------------------------------
 + RESULTS:
 Consensus TPS: 300 tx/s
 Consensus BPS: 38,456 B/s
 Consensus latency: 2 ms

 End-to-end TPS: 300 tx/s
 End-to-end BPS: 38,444 B/s
 End-to-end latency: 9 ms
-----------------------------------------
```

## Next Steps
The [wiki](https://github.com/yanickpauloamaro/hotcrypto/wiki) documents the codebase, explains its architecture and how to read benchmarks' results, and provides a step-by-step tutorial to run [benchmarks on Amazon Web Services](https://github.com/yanickpauloamaro/hotcrypto/wiki/AWS-Benchmarks) accross multiple data centers (WAN).

## License
This software is licensed as [Apache 2.0](LICENSE).

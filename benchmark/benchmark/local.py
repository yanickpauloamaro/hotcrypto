import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, Register, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker, progress_bar


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, mode, debug=False):
        assert isinstance(debug, bool)
        Print.heading(f'Starting local benchmark of {mode}')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            Print.info(f'Recompiling...')
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files for nodes.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            # Generate configuration files for clients.
            client_keys = []
            client_key_files = [PathMaker.key_file(i, 'client') for i in range(nodes)]
            for filename in client_key_files:
                cmd = CommandMaker.generate_key(filename, 'client').split()
                subprocess.run(cmd, check=True)
                client_keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT)
            committee.print(PathMaker.committee_file())

            client_names = [x.name for x in client_keys]
            register = Register(client_names)
            register.print(PathMaker.register_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Do not boot faulty nodes.
            nodes = nodes - self.faults

            # Run the clients (they will wait for the nodes to be ready).
            addresses = committee.front
            request_ports = committee.request_ports
            rate_share = rate if mode == 'movevm' else ceil(rate / nodes)
            timeout = self.node_parameters.timeout_delay

            client_logs = [PathMaker.client_log_file(i) for i in range(nodes)]
            for key_file, addr, port, log_file in zip(client_key_files, addresses, request_ports, client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    self.tx_size,
                    rate_share,
                    timeout,
                    self.duration,
                    key_file,
                    PathMaker.register_file(),
                    mode
                )
                self._background_run(cmd, log_file)

                if mode == 'movevm':
                    # Only run one client for benchmarking MoveVM
                    break

            if mode != 'movevm':
                # Run the nodes.
                dbs = [PathMaker.db_path(i) for i in range(nodes)]
                node_logs = [PathMaker.node_log_file(i) for i in range(nodes)]
                for key_file, db, log_file, port in zip(key_files, dbs, node_logs, request_ports):
                    cmd = CommandMaker.run_node(
                        key_file,
                        PathMaker.committee_file(),
                        db,
                        PathMaker.parameters_file(),
                        port,
                        PathMaker.register_file(),
                        mode,
                        debug=debug
                    )
                    self._background_run(cmd, log_file)

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(2 * self.node_parameters.timeout_delay / 1000)

            # Wait for all transactions to be processed.
            for _ in progress_bar(range(20), prefix=f'Running benchmark ({self.duration} sec):'):
                sleep(ceil(self.duration / 20))

            sleep(2) # Make sure the clients have time to stop on their own
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process('./logs', faults=self.faults, nb_accounts=nodes)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)

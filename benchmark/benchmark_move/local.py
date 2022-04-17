import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark_move.commands import CommandMaker
from benchmark_move.config import Key, NodeParameters, BenchParameters, ConfigError
from benchmark_move.logs import LogParser, ParseError
from benchmark_move.utils import Print, BenchError, PathMaker
from benchmark.config import Register


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        
        self.bench_parameters.nodes = [2]

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

    def run(self, debug=False, parallel=0):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

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
            Print.info('Recompiling...')
            cmd = CommandMaker.compile(1).split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files for clients. ##
            client_keys = []
            # Print.info('Generating client keys...')
            client_key_files = [PathMaker.key_file(i, 'client') for i in range(nodes)]
            for filename in client_key_files:
                cmd = CommandMaker.generate_key(filename, 'client').split()
                subprocess.run(cmd, check=True)
                client_keys += [Key.from_file(filename)]

            client_names = [x.name for x in client_keys]    ##
            register = Register(client_names)  ##
            register.print(PathMaker.register_file())   ##

            # Do not boot faulty nodes.
            nodes = nodes - self.faults

            # Run the clients (they will wait for the nodes to be ready).
            rate_share = rate #ceil(rate / nodes)

            # for addr, log_file in zip(addresses, client_logs):
            log_file = PathMaker.client_log_file(0)
            cmd = CommandMaker.run_client(
                rate_share,
                self.duration,
                parallel
            )
            self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration + 1)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process('./logs', faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)

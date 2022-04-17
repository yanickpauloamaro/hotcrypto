from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess

from benchmark_move.config import Committee, Key, NodeParameters, BenchParameters, ConfigError, Register
from benchmark_move.utils import BenchError, Print, PathMaker, progress_bar
from benchmark_move.commands import CommandMaker
from benchmark_move.logs import LogParser, ParseError
from benchmark_move.instance import InstanceManager

class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        self.client_log_id = 0
        assert len(self.manager.clients.values()) == 1

        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the Rocksdb installer (needed for Rocksdb).
            'sudo apt-get install -y clang',

            'sudo apt-get install -y unzip',

            # Clone the repo.
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))',
        ]
        cmd_diem = [
            # Run dependency script
            f'(cd {self.settings.repo_name} ; yes | ./scripts/dev_setup.sh v)',
            'source ~/.cargo/env',
            ]
        hosts = self.manager.hosts(flat=True)
        try:
            ## Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)

            Print.info('Installing rust and cloning the diem repo...')
            g.run(' && '.join(cmd), hide=True)

            Print.info('Installing Diem dependencies...')
            g.run(' && '.join(cmd_diem), hide=True)

            Print.info('Uploading repo...')
            ## Send repo to the instances
            # self._upload(public_ips, cd=True)
            self._upload(public_ips)

            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            # Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        return list(*self.manager.hosts().values())

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "({command} |& tee {log_file})"'
        c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _upload(self, public_ips, cd=False):
        Print.info('Compressing repo...')
        repo_name = 'hotcrypto'

        filename = repo_name #self.settings.repo_name
        zip_name = repo_name #self.settings.repo_name
        cmd = CommandMaker.compress_repo(filename, zip_name)
        subprocess.run([cmd], check=True, shell=True)

        # Upload files.
        progress = progress_bar(public_ips, prefix='Uploading repo:')
        for _, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.put(f'{zip_name}.zip', '.')

        Print.info('Decompressing...')
        decompress = CommandMaker.decompress_repo(repo_name)
        cmd = [
            f'(({decompress}) || true)'
        ]

        if cd:
            cmd += [f'cd {filename}']

        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def remove(self):
        hosts = self.manager.hosts(flat=True)
        filename = 'hotcrypto' ##
        try:
            cmd = f'{CommandMaker.remove_repo(filename)} || true'

            # Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
            g.run(cmd, hide=True)

        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _update(self, hosts, parallel=0):
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...'
        )

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]

        ## Reupload code (Only need to reupload if there was any changes to rust code)
        # self._upload(public_ips)

        jobs = 2
        repo_name = 'hotcrypto'
        Print.info(f'Recompiling with {jobs} jobs...')
        cmd = [
            'source $HOME/.cargo/env',
            f'(cd {repo_name}/node && {CommandMaker.compile(jobs)})',
            CommandMaker.alias_binaries(
                f'./{repo_name}/target/release/'
            )
        ]

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]
        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=False, parallel=0):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        rate_share = rate

        log_file = PathMaker.client_log_file(self.client_log_id)

        cmd = CommandMaker.run_client(
            rate_share,
            bench_parameters.duration,
            parallel
        )

        self._background_run(hosts[0], cmd, log_file)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration + 1
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))

        self.kill(hosts=hosts, delete_logs=False)

    def _get_logs(self, hosts):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        c = Connection(hosts[0].public, user='ubuntu', connect_kwargs=self.connect)
        c.get(
            PathMaker.client_log_file(self.client_log_id), local=PathMaker.client_log_file(self.client_log_id)
        )

    def _logs(self, hosts, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        c = Connection(hosts[0].public, user='ubuntu', connect_kwargs=self.connect)
        c.get(
            PathMaker.client_log_file(self.client_log_id), local=PathMaker.client_log_file(self.client_log_id)
        )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def _config(self, hosts, node_parameters, parallel=0):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        jobs = 2    # //TODOTODO add self.jobs
        cmd = CommandMaker.compile(jobs).split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files for nodes.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        # Generate configuration files for clients. ##
        client_keys = []
        # client_key_files = [PathMaker.key_file(i, 'client') for i in range(len(hosts))]
        client_key_files = [PathMaker.key_file(i, 'client') for i in range(2)]
        for filename in client_key_files:
            cmd = CommandMaker.generate_key(filename, 'client').split()
            subprocess.run(cmd, check=True)
            client_keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        client_names = [x.name for x in client_keys]    ##
        # Using private IP to communicate between nodes
        consensus_addr = [f'{x.private}:{self.settings.consensus_port}' for x in hosts]
        front_addr = [f'{x.private}:{self.settings.front_port}' for x in hosts]
        mempool_addr = [f'{x.private}:{self.settings.mempool_port}' for x in hosts]
        request_ports = [f'{self.settings.request_port}' for x in hosts]    ##
        committee = Committee(names, consensus_addr, front_addr, mempool_addr, request_ports)
        committee.print(PathMaker.committee_file())
        register = Register(client_names)  ##
        register.print(PathMaker.register_file())   ##

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]
        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix='Uploading config files:')
        for i, host in enumerate(progress):
            c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)
            c.put(PathMaker.committee_file(), '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.key_file(i, 'client'), '.')
            c.put(PathMaker.parameters_file(), '.')
            c.put(PathMaker.register_file(), '.')

        return committee

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False, parallel=0):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        
        # assert bench_parameters.nodes == [1]

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Ensure there enough files can be opened
        cmd = 'ulimit -n 3000'
        subprocess.run([cmd], shell=True)

        public_ips = [x.public for x in selected_hosts]
        # for n in bench_parameters.nodes:
        n = 1
        for r in bench_parameters.rate:
            Print.heading(f'\nRunning input rate: {r:,} tx/s')
            hosts = selected_hosts[:1]

            # Upload all configuration files.
            try:
                self._config(hosts, node_parameters, parallel)
            except (subprocess.SubprocessError, GroupException) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                Print.error(BenchError('Failed to configure nodes', e))
                continue

            # Do not boot faulty nodes.
            faults = 0

            # Run the benchmark.
            for i in range(bench_parameters.runs):
                Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                try:
                    self._run_single(
                        hosts, r, bench_parameters, node_parameters, debug, parallel
                    )
                    self._logs(hosts, faults).print(PathMaker.result_file(
                        faults, n, r, bench_parameters.tx_size
                    ))
                    # self._get_logs(hosts)
                except (subprocess.SubprocessError, GroupException, ParseError) as e:
                    self.kill(hosts=hosts)
                    if isinstance(e, GroupException):
                        e = FabricError(e)
                    Print.error(BenchError('Benchmark failed', e))
                    continue

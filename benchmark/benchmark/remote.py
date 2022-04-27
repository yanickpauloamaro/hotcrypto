from atexit import register
from distutils.command.upload import upload
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError, Register
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx, mode):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings

        self.instance_type = self.settings.instance_type.replace('.', '_')
        self.mode = mode

        self.jobs = 2
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

        # Print.info('Installing rust and decompressing the repo...')
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

            f'sudo apt-get install -y unzip',            
        ]

        cmd_diem = [
            # Clone diem repo and run dependency script
            f'(git clone {self.settings.diem_url} || (cd {self.settings.diem_name} ; git pull))',
            f'(cd {self.settings.diem_name} ; yes | ./scripts/dev_setup.sh v)',
            'source ~/.cargo/env',
            ]
        hosts = self.manager.hosts(flat=True)

        try:
            ## Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)

            Print.info('Installing rust...')
            g.run(' && '.join(cmd), hide=True)

            Print.info('Cloning Diem repo and installing Diem dependencies...')
            g.run(' && '.join(cmd_diem), hide=True)

            # TODOTODO make hotcrypto repo public and clone it instead of zip/sending it
            Print.info('Uploading repo...')
            self._upload(public_ips)

            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def remove(self):
        hosts = self.manager.hosts(flat=True)
        filename = self.settings.repo_name

        try:
            cmd = f'{CommandMaker.remove_repo(filename)} || true'

            # Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
            g.run(cmd, hide=True)

        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

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

    def _upload(self, public_ips, cd=False):
        Print.info('Compressing repo...')

        filename = self.settings.repo_name
        zip_name = self.settings.repo_name
        cmd = CommandMaker.compress_repo(filename, zip_name)
        subprocess.run([cmd], check=True, shell=True)

        # Upload configuration files.
        progress = progress_bar(public_ips, prefix='Uploading repo:')
        for _, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.put(f'{zip_name}.zip', '.')

        Print.info('Decompressing...')
        decompress = CommandMaker.decompress_repo(self.settings.repo_name)
        cmd = [
            f'(({decompress}) || true)'
        ]

        if cd:
            cmd += [f'cd {filename}']

        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if self.mode == 'movevm':
            h = [ip[0] for ip in hosts.values()]
            return h[:1]
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(
            f'Updating {len(hosts)} nodes...'
        )

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]

        # TODOTODO make hotcrypto repo public and pull new changes instead of reuploading
        ## Reupload code (Only ned to reupload if there was any changes to rust code)
        # self._upload(public_ips)

        ## Recompile code
        Print.info(f'Recompiling with {self.jobs} jobs...')
        cmd = [
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile(self.jobs)})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]

        # Using public IP to connect to the instances
        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, nb_accounts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile(self.jobs).split()
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
        client_key_files = [PathMaker.key_file(i, 'client') for i in range(nb_accounts)]
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

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        addresses = [f'{x.public}:{self.settings.front_port}' for x in hosts]
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        if self.mode == 'movevm':
            rate_share = rate
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        client_key_files = [PathMaker.key_file(i, 'client') for i in range(len(hosts))] ##

        for host, key_file, addr, log_file in zip(hosts, client_key_files, addresses, client_logs):
            cmd = CommandMaker.run_client(
                addr,
                bench_parameters.tx_size,
                rate_share,
                timeout,
                bench_parameters.duration,
                key_file,
                PathMaker.register_file(),
                self.mode,
                nodes=addresses,
            )
            self._background_run(host, cmd, log_file)

            if self.mode == 'movevm':
                # Only run one client for benchmarking MoveVM
                break

        if self.mode not in ['movevm']:
            # Run the nodes.
            key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
            dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
            node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
            for host, key_file, db, log_file in zip(hosts, key_files, dbs, node_logs):
                cmd = CommandMaker.run_node(
                    key_file,
                    PathMaker.committee_file(),
                    db,
                    PathMaker.parameters_file(),
                    self.settings.request_port,
                    PathMaker.register_file(),
                    self.mode,
                    debug=debug
                )
                self._background_run(host, cmd, log_file)

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(2 * node_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _get_logs(self, hosts):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)
            c.get(
                PathMaker.client_log_file(i), local=PathMaker.client_log_file(i)
            )
            if self.mode not in ['movevm']:
                c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))

    def _logs(self, hosts, faults, nb_accounts):
        
        self._get_logs(hosts)

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults, nb_accounts=nb_accounts)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading(f'Starting remote benchmark of {self.mode}')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

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
        cmd = 'ulimit -n 6000'
        subprocess.run([cmd], shell=True)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')
                hosts = selected_hosts[:n]

                # Upload all configuration files.
                try:
                    self._config(hosts, n, node_parameters)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError('Failed to configure nodes', e))
                    continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[:n-faults]

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, node_parameters, debug
                        )
                        self._logs(hosts, faults, n).print(PathMaker.result_file(
                            faults, n, r, bench_parameters.tx_size, self.mode, self.instance_type
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue

from pathlib import Path

from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext, join
from time import sleep
from math import ceil
import subprocess
import random
import uuid

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError, Register
from benchmark.utils import BenchError, Print, PathMaker, progress_bar, in_parallel, Mode
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
    def __init__(self, ctx, mode=Mode.default()):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings

        self.instance_type = self.settings.instance_type.replace('.', '_')
        if type(mode) == Mode:
            self.mode = mode
        else:
            self.mode = Mode(mode)

        self.jobs = 2
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    @staticmethod
    def _check_stderr(output):
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
            f'sudo apt-get install -y zip',
        ]

        cmd_diem = [
            # Clone diem repo and run dependency script
            f'(git clone {self.settings.diem_url} || (cd {self.settings.diem_name} ; git pull))',
            f'(cd {self.settings.diem_name} ; yes | ./scripts/dev_setup.sh v)',
            'source ~/.cargo/env',
            ]
        hosts = self.manager.hosts(flat=True)

        try:
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)

            Print.info('Installing rust...')
            g.run(' && '.join(cmd), hide=True)

            Print.info('Cloning Diem repo and installing Diem dependencies...')
            g.run(' && '.join(cmd_diem), hide=True)

            # TODOTODO make hotcrypto repo public and clone it instead of zip/sending it
            # Print.info('Uploading repo...')
            # self._upload(public_ips)

            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
            g.close()
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def remove(self):
        hosts = self.manager.hosts(flat=True)
        filename = self.settings.repo_name

        try:
            cmd = f'{CommandMaker.remove(filename)} || true'

            # Using public IP to connect to the instances
            public_ips = [x.public for x in hosts]
            g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
            g.run(cmd, hide=True)
            g.close()
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
            g.close()
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _upload(self, public_ips):
        Print.info('Compressing repo...')

        filename = self.settings.repo_name
        zip_name = self.settings.repo_name
        cmd = CommandMaker.compress_repo(filename, zip_name)
        subprocess.run([cmd], check=True, shell=True)

        args = []
        for i in range(0, len(public_ips)):
            args += [(i, public_ips[i], zip_name, self.manager.settings.key_path)]

        in_parallel(Bench.send_repo, args, 'Uploading repo archive', 'Failed to upload repo archive')

        Print.info('Decompressing...')
        decompress = CommandMaker.decompress(self.settings.repo_name)
        cmd = [
            f'(({decompress}) || true)'
        ]

        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)
        g.close()

    @staticmethod
    def send_repo(tmp):
        (i, host_ip, zip_name, key_path) = tmp
        pkey = RSAKey.from_private_key_file(key_path)
        c = Connection(
            host_ip,
            user='ubuntu',
            connect_kwargs={
            "pkey": pkey,
        },)
        c.put(f'{zip_name}.zip', '.')
        c.close()

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        for k in hosts.keys():
            random.shuffle(hosts[k])

        if self.mode.is_vm():
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
        c.close()
        Bench._check_stderr(output)

    @staticmethod
    def _run_in_background(host, command, log_file, pkey):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host.public, user='ubuntu', connect_kwargs={"pkey": pkey,})
        output = c.run(cmd, hide=True)
        c.close()
        Bench._check_stderr(output)

    def _update(self, hosts):

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]

        cmd = []
        if self.mode == Mode.hotstuff:
            Print.info(
                f'Updating {len(hosts)} nodes (branch "main" of hotstuff)...'
            )
            cmd = [
                f'(cd hotstuff && git fetch -f)',
                f'(cd hotstuff && git checkout -f main)',
                f'(cd hotstuff && git pull -f)',
                'source $HOME/.cargo/env',
                f'(cd hotstuff/node && {CommandMaker.compile()})',
                CommandMaker.alias_binaries(
                    f'./hotstuff/target/release/'
                )
            ]
        else:
            Print.info(
                f'Updating {len(hosts)} nodes...'
            )

            # TODOTODO make hotcrypto repo public and pull new changes instead of reuploading
            # Reupload code (Only need to reupload if there was any changes to rust code)
            self._upload(public_ips)

            # Recompile code
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
        g.close()

    def _config(self, hosts, nb_accounts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        Print.info('Recompiling locally...')
        cmd = CommandMaker.compile(self.jobs).split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files for nodes.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        progress = progress_bar(key_files, prefix='Generating node keys:')
        for i, filename in enumerate(progress):
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        # Generate configuration files for clients.
        client_keys = []
        client_key_files = [PathMaker.key_file(i, 'client') for i in range(nb_accounts)]
        progress = progress_bar(client_key_files, prefix='Generating client keys:')
        for i, filename in enumerate(progress):
            cmd = CommandMaker.generate_key(filename, 'client').split()
            subprocess.run(cmd, check=True)
            client_keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        client_names = [x.name for x in client_keys]

        if len(self.settings.aws_regions) == 1:
            # Using private IP to communicate between nodes
            Print.info('Using private IPs...')
            consensus_addr = [f'{x.private}:{self.settings.consensus_port}' for x in hosts]
            front_addr = [f'{x.private}:{self.settings.front_port}' for x in hosts]
            mempool_addr = [f'{x.private}:{self.settings.mempool_port}' for x in hosts]
            request_ports = [f'{self.settings.request_port}' for x in hosts]
        else:
            # Using public IP to communicate between nodes
            Print.info('Using public IPs...')
            consensus_addr = [f'{x.public}:{self.settings.consensus_port}' for x in hosts]
            front_addr = [f'{x.public}:{self.settings.front_port}' for x in hosts]
            mempool_addr = [f'{x.public}:{self.settings.mempool_port}' for x in hosts]
            request_ports = [f'{self.settings.request_port}' for x in hosts]

        committee = Committee(names, consensus_addr, front_addr, mempool_addr, request_ports)
        committee.print(PathMaker.committee_file())
        register = Register(client_names)
        register.print(PathMaker.register_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'

        # Using public IP to connect to the instances
        public_ips = [x.public for x in hosts]
        g = Group(*public_ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)
        g.close()

        args = []
        for i in range(0, len(hosts)):
            args += [(i, hosts[i], self.mode, self.manager.settings.key_path)]
        in_parallel(Bench.send_config, args, 'Uploading config files', 'Failed to upload configs')

        # self.send_config_sequential(hosts)

        return committee

    def send_config_sequential(self, hosts):
        # Upload configuration files.
        progress = progress_bar(hosts, prefix='Uploading config files:')
        for i, host in enumerate(progress):
            c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)
            c.put(PathMaker.committee_file(), '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.key_file(i, 'client'), '.')
            c.put(PathMaker.parameters_file(), '.')
            c.put(PathMaker.register_file(), '.')
            c.close()

    @staticmethod
    def send_config(tmp):
        (i, host, mode, key_path) = tmp
        pkey = RSAKey.from_private_key_file(key_path)
        c = Connection(
            host.public,
            user='ubuntu',
            connect_kwargs={
            "pkey": pkey,
        },)
        c.put(PathMaker.committee_file(), '.')
        c.put(PathMaker.key_file(i), '.')
        c.put(PathMaker.key_file(i, 'client'), '.')
        c.put(PathMaker.parameters_file(), '.')
        c.put(PathMaker.register_file(), '.')
        c.close()

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        addresses_private = [f'{x.private}:{self.settings.front_port}' for x in hosts]
        if len(self.settings.aws_regions) == 1:
            addresses = addresses_private
        else:
            addresses = [f'{x.public}:{self.settings.front_port}' for x in hosts]
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        if self.mode.is_vm():
            rate_share = rate
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        client_key_files = [PathMaker.key_file(i, 'client') for i in range(len(hosts))]

        params = zip(hosts, client_key_files, addresses_private, client_logs)
        indexed = zip(range(0, len(hosts)), params)
        args = [(i, self.mode, self.manager.settings.key_path, \
            rate_share, timeout, addresses, \
                bench_parameters, param) for i, param in indexed]

        in_parallel(Bench.boot_client, args, 'Booting clients', 'Failed to boot clients')

        if not self.mode.is_vm():
            # Run the nodes.
            key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
            dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
            node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]

            params = zip(hosts, key_files, dbs, node_logs)
            indexed = zip(range(0, len(hosts)), params)
            args = [(i, self.mode, self.manager.settings.key_path, \
                debug, self.settings.request_port, \
                    param) for i, param in indexed]

            in_parallel(Bench.boot_node, args, 'Booting nodes', 'Failed to boot nodes')

        delay = 0
        if self.mode == Mode.diemvm:
            Print.info('Waiting DiemVM to create accounts...')
            delay = Mode.diemvm_delay()
        else:
            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            delay = self.node_parameters.timeout_delay

        sleep(2 * delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))

        sleep(2) # Make sure the clients have time to stop on their own
        self.kill(hosts=hosts, delete_logs=False)

    @staticmethod
    def boot_client(tmp):
        (i, mode, key_path, \
            rate_share, timeout, addresses, \
            bench_parameters, params) = tmp
        (host, key_file, addr, log_file) = params

        pkey = RSAKey.from_private_key_file(key_path)
        if mode.is_vm() and i != 0:
            # Only boot one client for benchmarking MoveVM
            return

        cmd = CommandMaker.run_client(
            addr,
            bench_parameters.tx_size,
            rate_share,
            timeout,
            bench_parameters.duration,
            key_file,
            PathMaker.register_file(),
            mode,
            nodes=addresses,
        )
        Bench._run_in_background(host, cmd, log_file, pkey)

    @staticmethod
    def boot_node(tmp):
        (i, mode, key_path, debug, request_port, params) = tmp
        (host, key_file, db, log_file) = params

        pkey = RSAKey.from_private_key_file(key_path)

        cmd = CommandMaker.run_node(
            key_file,
            PathMaker.committee_file(),
            db,
            PathMaker.parameters_file(),
            request_port,
            PathMaker.register_file(),
            mode,
            debug=debug
        )
        Bench._run_in_background(host, cmd, log_file, pkey)

    def _get_logs(self, hosts):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        self._get_logs_parallel(hosts)
        # self._get_logs_sequential(hosts)

    def _get_logs_sequential(self, hosts):
        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host.public, user='ubuntu', connect_kwargs=self.connect)

            zip_name = f'temp-logs-{i}.zip'
            # Compress logs before downloading them (highly compressible)
            cmd = CommandMaker.compress(PathMaker.log_files(i), zip_name, flat=True)
            output = c.run(cmd, hide=True)

            c.get(zip_name, local=zip_name)

            cmd = CommandMaker.decompress(zip_name, into=PathMaker.logs_path())
            subprocess.run([cmd], shell=True)

            cmd = CommandMaker.remove(zip_name)
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

            zip_name = f'temp-logs-*.zip'
            cmd = CommandMaker.remove(zip_name)
            output = c.run(cmd, hide=True)
            c.close()

    def _get_logs_parallel(self, hosts):
        # Download log files.
        args = []
        for i in range(0, len(hosts)):
            args += [(i, hosts[i], self.mode, self.manager.settings.key_path)]

        in_parallel(Bench.get_host_logs, args, 'Downloading logs', 'Failed to download logs')

    @staticmethod
    def get_host_logs(tmp):
        (i, host, mode, key_path) = tmp
        pkey = RSAKey.from_private_key_file(key_path)
        c = Connection(
            host.public,
            user='ubuntu',
            connect_kwargs={
            "pkey": pkey,
        },)

        zip_name = f'temp-logs-{i}.zip'
        # Compress logs before downloading them (highly compressible)
        cmd = CommandMaker.compress(PathMaker.log_files(i), zip_name, flat=True)
        output = c.run(cmd, hide=True)

        c.get(zip_name, local=zip_name)

        cmd = CommandMaker.decompress(zip_name, into=PathMaker.logs_path())
        subprocess.run([cmd], shell=True)

        cmd = CommandMaker.remove(zip_name)
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        
        zip_name = f'temp-logs-*.zip'
        cmd = CommandMaker.remove(zip_name)
        output = c.run(cmd, hide=True)
        c.close()

    def _logs(self, hosts, faults, nb_accounts):

        self._get_logs(hosts)

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults, nb_accounts=nb_accounts)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False, warmup=False):
        assert isinstance(debug, bool)
        Print.heading(f'Starting remote benchmark of {self.mode}')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Update all nodes.
        try:
            self._update(self.manager.hosts(flat=True))
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Ensure there enough files can be opened
        cmd = 'ulimit -n 1048575'
        subprocess.run([cmd], shell=True)

        tx_size = self.mode.tx_size(bench_parameters.tx_size)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            # Select which hosts to use.
            selected_hosts = self._select_hosts(bench_parameters)
            if not selected_hosts:
                Print.warn('There are not enough instances available')
                return

            # Do not boot faulty nodes.
            faults = bench_parameters.faults
            hosts = selected_hosts[:n-faults]

            # Upload all configuration files.
            try:
                self._config(hosts, n, node_parameters)
            except (subprocess.SubprocessError, GroupException) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                Print.error(BenchError('Failed to configure nodes', e))
                continue

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    params = PathMaker.params(
                        faults, n, r, bench_parameters.tx_size, self.mode, self.instance_type
                    )
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, node_parameters, debug
                        )
                        self._logs(hosts, faults, n).print(params, debug or warmup)
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)

                        Print.error(BenchError('Benchmark failed', e))
                        uid = uuid.uuid4().hex
                        dir = join(PathMaker.results_path(), PathMaker.error_path)

                        zip_file = PathMaker.save_file(dir, params, uid)
                        Print.info(f'Saving logs to {zip_file}...')
                        cmd = CommandMaker.save_logs(zip_file)
                        subprocess.run([cmd], shell=True)
                        continue

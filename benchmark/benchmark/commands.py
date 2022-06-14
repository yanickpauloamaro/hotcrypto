from os.path import join

from benchmark.utils import PathMaker, Mode


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'
    
    @staticmethod
    def save_logs(params, uid, debug=False, error=False):
        sources = [f'{PathMaker.logs_path()}/*.log']
        zip_name = f'{PathMaker.save_path(params, uid, debug, error)}/{uid}'
        cmd = [
            f'mkdir -p {PathMaker.save_path(params, uid, debug, error)}',
            CommandMaker.compress(sources, zip_name),
        ]
        return '; '.join(cmd)
        return (
            f'mkdir -p {PathMaker.save_path(params, uid, debug)} ; '
            f'cp -r logs/*.log {PathMaker.save_path(params, uid, debug)}/'
        )

    @staticmethod
    def compile(jobs = None):
        str = '--jobs 2' if jobs is None else f'--jobs {jobs}'
        return f'cargo build --quiet --release --features benchmark {str}'

    @staticmethod
    def generate_key(filename, bin='node'):
        assert isinstance(filename, str)
        return f'./{bin} keys --filename {filename}'

    @staticmethod
    def run_node(keys, committee, store, parameters, port, register_file, mode, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(register_file, str)
        assert isinstance(mode, Mode)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --port {port} --accounts {register_file} '
                f'{mode}')
        # return (f'./node {v} run --keys {keys} --committee {committee} '
                # f'--store {store} --parameters {parameters}') ## HotStuff

    @staticmethod
    def run_client(address, size, rate, timeout, duration, keys, register_file, mode, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(timeout, int) and size >= 0
        assert isinstance(duration, int) and size > 0
        assert isinstance(nodes, list)
        assert isinstance(keys, str)
        assert isinstance(register_file, str)
        assert isinstance(mode, Mode)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''

        cmd = [f'./client run --rate {rate} --timeout {timeout} --duration {duration}']
        
        cmd += [f'{mode}']
        if mode == Mode.hotstuff:
            cmd += [f'--size {size}']

        if mode.has_consensus():
            cmd += [f'{address} {nodes}']

        if mode in [Mode.hotcrypto, Mode.hotmove, Mode.movevm]:
            cmd += [f'--keys {keys} --accounts {register_file}']

        if mode == Mode.diemvm:
            cmd += ['--with_signature false']
        
        print(' '.join(cmd))
        return ' '.join(cmd)
        # return (f'./client {address} --size {size} '
                # f'--rate {rate} --timeout {timeout} {nodes}') ## HotStuff

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} .'

    @staticmethod
    def compress_repo(repo, zip_name):
        assert isinstance(repo, str)
        assert isinstance(zip_name, str)
        zip_name = zip_name if '.zip' in zip_name else f'{zip_name}.zip'
        return (f'(cd .. && cd .. &&  zip -r {repo}/benchmark/{zip_name} {repo} -q '
                '-x "**/target/*" -x "*/\db_*" -x "*/\.git*" -x "*/\.db-*" '
                '-x "*/\\benchmark/*"'
                ')')

    @staticmethod
    def compress(sources, zip_name, flat=False):
        assert isinstance(sources, list)
        assert len(sources) != 0
        assert all(isinstance(x, str) for x in sources)
        assert isinstance(zip_name, str)
        zip_name = zip_name if '.zip' in zip_name else f'{zip_name}.zip'
        s = ' '.join(sources)
        arg = ' -j' if flat else ''
        return (
            f'zip {zip_name} {s} -FSq'
            f'{arg}'
            )

    @staticmethod
    def decompress(zip_name, into=None):
        assert isinstance(zip_name, str)
        zip_name = zip_name if '.zip' in zip_name else f'{zip_name}.zip'
        dir = '' if into is None else f' -d {into}'
        return (
            f'unzip -uoq {zip_name}'
            f'{dir}'
            )

    @staticmethod
    def remove(filename):
        assert isinstance(filename, str)
        return f'rm -r {filename}'
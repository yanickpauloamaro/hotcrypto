from os.path import join

from benchmark.utils import PathMaker


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
    def save_logs(params, uid):
        return (
            f'mkdir -p {PathMaker.save_path(params, uid)} ; '
            f'cp -r logs/*.log {PathMaker.save_path(params, uid)}/'
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
        assert isinstance(mode, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --port {port} --accounts {register_file} '
                f'{mode}')

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
        assert isinstance(mode, str)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''

        cmd = [f'./client run --rate {rate} --timeout {timeout} --duration {duration}']
        
        cmd += [f'{mode}']
        if mode in ['hotstuff']:
            cmd += [f'--size {size}']

        if mode in ['hotstuff', 'hotcrypto', 'hotmove']:
            cmd += [f'{address} {nodes}']

        if mode in ['movevm', 'hotcrypto', 'hotmove']:
            cmd += [f'--keys {keys} --accounts {register_file}']

        return ' '.join(cmd)

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} .'

    @staticmethod
    def remove_repo(filename):
        assert isinstance(filename, str)
        return f'rm -r {filename}'

    @staticmethod
    def compress_repo(repo, zip_name):
        assert isinstance(repo, str)
        assert isinstance(zip_name, str)
        return (f'(cd .. && cd .. &&  zip -r {repo}/benchmark/{zip_name}.zip {repo} -q '
                '-x "**/target/*" -x "*/\db_*" -x "*/\.git*" -x "*/\.db-*" -x "*/\plots/*" -x "*/\results/*" -x "*/\data/*" -x "*/\logs/*")')

    @staticmethod
    def decompress_repo(zip_name):
        assert isinstance(zip_name, str)
        return f'unzip -uo {zip_name}.zip'

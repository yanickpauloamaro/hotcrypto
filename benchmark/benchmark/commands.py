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
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename, bin='node'):
        assert isinstance(filename, str)
        return f'./{bin} keys --filename {filename}'

    @staticmethod
    def run_node(keys, committee, store, parameters, port, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(port, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --port {port}')

    @staticmethod
    def run_client(address, size, rate, timeout, keys, register_file, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert isinstance(keys, str)
        assert isinstance(register_file, str)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'./client run {address} --size {size} '
                f'--rate {rate} --timeout {timeout} '
                f'--keys {keys} --accounts {register_file} {nodes}')

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} .'

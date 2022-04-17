from os.path import join

from benchmark_move.utils import PathMaker


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
    def compile(jobs = None):
        str = '' if jobs is None else f'--jobs {jobs}'
        return f'cargo build --quiet --release --features benchmark {str}'
        # return f'cargo build --release --features benchmark {str}'

    # @staticmethod
    # def generate_key(filename):
    #     assert isinstance(filename, str)
    #     return f'./node keys --filename {filename}'
    @staticmethod
    def generate_key(filename, bin='node'):
        assert isinstance(filename, str)
        return f'./{bin} keys --filename {filename}'

    # @staticmethod
    # def run_node(keys, committee, store, parameters, debug=False):
    #     assert isinstance(keys, str)
    #     assert isinstance(committee, str)
    #     assert isinstance(parameters, str)
    #     assert isinstance(debug, bool)
    #     v = '-vvv' if debug else '-vv'
    #     return (f'./node {v} run --keys {keys} --committee {committee} '
    #             f'--store {store} --parameters {parameters}')

    @staticmethod
    def run_client(rate, duration, parallel=0):
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(duration, int) and duration >= 0
        assert isinstance(parallel, int) and parallel == 0 or parallel == 1

        str = 'true' if parallel == 1 else 'false'

        return (f'./bench_move run '
                f'--rate {rate} '
                f'--duration {duration} ')

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client, move = join(origin, 'node'), join(origin, 'client'), join(origin, 'bench_move')
        return (f'rm node ; rm client ; rm bench_move ; '
                f'ln -s {node} . ; ln -s {client} . ; ln -s {move} .')

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
        # return f'zip -r ../../{zip_name}.zip ../../{repo} -q -x "**/target/*" -x "**/results/*" -x "**/plots/*" -x "**/.**"'

    @staticmethod
    def decompress_repo(zip_name):
        assert isinstance(zip_name, str)
        return f'unzip -uo {zip_name}.zip'
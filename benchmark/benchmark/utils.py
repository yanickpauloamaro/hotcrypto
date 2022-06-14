from os.path import join
from multiprocessing import Pool
from enum import Enum, auto
import tqdm

class BenchError(Exception):
    def __init__(self, message, error):
        assert isinstance(error, Exception)
        self.message = message
        self.cause = error
        super().__init__(message)


class PathMaker:
    @staticmethod
    def binary_path():
        return join('..', 'target', 'release')

    @staticmethod
    def node_crate_path():
        return join('..', 'node')

    @staticmethod
    def committee_file():
        return '.committee.json'

    @staticmethod
    def register_file():    ##
        return '.register.json'

    @staticmethod
    def parameters_file():
        return '.parameters.json'

    @staticmethod
    def key_file(i, bin='node'):    ##
        assert isinstance(i, int) and i >= 0
        return f'.{bin}-{i}.json'

    @staticmethod
    def db_path(i):
        assert isinstance(i, int) and i >= 0
        return f'.db-{i}'

    @staticmethod
    def logs_path():
        return 'logs'

    @staticmethod
    def debug_path():
        return 'debug'
        # return '/run/user/1000/gvfs/smb-share:server=minicloud.local,share=dcl/debug' ## TODOTODO

    @staticmethod
    def nas_path(): #TODOTODO Remove this
        # return '/run/user/1000/gvfs/smb-share:server=minicloud.local,share=yanick/project_backup/clean_results'
        return '/run/user/1000/gvfs/smb-share:server=minicloud.local,share=dcl/saved'

    @staticmethod
    def fail_path():
        # return 'fail'
        return '/run/user/1000/gvfs/smb-share:server=minicloud.local,share=dcl/fail' ## TODOTODO


    @staticmethod
    def save_path(params, uid, debug=False, error=False):
        # dir = f'{PathMaker.debug_path()}' if debug else 'saved'
        dir = f'{PathMaker.debug_path()}' if debug else f'{PathMaker.nas_path()}' ##TODOTOOD Save to nas
        dir = f'{PathMaker.fail_path()}' if error else dir
        return f'{dir}/{params}/{uid}'

    @staticmethod
    def log_files(i):
        assert isinstance(i, int) and i >= 0
        return [PathMaker.client_log_file(i), PathMaker.node_log_file(i)]
    

    @staticmethod
    def node_log_file(i):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(), f'node-{i}.log')

    @staticmethod
    def client_log_file(i):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(), f'client-{i}.log')

    @staticmethod
    def results_path():
        return 'results'

    @staticmethod
    def result_file(faults, nodes, rate, tx_size, mode, instance):
        return join(
            PathMaker.results_path(), 
            f'bench-{mode}-{faults}-{nodes}-{rate}-{tx_size}-{instance}.txt'
        )

    @staticmethod
    def plots_path():
        return 'plots'

    @staticmethod
    def agg_file(type, mode, faults, nodes, rate, tx_size, max_latency, cores):
        return join(
            PathMaker.plots_path(),
            f'{mode}-{type}-{faults}-{nodes}-{rate}-{tx_size}-{max_latency}-{cores}.txt'
        )

    @staticmethod
    def plot_file(name, ext):
        return join(PathMaker.plots_path(), f'{name}.{ext}')


class Color:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Print:
    @staticmethod
    def heading(message):
        assert isinstance(message, str)
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def info(message):
        assert isinstance(message, str)
        print(message)

    @staticmethod
    def warn(message):
        assert isinstance(message, str)
        print(f'{Color.BOLD}{Color.WARNING}WARN{Color.END}: {message}')

    @staticmethod
    def error(e):
        assert isinstance(e, BenchError)
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {e}\n')
        causes, current_cause = [], e.cause
        while isinstance(current_cause, BenchError):
            causes += [f'  {len(causes)}: {e.cause}\n']
            current_cause = current_cause.cause
        causes += [f'  {len(causes)}: {type(current_cause)}\n']
        causes += [f'  {len(causes)}: {current_cause}\n']
        print(f'Caused by: \n{"".join(causes)}\n')


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=30, fill='█', print_end='\r'):
    total = len(iterable)

    def printProgressBar(iteration):
        formatter = '{0:.'+str(decimals)+'f}'
        percent = formatter.format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

    printProgressBar(0)
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    print()


def in_parallel(fn, args, desc, err_msg):
    try:
        with Pool() as p:
            for _ in tqdm.tqdm(
                p.imap(fn, args),
                desc=desc,
                bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                total=len(args)):
                pass
    except (ValueError, IndexError) as e:
        raise BenchError(f'{err_msg}: {e}')

class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

class Mode(str, AutoName):
    hotstuff = auto()
    hotcrypto = auto()
    hotmove = auto()
    movevm = auto()
    diemvm = auto()
    
    def __str__(self):
        return self.name
    
    @staticmethod
    def default():
        return Mode.hotstuff

    @staticmethod
    def possible_values():
        return [m for m in list(Mode)]

    def has_consensus(self):
        return self in [Mode.hotstuff, Mode.hotcrypto, Mode.hotmove]

    def has_currency(self):
        return self in [Mode.hotcrypto, Mode.hotmove]

    def is_vm(self):
        return self in [Mode.movevm, Mode.diemvm]

    def print(self):
        if self == Mode.hotstuff:
            return 'HotStuff'
        elif self == Mode.hotcrypto:
            return 'HotCrypto'
        elif self == Mode.hotmove:
            return 'HotMove'
        elif self == Mode.movevm:
            return 'MoveVM'
        elif self == Mode.diemvm:
            return 'DiemVM'
        else:
            self.__str__()
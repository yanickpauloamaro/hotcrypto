from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print, Mode
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError

@task
def local(ctx, mode=Mode.default()):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 2,
        'rate': 10_000,
        'tx_size': 512,
        'duration': 30,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 1_000,
            'sync_retry_delay': 10_000,
        },
        'mempool': {
            'gc_depth': 50,
            'sync_retry_delay': 5_000,
            'sync_retry_nodes': 3,
            'batch_size': 15_000,
            'max_batch_delay': 10
        }
    }

    if mode not in Mode.possible_values():
        print('Unknown mode')
        return

    try:
        ret = LocalBench(bench_params, node_params).run(mode, debug=False).result()
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=1):
    ''' Create a testbed (N nodes PER region) '''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)

@task
def reduce(ctx):
    ''' Halves the number of nodes in the testbed '''
    try:
        per_region = lambda n: n/2
        InstanceManager.make().reduce_instances(per_region=per_region)
    except BenchError as e:
        Print.error(e)


@task
def cancel(ctx):
    ''' Cancel all spot requests '''
    try:
        InstanceManager.make().cancel_spot_request()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' (Normal instance only) Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' (Normal instance only) Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task # ------------------------------------------------------------------------------
def remote(ctx, mode):
    ''' Run benchmarks on AWS (N nodes across all regions) '''
    bench_params = {
        'faults': 0,
        # 'tx_size': 512,
        'tx_size': 128,
        'nodes': [4],
        
        'rate': [25_000],

        ## HotStuff benchmark
        # 'rate': [25_000, 50_000,  75_000, 100_000, 110_000], # 96 nodes
        # 'rate': [25_000,          75_000, 100_000,            125_000, 150_000,          200_000], # 48 nodes
        # 'rate': [25_000,          75_000, 100_000,            125_000, 150_000, 175_000, 200_000, 250_000], # 24 nodes
        # 'rate': [25_000,          75_000, 100_000,                     150_000,          200_000, 250_000, 300_000], # 12 nodes
        # 'rate': [25_000,          75_000, 100_000,                     150_000,          200_000, 250_000, 300_000], # 6 nodes

        ## Hotcrypto benchmark
        # 'rate': [10_000, 20_000, 40_000,  45_000,   50_000, 55_000, 60_000, 65_000], # 4 vCPU
        # 'rate': [10_000, 20_000, 40_000,            50_000,         60_000,         70_000, 75_000, 80_000, 85_000], # 8 vCPU
        # 'rate': [10_000,                            50_000,                         70_000,         80_000, 100_000, 110_000], # 16 vCPU
        # 'rate': [10_000,                            50_000,                                         80_000, 100_000,          120_000], # 36 vCPU
        
        ## MoveVM
        # 'rate': [10_000, 20_000, 60_000, 65_000, 70_000, 75_000,  80_000, 85_000, 90_000, 95_000], # 4 vCPU
        # 'rate': [10_000, 40_000,                  70_000,         80_000, 85_000, 90_000, 95_000], # 8 vCPU
        # 'rate': [10_000,                                          80_000, 85_000, 90_000, 95_000], # 16 vCPU


        'duration': 120,
        'runs': 1,
    }

    node_params = {
        'consensus': {
            'timeout_delay': 5_000,
            'sync_retry_delay': 5_000,
        },
        'mempool': {
            'gc_depth': 50,
            'sync_retry_delay': 5_000,
            'sync_retry_nodes': 3,
            'batch_size': 500_000,
            'max_batch_delay': 100
        }
    }
    
    if mode not in Mode.possible_values():
        print('Unknown mode')
        return

    try:
        Bench(ctx, mode).run(bench_params, node_params, debug=False)
    except BenchError as e:
        Print.error(e)

@task # ------------------------------------------------------------------------------
def plot(ctx, mode): 
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [6, 12, 24, 48],
        'cores': [4, 8, 16],
        # 'nodes': [6, 12, 24, 48, 96],
        'tx_size': 197,
        'max_latency': [2_000, 5_000]
    }
    try:
        Ploter.plot(plot_params, mode)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop any HotStuff execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))

@task
def remove(ctx):
    ''' Remove repo from all machines '''
    try:
        Bench(ctx).remove()
    except BenchError as e:
        Print.error(e)

@task
def warmupremote(ctx):
    ''' Run benchmarks on AWS (N nodes across all regions) '''
    bench_params = {
        'faults': 0,
        'tx_size': 128,
        'nodes': [4],
        'rate': [10_000],
        'duration': 30,
        'runs': 1,
    }

    node_params = {
        'consensus': {
            'timeout_delay': 5_000,
            'sync_retry_delay': 5_000,
        },
        'mempool': {
            'gc_depth': 50,
            'sync_retry_delay': 5_000,
            'sync_retry_nodes': 3,
            'batch_size': 500_000,
            'max_batch_delay': 100
        }
    }

    # for mode in Mode.possible_values():
    for mode in [Mode.hotstuff, Mode.hotcrypto]:
        try:
            Bench(ctx, mode).run(bench_params, node_params, debug=False, warmup=True)
        except BenchError as e:
            Print.error(e)

@task
def warmuplocal(ctx):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 2,
        'rate': 10_000,
        'tx_size': 128,
        'duration': 10,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 1_000,
            'sync_retry_delay': 10_000,
        },
        'mempool': {
            'gc_depth': 50,
            'sync_retry_delay': 5_000,
            'sync_retry_nodes': 3,
            'batch_size': 15_000,
            'max_batch_delay': 10
        }
    }

    for mode in Mode.possible_values():
        try:
            ret = LocalBench(bench_params, node_params).run(mode, debug=False).result()
            print(ret)
        except BenchError as e:
            Print.error(e)
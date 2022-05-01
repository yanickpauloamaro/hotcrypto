from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from socket import timeout
from statistics import mean
import uuid
import subprocess
from benchmark.commands import CommandMaker
import tqdm
import threading

from benchmark.utils import Print, PathMaker


class ParseError(Exception):
    pass

class LogParser:

    def __init__(self, directory, faults, nb_accounts):
        client_filenames = sorted(glob(join(directory, 'client-*.log')))
        nodes_filenames = sorted(glob(join(directory, 'node-*.log')))

        self.faults = faults
        self.warn = False
        if len(nodes_filenames) == 0:
            self.committee_size = nb_accounts
        elif isinstance(faults, int):
            self.committee_size = len(nodes_filenames) + int(faults)
        else:
            self.committee_size = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = list(tqdm.tqdm(
                        p.imap(self._parse_clients, client_filenames),
                        desc="Parsing client logs",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(client_filenames)))

        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client logs: {e}')
        self.tx_size, self.rate, start, end, misses, self.mode, self.cores, \
            consensus_samples, currency_samples, \
            move_samples, move_commits, self.move_nb_tx, self.move_stats \
            = zip(*results)
        self.misses = sum(misses)
        
        self.start = min(start)
        self.end = max(end)
        self.duration = self.end - self.start
        self.start_str = datetime.fromtimestamp(self.start).strftime('%d-%m-%Y %H:%M:%S')
        self.end_str = datetime.fromtimestamp(self.end).strftime('%d-%m-%Y %H:%M:%S')
        self.misses_str = ''
        self.timeouts_str = ''
        self.crypto_channel_str = ''
        self.consensus_channel_str = ''
        
        self.sent = {
            'consensus': consensus_samples,
            'crypto': currency_samples,
            'currency': currency_samples,
            'move': move_samples
        }

        self.commits = { 'move': self._merge_results([x.items() for x in move_commits]) }
        self.received = { 'move': move_commits }

        # Check whether clients missed their target rate.
        msg = f'Clients missed their target rate {self.misses:,} time(s)'
        if self.misses != 0:
            self.warn = True
            Print.warn(msg)
            self.misses_str = f' {msg}\n'

        # Don't parse nodes for MoveVM benchmark (there are none)
        if self.mode[0] != 'MoveVM':
            # Parse the nodes logs.
            try:
                with Pool(processes=3) as p:
                    results = list(tqdm.tqdm(
                        p.imap(self._parse_nodes, nodes_filenames),
                        desc="Parsing node logs",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(nodes_filenames)))

            except (ValueError, IndexError) as e:
                raise ParseError(f'Failed to parse node logs: {e}')
            consensus_proposals, consensus_commits, consensus_sizes, \
            timeouts, self.configs, consensus_received, \
            currency_received, currency_commits, crypto_sizes, \
            currency_sizes, consensus_channel_full, crypto_channel_full = zip(*results)
            
            self.crypto_channel_full = sum(crypto_channel_full)
            self.consensus_channel_full = sum(consensus_channel_full)
            self.proposals = self._merge_results([x.items() for x in consensus_proposals])
            self.received = {
                'consensus': consensus_received,
                'crypto': currency_received,
                'currency': currency_received,
            }
            self.commits = {
                'consensus': self._merge_results([x.items() for x in consensus_commits]),
                'crypto': self._merge_results([x.items() for x in currency_received]),
                'currency': self._merge_results([x.items() for x in currency_commits]),
            }
            self.sizes = {
                'consensus': {
                    k: v for x in consensus_sizes for k, v in x.items() if k in self.commits['consensus']
                },
                'crypto': {
                    batch_id: nb_tx for c in crypto_sizes for batch_id, (ts, nb_tx) in c.items()
                },
                'currency': {
                    batch_id: nb_tx for c in currency_sizes for batch_id, (ts, nb_tx) in c.items()
                },
            }

            self.timeouts = max(timeouts)
            # Check whether the nodes timed out.
            # Note that nodes are expected to time out once at the beginning.
            msg = f'Nodes timed out {self.timeouts:,} time(s)'
            if self.timeouts > 2:
                self.warn = True
                Print.warn(msg)
                self.timeouts_str = f' {msg}\n'

            msg = f'Consensus waited on Crypto {self.consensus_channel_full:,} time(s)'
            if self.consensus_channel_full != 0:
                self.warn = True
                Print.warn(msg)
                self.consensus_channel_str = f' {msg}\n'
            
            msg = f'Crypto waited on Currency {self.crypto_channel_full:,} time(s)'
            if self.crypto_channel_full != 0:
                self.warn = True
                Print.warn(msg)
                self.crypto_channel_str = f' {msg}\n'

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_currency_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for batch_id, (ts, nb) in x:
                if not batch_id in merged or merged[batch_id][0] > ts:
                    merged[batch_id] = (ts, nb)
        return merged

    # def _parse_clients(self, log):
    def _parse_clients(self, filename):
        with open(filename, 'r') as f:
            log = f.read()
            if search(r'Z Error', log) is not None:
                raise ParseError('Client(s) panicked')

            mode = search(r'Benchmarking (.*)', log).group(1)
            cores = int(search(r'Number of cores: (\d+)', log).group(1))
            size = int(search(r'Transactions size: (\d+)', log).group(1))
            rate = int(search(r'Transactions rate: (\d+)', log).group(1))

            tmp = search(r'\[(.*Z) .* Start sending', log).group(1)
            start = self._to_posix(tmp)

            tmp = search(r'\[(.*Z) .* Stop sending ', log)
            if tmp is None:
                tmp = tmp = search(r'\[(.*Z) .* Stop processing ', log)
            if tmp is None:
                end = self._to_posix('2000-05-01T19:44:47.328Z')
            else:
                end = self._to_posix(tmp.group(1))

            misses = len(findall(r'rate too high', log))

            consensus_samples = self._parse_consensus_client(log)
            currency_samples = self._parse_currency_client(log)

            move_samples, move_commits, move_nb_tx, move_stats = self._parse_move_client(log)
            
            return size, rate, start, end, misses, mode, cores, \
                consensus_samples, currency_samples, \
                move_samples, move_commits, move_nb_tx, move_stats

    
    def _parse_consensus_client(self, log):
        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}
        return samples

    def _parse_currency_client(self, log):
        tmp = findall(r'\[(.*Z) .* Sending sample transaction (\d+) from (.*)', log)
        samples = {(int(s), c): self._to_posix(t) for t, s, c in tmp}
        return samples

    def _parse_move_client(self, log):

        tmp = findall(r'\[(.*Z) .* Sending sample input (\d+) from .*', log)
        move_samples = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = findall(r'\[(.*Z) .* Processed sample input (\d+)', log)
        move_commits = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = search(r'\[.*\] MoveVM processed (\d+) inputs', log)
        move_nb_tx = 0 if tmp is None else int(tmp.group(1))

        stats = []
        tmp = search(r'\[.*\]( MoveVM session creation: .*)', log)
        stats += [] if tmp is None else [tmp.group(1)]

        tmp = search(r'\[.*\]( MoveVM script execution: .*)', log)
        stats += [] if tmp is None else [tmp.group(1)]

        tmp = search(r'\[.*\]( MoveVM closing session: .*)', log)
        stats += [] if tmp is None else [tmp.group(1)]

        tmp = search(r'\[.*\]( MoveVM applying changset: .*)', log)
        stats += [] if tmp is None else [tmp.group(1)]

        tmp = search(r'\[.*\]( MoveVM execution time: .*)', log)
        stats += [] if tmp is None else [tmp.group(1)]

        
        return move_samples, move_commits, move_nb_tx, '\n'.join(stats)

    def _parse_consensus_node(self, log):
        
        tmp = findall(r'\[(.*Z) .* Created B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])
        
        tmp = findall(r'\[.* Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'\[.* Batch ([^ ]+) contains sample tx (\d+)', log)
        received = {int(s): d for d, s in tmp}

        tmp = findall(r'\[.* WARN .* Consensus commit channel reached capacity (\d+) times', log)
        tmp += [0] # Ensure tmp is not empty
        consensus_channel_full = max([int(count) for count in tmp])

        tmp = findall(r'\[.* WARN .* Timeout', log)
        timeouts = len(tmp)

        configs = {
            'consensus': {
                'timeout_delay': int(
                    search(r'\[.* Timeout delay .* (\d+)', log).group(1)
                ),
                'sync_retry_delay': int(
                    search(
                        r'\[.* consensus.* Sync retry delay .* (\d+)', log
                    ).group(1)
                ),
            },
            'mempool': {
                'gc_depth': int(
                    search(r'.* Garbage collection .* (\d+)', log).group(1)
                ),
                'sync_retry_delay': int(
                    search(r'.* mempool.* Sync retry delay .* (\d+)', log).group(1)
                ),
                'sync_retry_nodes': int(
                    search(r'.* Sync retry nodes .* (\d+)', log).group(1)
                ),
                'batch_size': int(
                    search(r'.* Batch size .* (\d+)', log).group(1)
                ),
                'max_batch_delay': int(
                    search(r'.* Max batch delay .* (\d+)', log).group(1)
                ),
            }
        }

        return proposals, commits, sizes, timeouts, configs, received, consensus_channel_full

    def _parse_currency_node(self, log):

        if search(r'\[ .* Some transactions are invalid .*', log) is not None:
            raise ParseError('Faulty transaction signatures.')
        
        tmp = findall(r'\[(.*Z) .* Verified sample transaction (\d+) from (.*)', log)
        received = {(int(s), c): self._to_posix(t) for t, s, c in tmp}

        tmp = findall(r'\[.* WARN .* Crypto output channel reached capacity (\d+) times', log)
        tmp += [0] # Ensure tmp is not empty
        crypto_channel_full = max([int(count) for count in tmp])

        tmp = findall(r'\[(.*Z) .* Processed sample transaction (\d+) from (.*)', log)
        commits = {(int(s), c): self._to_posix(t) for t, s, c in tmp}
        
        tmp = findall(r'\[(.*Z) .* Batch ([^ ]+) contains (\d+) (.*) tx', log)
        crypto_tmp = [(d, (self._to_posix(ts), int(s))) for ts, d, s, tpe in tmp if tpe == 'crypto']
        crypto_sizes = self._merge_currency_results([crypto_tmp])
        
        currency_tmp = [(d, (self._to_posix(ts), int(s))) for ts, d, s, tpe in tmp if tpe == 'currency']
        currency_sizes = self._merge_currency_results([currency_tmp])

        return received, commits, crypto_sizes, currency_sizes, crypto_channel_full

    # def _parse_nodes(self, log):
    def _parse_nodes(self, filename):
        with open(filename, 'r') as f:

            log = f.read()
            if search(r'panic', log) is not None:
                raise ParseError('Node(s) panicked')

            consensus_proposals, consensus_commits, consensus_sizes,  consensus_timeouts, \
                consensus_configs, consensus_received, consensus_channel_full = self._parse_consensus_node(log)

            currency_received, currency_commits, crypto_sizes, \
                currency_sizes, crypto_channel_full = self._parse_currency_node(log)

            return consensus_proposals, consensus_commits, consensus_sizes, \
                consensus_timeouts, consensus_configs, consensus_received, \
                currency_received, currency_commits, \
                crypto_sizes, currency_sizes, consensus_channel_full, crypto_channel_full

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    # Consensus ------------------------------------------------------------------
    def _consensus_throughput(self):
        if not self.commits['consensus']:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits['consensus'].values())
        duration = end - start
        
        bytes = sum(self.sizes['consensus'].values())
        bps = bytes / duration
        tps = bps / self.tx_size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        commits = self.commits['consensus'].items()
        latency = 0
        for d, c in commits:
            # if d in self.proposals:
            if c - self.proposals[d] > 0:
                latency += c - self.proposals[d]
        return latency / len(commits) if commits else 0
        
        latency = [c - self.proposals[d] for d, c in self.commits['consensus'].items()]
        return mean(latency) if latency else 0

    # Consensus e2e ------------------------------------------------------------------
    def _end_to_end_throughput(self):
        if not self.commits['consensus']:
            return 0, 0, 0
        start, end = self.start, max(self.commits['consensus'].values())
        duration = end - start

        bytes = sum(self.sizes['consensus'].values())
        bps = bytes / duration
        tps = bps / self.tx_size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []

        for sent, received in zip(self.sent['consensus'], self.received['consensus']):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits['consensus']:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits['consensus'][batch_id]
                    latency += [end-start]

        return mean(latency) if latency else 0

    # Crypto ------------------------------------------------------------------
    def _crypto_throughput(self):
        return self._throughput('crypto')

    def _crypto_latency(self):
        return self._latency('crypto')

    # Currency ------------------------------------------------------------------
    def _currency_throughput(self):
        return self._throughput('currency')

    def _currency_latency(self):
        return self._latency('currency')

    def _throughput(self, mode):
        if not self.commits[mode]:
            return 0, 0, 0

        # start, end = self.start, max(self.commits[mode].values())
        # duration = end - start
        # TODOTODO the other version gives weird results during high contention
        duration = self.duration

        nb_tx = sum(self.sizes[mode].values())
        tps = nb_tx / duration
        bps = tps * self.tx_size[0]

        return tps, bps, duration

    def _latency(self, mode):
        latency = []
        # For each client/node pair
        for sent, received in zip(self.sent[mode], self.received[mode]):
            # Compute latency for transactions sent by this client
            for (sample, client), ts in received.items():
                
                if (sample, client) in self.commits[mode]:
                    if (sample, client) in sent:
                        start = sent[(sample, client)]
                        end = ts
                        latency += [end-start]
        return mean(latency) if latency else 0

    # Move ------------------------------------------------------------------
    def _move_throughput(self):
        # start, end = self.start, max(self.commits['move'].values())
        # duration = end - start
        duration = self.duration
        return self.move_nb_tx[0]/duration

    def _move_latency(self):  ##
        latency = []
        
        sent = self.sent['move'][0]
        received = self.received['move'][0]

        for (sample, ts) in received.items():
            assert sample in sent
            start = sent[sample]
            end = ts
            latency += [end-start]
            # print('sample', sample, f'had {round(end-start)}s of latency')
        
        return mean(latency) if latency else 0

    def _consensus_result(self):
        consensus_str = ''
        if self.mode[0] in ['HotStuff', 'HotCrypto', 'HotMove']:
            consensus_latency = self._consensus_latency() * 1000
            consensus_tps, consensus_bps, _ = self._consensus_throughput()
            consensus_e2e_latency = self._end_to_end_latency() * 1000
            consensus_e2e_tps, consensus_e2e_bps, _ = self._end_to_end_throughput()
            consensus_str = (
                f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
                f' Consensus BPS: {round(consensus_bps):,} B/s\n'
                f' Consensus latency: {round(consensus_latency):,} ms\n'
                '\n'
                f' End-to-end TPS: {round(consensus_e2e_tps):,} tx/s\n'
                f' End-to-end BPS: {round(consensus_e2e_bps):,} B/s\n'
                f' End-to-end latency: {round(consensus_e2e_latency):,} ms\n'
                )
        return consensus_str

    def _consensus_config(self):
        config_str = ''
        if self.mode[0] in ['HotStuff', 'HotCrypto', 'HotMove']:
            consensus_timeout_delay = self.configs[0]['consensus']['timeout_delay']
            consensus_sync_retry_delay = self.configs[0]['consensus']['sync_retry_delay']
            mempool_gc_depth = self.configs[0]['mempool']['gc_depth']
            mempool_sync_retry_delay = self.configs[0]['mempool']['sync_retry_delay']
            mempool_sync_retry_nodes = self.configs[0]['mempool']['sync_retry_nodes']
            mempool_batch_size = self.configs[0]['mempool']['batch_size']
            mempool_max_batch_delay = self.configs[0]['mempool']['max_batch_delay']

            config_str = (
                ' + CONFIG:\n'
                f' Faults: {self.faults} nodes\n'
                f' Committee size: {self.committee_size} nodes\n'
                f' Transaction size: {self.tx_size[0]:,} B\n'
                '\n'
                f' Consensus timeout delay: {consensus_timeout_delay:,} ms\n'
                f' Consensus sync retry delay: {consensus_sync_retry_delay:,} ms\n'
                f' Mempool GC depth: {mempool_gc_depth:,} rounds\n'
                f' Mempool sync retry delay: {mempool_sync_retry_delay:,} ms\n'
                f' Mempool sync retry nodes: {mempool_sync_retry_nodes:,} nodes\n'
                f' Mempool batch size: {mempool_batch_size:,} B\n'
                f' Mempool max batch delay: {mempool_max_batch_delay:,} ms\n'
                '\n'
                '-----------------------------------------\n'
            )
        return config_str

    def _currency_result(self):
        currency_str = ''
        if self.mode[0] in ['HotCrypto', 'HotMove']:
            currency_tps, currency_bps, _ = self._crypto_throughput()
            currency_latency = self._crypto_latency() * 1000
            currency_str = (
                f'\n'
                f' Crypto TPS: {round(currency_tps):,} tx/s\n'
                f' Crypto BPS: {round(currency_bps):,} B/s\n'
                f' Crypto latency: {round(currency_latency):,} ms\n'
            )

            if self.mode[0] in ['HotMove']:
                currency_e2e_tps, currency_e2e_bps, _ = self._currency_throughput()
                currency_e2e_latency = self._currency_latency() * 1000
                currency_str = (
                    f'{currency_str}'
                    f'\n'
                    f' Currency TPS: {round(currency_e2e_tps):,} tx/s\n'
                    f' Currency BPS: {round(currency_e2e_bps):,} B/s\n'
                    f' Currency latency: {round(currency_e2e_latency):,} ms\n'
                )
        
        return currency_str

    def _move_results(self):
        move_str = ''
        if self.mode[0] in ['MoveVM']:
            movevm_tps = self._move_throughput()
            movevm_latency = self._move_latency() * 1000
            move_str = (
                f' MoveVM TPS: {round(movevm_tps):,} tx/s\n'
                f' MoveVM latency: {round(movevm_latency):,} ms\n'
                '\n'
                f'{self.move_stats[0]}\n'
            )
        return move_str

    def _warn(self):
        if self.warn:
            return (
                ' + WARN:\n'
                f'{self.misses_str}'
                f'{self.timeouts_str}'
                f'{self.consensus_channel_str}'
                f'{self.crypto_channel_str}'
                '-----------------------------------------\n'
            )
        return ''

    def result(self, uid=''):
        return (
            '\n'
            '-----------------------------------------\n'
            f' SUMMARY: {self.mode[0]} {uid}\n'
            '-----------------------------------------\n'
            f'{self._consensus_config()}'
            ' + INFO:\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Number of cores: {self.cores[0]}\n'
            f' Number of accounts: {self.committee_size}\n'
            f' Execution time: {round(self.duration):,} s\n'
            f' Benchmark start: {self.start_str}\n'
            f' Benchmark end:   {self.end_str}\n'
            '-----------------------------------------\n'
            f'{self._warn()}'
            ' + RESULTS:\n'
            f'{self._consensus_result()}'
            f'{self._currency_result()}'
            f'{self._move_results()}'
            '-----------------------------------------\n'
        )

    def print(self, filename, debug=False):
        assert isinstance(filename, str)

        # Save logs and results
        uid = uuid.uuid4().hex
        tmp = filename.replace('results/', '')
        params = tmp.replace('.txt', '')
        path = PathMaker.save_path(params, uid, debug)

        Print.info(f'Saving to {path}/...')
        cmd = CommandMaker.save_logs(params, uid, debug)
        subprocess.run([cmd], shell=True)

        result = self.result(uid)
        with open(f'{path}/result.txt', 'a') as f:
            f.write(result)

        if debug:
            filename = f'{PathMaker.debug_path()}/{tmp}'
        Print.info(f'Saving to {filename}...')
        with open(filename, 'a') as f:
            f.write(result)

    @classmethod
    def process(cls, directory, faults, nb_accounts = '?'):
        assert isinstance(directory, str)
        return cls(directory, faults, nb_accounts)

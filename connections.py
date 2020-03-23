from gevent import monkey
monkey.patch_all()  # NOQA

import argparse
import logging
import multiprocessing
import time

import gevent.pool as gevent_pool
import requests


LOG = logging.getLogger(__name__)


class Stats:
    def __init__(self):
        self.first = 0


def request(session, url, timeout):
    try:
        t0 = time.time()
        resp = session.get(url, timeout=timeout)
        t1 = time.time()
        elapsed_ms = (t1 - t0) * 1000
        LOG.debug('resp=%s elapsed_ms=%d', resp, elapsed_ms)
        resp.raise_for_status()
    except Exception as e:
        LOG.error(e)
        return False
    return True


def run_session(stats, url, interval, no_keep_alive, connect_timeout, read_timeout):
    first = False
    timeout = (connect_timeout, read_timeout)
    session = None
    while True:
        t0 = time.time()
        if not session:
            session = requests.Session()
        ok = request(session, url, timeout)
        if not ok:
            break
        if first:
            first = False
            stats.first += 1
        if no_keep_alive:
            session.close()
            session = None
        t1 = time.time()
        wait = max(interval - (t1 - t0), 0)
        time.sleep(wait)


def get_args():
    p = argparse.ArgumentParser()
    p.add_argument('url')
    p.add_argument('--connections', type=int, required=True)
    p.add_argument('--connection-per-second', type=float, default=0)
    p.add_argument('--request-interval', type=float, default=0)
    p.add_argument('--processes', type=int)
    p.add_argument('--no-keep-alive', action='store_true')
    p.add_argument('--connect-timeout', type=float)
    p.add_argument('--read-timeout', type=float)
    args = p.parse_args()
    if not args.processes:
        args.processes = multiprocessing.cpu_count()
    return args


def worker_main(url, connections, connection_per_second, request_interval, no_keep_alive, connect_timeout, read_timeout):  # NOQA
    gepool = gevent_pool.Pool(connections + 10)
    if connection_per_second > 0:
        interval = 1.0 / connection_per_second
    else:
        interval = 0
    stats = Stats()

    def f():
        for i in range(connections):
            t0 = time.time()
            gepool.spawn(run_session, stats, url, request_interval, no_keep_alive, connect_timeout, read_timeout)
            t1 = time.time()
            wait = max(interval - (t1 - t0), 0)
            time.sleep(wait)

    gepool.spawn(f)
    first_reported = False
    while True:
        time.sleep(1)
        if not first_reported:
            if stats.first >= connections:
                LOG.info('established %d connections', stats.first)
                first_reported = True


def n_split(total, n):
    return [(total + i) // n for i in range(n)]


def main():
    args = get_args()
    p_list = []
    for connections in n_split(args.connections, args.processes):
        if connections < 1:
            continue
        share = float(connections) / args.connections
        connection_per_second = args.connection_per_second * share
        target_args = (
            args.url,
            connections,
            connection_per_second,
            args.request_interval,
            args.no_keep_alive,
            args.connect_timeout,
            args.read_timeout,
            )
        p = multiprocessing.Process(
            target=worker_main,
            args=target_args)
        p.start()
        p_list.append(p)
    for p in p_list:
        p.join()


if __name__ == '__main__':
    import os
    level = 'DEBUG' if int(os.environ.get('DEBUG', '0')) else 'INFO'
    format = '%(asctime)s %(levelname)s %(name)s %(message)s'
    logging.basicConfig(level=level, format=format)
    main()

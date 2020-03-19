from gevent import monkey
monkey.patch_all()  # NOQA

import argparse
import multiprocessing
import time

import gevent.pool as gevent_pool
import requests


def request(session, url):
    try:
        resp = session.get(url)
        resp.raise_for_status()
    except Exception as e:
        print(e)
        return False
    return True


def run_session(url, interval, no_keep_alive):
    session = None
    while True:
        t0 = time.time()
        if not session:
            session = requests.Session()
        ok = request(session, url)
        if not ok:
            break
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
    p.add_argument('--connection-per-second', type=float, default=100000.0)
    p.add_argument('--request-interval', type=float, default=10.0)
    p.add_argument('--processes', type=int)
    p.add_argument('--no-keep-alive', action='store_true')
    args = p.parse_args()
    if not args.processes:
        args.processes = multiprocessing.cpu_count()
    return args


def worker_main(url, connections, connection_per_second, request_interval, no_keep_alive):  # NOQA
    gepool = gevent_pool.Pool(connections + 10)
    interval = 1.0 / connection_per_second

    def f():
        for i in range(connections):
            t0 = time.time()
            gepool.spawn(run_session, url, request_interval, no_keep_alive)
            t1 = time.time()
            wait = max(interval - (t0 - t1), 0)
            time.sleep(wait)

    gepool.spawn(f)
    gepool.join()


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
            )
        p = multiprocessing.Process(
            target=worker_main,
            args=target_args)
        p.start()
        p_list.append(p)
    for p in p_list:
        p.join()


if __name__ == '__main__':
    main()

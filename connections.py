from gevent import monkey; monkey.patch_all()

import argparse
import multiprocessing
import sys
import time

from gevent.pool import Pool
import requests


def x(session, url):
    try:
        resp = session.get(url)
        resp.raise_for_status()
    except Exception as e:
        print(e)
        return False
    return True


def run_session(url, interval):
    session = requests.Session()
    i = 0
    while True:
        ok = x(session, url)
        if not ok:
            break
        time.sleep(interval)
        i += 1


def launch(pool, url, connections, connection_per_second, request_interval):
    interval = 1.0 / connection_per_second
    for i in range(connections):
        pool.spawn(run_session, url, request_interval)
        time.sleep(interval)


def get_args():
    p = argparse.ArgumentParser()
    p.add_argument('url')
    p.add_argument('--connections', type=int, required=True)
    p.add_argument('--connection-per-second', type=float, default=100000.0)
    p.add_argument('--request-interval', type=float, default=10.0)
    p.add_argument('--processes', type=int)
    args = p.parse_args()
    if not args.processes:
        args.processes = multiprocessing.cpu_count()
    return args


def worker_main(url, connections, connection_per_second, request_interval):
    pool = Pool(connections + 10)
    pool.spawn(launch, pool, url, connections, connection_per_second, request_interval)
    pool.join()


def n_split(total, n):
    return [(total + i) // n for i in range(n)]


def main():
    args = get_args()
    p_list = []
    for connections in n_split(args.connections, args.processes):
        share = float(connections) / args.connections
        connection_per_second = args.connection_per_second * share
        target_args = (
            args.url,
            connections,
            connection_per_second,
            args.request_interval,
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

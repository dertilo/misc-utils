"""
copypasted from https://github.com/dertilo/tilosutils
"""
import collections
import os
import queue
import subprocess
from concurrent import futures as cf, futures
from concurrent.futures import wait, as_completed, FIRST_COMPLETED
from multiprocessing.pool import ThreadPool
from time import time
from typing import Callable, Optional, Any
from typing import Generator
from typing import Iterable
from typing import List
from typing import TypeVar

from beartype import beartype


def exec_command(command):
    with subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        stdout, stderr = p.stdout.readlines(), p.stderr.readlines()
    return stdout, stderr


def exec_command_print_stdout(command: str):
    with subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        while p.poll() is None:
            for l in iter(p.stdout.readline, ""):
                print(l.decode("utf-8").rstrip())
            for l in iter(p.stderr.readline, ""):
                print(l.decode("utf-8").rstrip())


def exec_command_yield_stdout(command: str):
    with subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        while p.poll() is None:
            for l in iter(p.stdout.readline, ""):
                yield l.decode("utf-8").rstrip()

            for l in iter(p.stderr.readline, ""):
                print(l.decode("utf-8").rstrip())


def process_batchwise(process_fun, iterable: Iterable, batch_size=1024):
    return (
        d
        for batch in iterable_to_batches(iterable, batch_size)
        for d in process_fun(batch)
    )


def consume_batchwise(consume_fun, iterable: Iterable, batch_size=1024):
    for batch in iterable_to_batches(iterable, batch_size):
        consume_fun(batch)


T = TypeVar("T")


def iterable_to_batches(
    g: Iterable[T], batch_size: int
) -> Generator[list[T], None, None]:
    g = iter(g) if not isinstance(g, collections.Iterator) else g
    batch = []
    while True:
        try:
            batch.append(next(g))
            if len(batch) == batch_size:
                yield batch
                batch = []
        except StopIteration:  # there is no next element in iterator
            break
    if len(batch) > 0:
        yield batch


@beartype
def process_with_threadpool(
    data: list, process_fun: Callable, max_workers=1, timeout=None
):
    """see: https://docs.python.org/3/library/concurrent.futures.html"""
    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sample = [executor.submit(process_fun, d) for d in data]
        for future in cf.as_completed(future_to_sample, timeout=timeout):
            yield future.result()


# TODO: stupid multiprocessing Pool does not know back-pressure!!!
# see: https://stackoverflow.com/questions/30448267/multiprocessing-pool-imap-unordered-with-fixed-queue-size-or-buffer


@beartype
def process_with_threadpool_backpressure(
    process_batch_fun: Callable[[list[Any]], Any],
    data: Iterable,
    max_workers=1,
    batch_size: int = 1,
):
    it = iter(iterable_to_batches(data, batch_size=batch_size))

    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_batch_fun, next(it)) for _ in range(max_workers)
        ]
        while True:
            try:
                completed, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fu in completed:
                    yield from fu.result()
                    futures.add(executor.submit(process_batch_fun, next(it)))
            except StopIteration:
                break

        for fu in cf.as_completed(futures):
            yield from fu.result()


class ThreadPoolWithQueueSizeLimit(ThreadPool):
    """
    # https://stackoverflow.com/questions/48263704/threadpoolexecutor-how-to-limit-the-queue-maxsize
    TODO: not working for multiprocessing.pool.ThreadPool !!!
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("not working!")

        self.maxsize = kwargs["processes"]
        super().__init__(*args, **kwargs)
        self._taskqueue = queue.Queue(maxsize=100)


def main():
    start = time()

    def sleep(k):
        os.system("sleep 1")
        return k

    data = [{"k": k} for k in range(10)]
    print(list(process_with_threadpool(data, sleep, 10)))
    print(
        "concurrently sleeping %d times in took %0.2f seconds"
        % (len(data), time() - start)
    )


if __name__ == "__main__":
    main()

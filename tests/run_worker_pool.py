import multiprocessing

import sys

from misc_utils.filelock_queuing import FileBasedWorker


def worker(k):
    FileBasedWorker(queue_dir=queue_dir).run()


if __name__ == "__main__":
    queue_dir = sys.argv[1]

    n = 3
    with multiprocessing.Pool(processes=n) as p:
        result = list(p.imap_unordered(worker, list(range(n))))

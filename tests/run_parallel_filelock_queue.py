import os

import sys

from misc_utils.cached_data import DEFAULT_CACHE_BASES
from misc_utils.dummy_task import DummyTask
from misc_utils.filelock_queuing import (
    FileLockQueuedCacheBuilder,
)

if __name__ == "__main__":

    """
    python tests/run_parallel_filelock_queue.py $BASE_PATH/data/cache/JOB_QUEUE
    """
    DEFAULT_CACHE_BASES[
        "debug"
    ] = f"{os.environ['BASE_PATH']}/data/cache/filelock_queuing_debug"
    queue_dir = sys.argv[1]

    o = ParallelBuildableList(
        [
            FileLockQueuedCacheBuilder(
                task=DummyTask(input=f"{k}-100"), queue_dir=queue_dir
            )
            for k in range(3 * 9)
        ]
    )
    o.build()
    print([d.task.state for d in o.data])

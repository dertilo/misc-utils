from misc_utils.dummy_task import DummyTask
from misc_utils.persist_queue_cache_building import (
    ParallelBuildableList,
    BuildCachedDataElseWhere,
)

if __name__ == "__main__":
    queue_dir = "/tmp/queue_dir"

    o = ParallelBuildableList(
        [
            BuildCachedDataElseWhere(task=DummyTask(input=f"{k}"), queue_dir=queue_dir)
            for k in range(3)
        ]
    )
    o.build()

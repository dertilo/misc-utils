import dataclasses
import itertools
from abc import abstractmethod
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Union, TypeVar, Generic, ClassVar, Iterable, Iterator, Optional

import sys
from time import sleep

from misc_utils.buildable import Buildable
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import (
    UNDEFINED,
    _UNDEFINED,
    hash_dataclass,
    serialize_dataclass,
)
from misc_utils.filelock_queuing import (
    _POISONPILL_TASK,
    FileBasedJobQueue,
    FileBasedJob,
    POISONPILL_TASK,
)
from misc_utils.prefix_suffix import PrefixSuffix

T = TypeVar("T")


@dataclass
class BuildCacheElseWhere(Buildable):  # Generic[T]
    """
    elsewhere != mainprocess!
    """

    task: Union[_UNDEFINED, CachedData, _POISONPILL_TASK] = UNDEFINED
    teardown_sleep_time: float = 1.0

    # callback_dir: str = dataclasses.field(init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        always returns True to prevent triggering build of children
        """
        task_is_ready = not self.task is POISONPILL_TASK and self.task._is_ready
        # self.callback_dir=f"{self.queue_dir}_{uuid.uuid4()}"
        if not task_is_ready:
            print(f"putting {self.task.name=} in queue")
            self._put_task_in_queue()
        else:
            print(f"{self.task.name=} is ready!")
        return True

    @abstractmethod
    def _put_task_in_queue(self):
        raise NotImplementedError

    def task_is_done(self):
        return self.task._found_and_loaded_from_cache()

    def _tear_down_self(self) -> Any:
        # q = Queue(self.callback_dir, serializer=json_serializer)
        wait_message = f"waiting for {self.task.name} {type(self.task)}"
        # TODO: fail-case? currently in case of failure it hangs forever!
        for k in itertools.count():
            if self.task_is_done():
                break
            elif k == 0:
                print(wait_message)
            else:
                sys.stdout.write(
                    f"\ralready waiting for {k*self.teardown_sleep_time} seconds"
                )
                sys.stdout.flush()
                sleep(self.teardown_sleep_time)

        return super()._tear_down_self()


@dataclass
class FileLockQueuedCacheBuilder(BuildCacheElseWhere):
    rank: Optional[int] = None
    queue_dir: Union[_UNDEFINED, PrefixSuffix] = UNDEFINED

    def __post_init__(self):
        self.queue: FileBasedJobQueue = FileBasedJobQueue(self.queue_dir).build()
        self.task_hash = hash_dataclass(self.task)

    def _put_task_in_queue(self):
        if self.rank is None:
            rank = round(
                (datetime.now() - datetime(2022, 1, 1, 0, 0, 0)).total_seconds()
            )
        else:
            rank = self.rank
        self.job = FileBasedJob(
            id=f"job-{self.task_hash}",
            task=serialize_dataclass(self.task),
            rank=rank,
        )
        self.queue.put(self.job)

    # TODO: overriding like this prevented loading the cache, no silent failing, someone else should catch the exception!
    # def task_is_done(self):
    #     """
    #     even if failed considered as done -> someone else handle fail-case later!
    #     """
    #     job_done_file_exists = os.path.isfile(self.job.job_file(self.queue.done_dir))
    #     return job_done_file_exists


@dataclass
class ParallelFileLockQueuedCacheBuilding(Buildable, Iterable[T]):
    tasks: list[T] = dataclasses.field(init=True, repr=True)
    queue_dir: Union[_UNDEFINED, PrefixSuffix] = UNDEFINED
    teardown_sleep_time: float = 10.0

    # flq_tasks: list[T] = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        """
        packing/wrapping tasks within FileLockQueuedCacheBuilder
        """
        self.tasks = [
            FileLockQueuedCacheBuilder(
                rank=k,
                task=task,
                queue_dir=self.queue_dir,
                teardown_sleep_time=self.teardown_sleep_time,
            )
            for k, task in enumerate(self.tasks)
        ]

    def _build_self(self):
        for k, task in enumerate(self.tasks):
            self.tasks[k] = task.build()

        self._tear_down()

    def _tear_down_all_chrildren(self):
        self.tasks = [x._tear_down() for x in self.tasks]

    def __iter__(self) -> Iterator[T]:
        """
        unpacking task of type T from FileLockQueuedCacheBuilder
        """
        self.tasks: Iterable[FileLockQueuedCacheBuilder]
        for flq_task in self.tasks:
            yield flq_task.task

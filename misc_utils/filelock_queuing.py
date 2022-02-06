import dataclasses
import json
import os
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Any, TypeVar, Union, Iterable, Iterator

import sys
from beartype import beartype
from filelock import FileLock
from time import sleep

from data_io.readwrite_files import read_file, write_file, write_json
from misc_utils.build_cache_elsewhere import BuildCacheElseWhere
from misc_utils.buildable import Buildable, BuildableList
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import (
    serialize_dataclass,
    deserialize_dataclass,
    UNDEFINED,
    _UNDEFINED,
    hash_dataclass,
)

T = TypeVar("T")

"""
https://github.com/peter-wangxu/persist-queue/issues/148
filelock works on nfs? https://github.com/tox-dev/py-filelock/issues/73
"""
# @dataclass
# class BuildCacheJob:
#     callback_dir: str
#     serialized_dataclass: str


@dataclass
class FileBasedJob:
    id: str
    task: str  # serializabled dataclass
    rank: int

    def __post_init__(self):
        assert "_target_" in self.task

    def job_file(self, queue_dir: str) -> str:
        return f"{queue_dir}/{self.id}-rank_{self.rank}.json"


# @dataclass
# class _POISONPILL(metaclass=Singleton):
#     pass
#
#
# POISONPILL = _POISONPILL()


@beartype
def consume_file(
    get_job_file: Callable[[], Optional[str]], break_if_no_file=True
) -> tuple[Optional[str], Optional[str]]:
    content, file = None, None
    while content is None:
        file = get_job_file()
        if file is None:
            # sleep(3)
            if break_if_no_file:
                break
            else:
                continue

        with FileLock(f"{file}.lock", timeout=0.1):
            if os.path.isfile(file):
                content = read_file(file)
                os.remove(file)
                os.remove(f"{file}.lock")
                break
            else:
                if os.path.isfile(f"{file}.lock"):
                    os.remove(f"{file}.lock")
        # if content is None:
        #     print(f"failed to consume file!")
        #     sleep(3)
    return file, content


def write_job(queue_dir: str, job: FileBasedJob):
    file = job.job_file(queue_dir)
    with FileLock(f"{file}.lock", timeout=1):
        write_json(
            file,
            asdict(job),
        )
    os.remove(f"{file}.lock")


@dataclass
class FileBasedJobQueue(Buildable):
    queue_dir: str

    @property
    def states(self):
        return ["TODO", "DONE", "DOING"]

    @property
    def todo_dir(self):
        return f"{self.queue_dir}/TODO"

    @property
    def doing_dir(self):
        return f"{self.queue_dir}/DOING"

    @property
    def done_dir(self):
        return f"{self.queue_dir}/DONE"

    def _build_self(self) -> Any:
        for s in self.states:
            os.makedirs(f"{self.queue_dir}/{s}", exist_ok=True)

    @beartype
    def get(self) -> Optional[FileBasedJob]:
        def fifo_order(f):
            timestamp_seconds = int(str(f).split("rank_")[-1].replace(".json", ""))
            return timestamp_seconds

        def get_fifo_file():
            job_files = [p for p in Path(self.todo_dir).glob("*.json")]
            job_files = [
                str(p)
                for p in sorted(
                    job_files,
                    key=fifo_order,
                )
            ]
            if len(job_files) > 0:
                return job_files[0]
            else:
                return None

        file, content = consume_file(get_job_file=get_fifo_file)
        if file is None:
            return None
        job = FileBasedJob(**json.loads(content))
        write_job(self.doing_dir, job)
        return job

    @beartype
    def put(self, job: FileBasedJob):
        write_job(self.todo_dir, job)

    @beartype
    def done(self, done_job: FileBasedJob):
        file, content = consume_file(
            get_job_file=lambda: done_job.job_file(self.doing_dir)
        )
        doing_job = FileBasedJob(**json.loads(content))
        assert doing_job is not None
        write_job(self.done_dir, done_job)


@dataclass
class FileBasedWorker:
    queue_dir: str
    stop_on_error: bool = False
    wait_even_though_queue_is_empty: bool = True

    def run(self):
        job_queue = FileBasedJobQueue(queue_dir=self.queue_dir)
        job_queue.build()

        while True:
            job: Optional[FileBasedJob] = job_queue.get()
            if job is not None:
                self._process_job(job, job_queue)
            else:
                if self.wait_even_though_queue_is_empty:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    sleep(3)
                    continue
                else:
                    break

    def _process_job(self, job, job_queue):
        error = None

        try:
            buildable = deserialize_dataclass(job.task)
            print(f"doing {job.id}")
            job.task = serialize_dataclass(buildable.build())
        except Exception as e:
            print(f"{job.id} FAILED!!! with {e}")
            traceback.print_exc()
            error = e
            if self.stop_on_error:
                raise e
        finally:
            print(f"done {job.id}")
            if error is not None:
                write_file(
                    job.job_file(job_queue.done_dir).replace(".json", "_error.txt"),
                    str(error),
                )
            job_queue.done(job)


@dataclass
class FileLockQueuedCacheBuilder(BuildCacheElseWhere):
    queue_dir: Union[_UNDEFINED, str] = UNDEFINED

    def __post_init__(self):
        self.queue = FileBasedJobQueue(self.queue_dir).build()
        self.task_hash = hash_dataclass(self.task)

    def _put_task_in_queue(self):
        rank = round((datetime.now() - datetime(2022, 1, 1, 0, 0, 0)).total_seconds())
        self.job = FileBasedJob(
            id=f"job-{self.task_hash}",
            task=serialize_dataclass(self.task),
            rank=rank,
        )
        self.queue.put(self.job)


@dataclass
class ParallelFileLockQueuedCacheBuilding(Buildable, Iterable[T]):
    tasks: list[T] = dataclasses.field(init=True, repr=True)
    queue_dir: Union[_UNDEFINED, str] = UNDEFINED

    # flq_tasks: list[T] = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        """
        packing/wrapping tasks within FileLockQueuedCacheBuilder
        """
        self.tasks = [
            FileLockQueuedCacheBuilder(task=task, queue_dir=self.queue_dir)
            for task in self.tasks
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

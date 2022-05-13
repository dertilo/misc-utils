import json
import os
import random
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional, Callable, Any, TypeVar

import sys

from beartype import beartype
from filelock import FileLock, Timeout
from time import sleep

from data_io.readwrite_files import read_file, write_file, write_json
from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import (
    serialize_dataclass,
    deserialize_dataclass,
)
from misc_utils.prefix_suffix import PrefixSuffix
from misc_utils.utils import Singleton, retry

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
class _POISONPILL_TASK(metaclass=Singleton):
    @property
    def name(self):
        return self.__class__.__name__

    def _found_and_loaded_from_cache(self):
        return True


POISONPILL_TASK = _POISONPILL_TASK()


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
        try:
            with FileLock(f"{file}.lock", timeout=1.0):
                if os.path.isfile(file):
                    content = read_file(file)
                    os.remove(file)
                    os.remove(f"{file}.lock")
                    break
                else:
                    if os.path.isfile(f"{file}.lock"):
                        os.remove(f"{file}.lock")
        except Timeout:
            sleep(random.uniform(0.5, 1))
            print(f"could not consume file: {file}")
        # if content is None:
        #     print(f"failed to consume file!")
        #     sleep(3)
    return file, content


def write_job(queue_dir: str, job: FileBasedJob):
    file = job.job_file(queue_dir)
    no_timeout = -1.0
    with FileLock(f"{file}.lock", timeout=no_timeout):
        write_json(
            file,
            asdict(job),
        )
    os.remove(f"{file}.lock")


@dataclass
class FileBasedJobQueue(Buildable):
    """
    FileLock-Queue
    ──█──█──█──█──█──█──█──█
    ──▄▀▀▀▄───────────────
    ──█───█───────────────
    ─███████─────────▄▀▀▄─
    ░██─▀─██░░█▀█▀▀▀▀█░░█░
    ░███▄███░░▀░▀░░░░░▀▀░░

    """

    queue_dir: PrefixSuffix

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
            sleep(random.uniform(0.1, 1))
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
        assert file is not None
        write_job(self.done_dir, done_job)


NUM_RETRIES = int(os.environ.get("NUM_RETRIES", 3))


@dataclass
class FileBasedWorker:
    queue_dir: PrefixSuffix
    worker_name: str = "noname"
    stop_on_error: bool = False
    wait_even_though_queue_is_empty: bool = True

    job_queue: FileBasedJobQueue = field(init=False, repr=False)

    def run(self):
        print(f"worker for {self.queue_dir=}")
        # wandb.init(project="asr-inference", name=self.worker_name)

        job_queue = FileBasedJobQueue(queue_dir=self.queue_dir)
        job_queue.build()

        idle_counter = 0
        wait_time = 3.0

        while True:
            job: Optional[FileBasedJob] = job_queue.get()
            if job is not None:
                got_poisoned = self._process_job(job, job_queue)
                if got_poisoned:
                    print(f"got poisoned")
                    break
                idle_counter = 0
            else:
                idle_counter += 1

                if self.wait_even_though_queue_is_empty:
                    sys.stdout.write(f"\r idle for {idle_counter*wait_time} seconds")
                    sys.stdout.flush()
                    sleep(wait_time)
                    continue
                else:
                    break

    def _process_job(self, job, job_queue: FileBasedJobQueue):
        error = None
        got_poisoned = False

        try:
            buildable = deserialize_dataclass(job.task)
            if buildable is POISONPILL_TASK:
                got_poisoned = True
                return got_poisoned
            retry(
                lambda: self._doing_job(job),
                num_retries=NUM_RETRIES,
                wait_time=1.0,
                increase_wait_time=True,
            )
        except Exception as e:
            print(f"{job.id} FAILED!!! with {e}")
            traceback.print_exc()
            error = e

        finally:
            print(f"done {job.id}")
            if error is not None:
                write_file(
                    job.job_file(job_queue.done_dir).replace(
                        ".json", f"{self.worker_name}_error.txt"
                    ),
                    str(error),
                )
            # job_queue.put(job) # could requeue it!
            job_queue.done(job)
            if error is not None and self.stop_on_error:
                raise error
        return got_poisoned

    def _doing_job(self, job):
        buildable = deserialize_dataclass(job.task)
        print(f"doing {job.id} with {NUM_RETRIES=}")
        job.task = serialize_dataclass(buildable.build())

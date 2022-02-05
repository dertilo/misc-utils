import os
import traceback

import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from time import sleep
from typing import Any, Optional, Callable

from beartype import beartype
from filelock import FileLock

from data_io.readwrite_files import read_file, write_file, read_json, write_json
from misc_utils.beartypes import NeList
from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass
from misc_utils.utils import Singleton


@dataclass
class FileBasedJob:
    id: str
    task: str  # serializabled dataclass
    rank: int

    def __post_init__(self):
        assert "_target_" in self.task

    def job_file(self, queue_dir: str) -> str:
        return f"{queue_dir}/{self.id}-rank_{self.rank}.json"


@dataclass
class DummyTask(Buildable):
    data: str = "foo"

    def _build_self(self) -> Any:
        sleep(3)
        print(f"dummy job {self.data} done!")


# @dataclass
# class _POISONPILL(metaclass=Singleton):
#     pass
#
#
# POISONPILL = _POISONPILL()


@beartype
def consume_file(
    get_job_file: Callable[[], Optional[str]],
) -> tuple[Optional[str], Optional[str]]:
    content, file = None, None
    while content is None:
        file = get_job_file()
        if file is None:
            # sleep(3)
            continue

        with FileLock(f"{file}.lock", timeout=0.1):
            if os.path.isfile(file):
                content = read_file(file)
                os.remove(file)
                os.remove(f"{file}.lock")
                break
            else:
                os.remove(f"{file}.lock")
        # if content is None:
        #     print(f"failed to consume file!")
        #     sleep(3)
    return file, content


@beartype
def consume_job(
    get_job_file: Callable[[], NeList[str]],
) -> FileBasedJob:
    """
    TODO: not sure if this is really thread-safe!
    :param job_files:
    :return:
    """
    job: Optional[FileBasedJob] = None
    file = None
    while job is None:
        file = get_job_file()
        with FileLock(f"{file}.lock", timeout=0.1):
            if os.path.isfile(file):
                job: FileBasedJob = FileBasedJob(**read_json(file))
                os.remove(file)
                break
            else:
                os.remove(f"{file}.lock")
        if job is None:
            print(f"failed to get job! -> retrying!")
            sleep(3)
    os.remove(f"{file}.lock")
    return job


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
        job_files = [p for p in Path(self.todo_dir).glob("*.json")]
        if len(job_files) == 0:
            return None

        def fifo_order(f):
            timestamp_seconds = int(str(f).split("rank_")[-1].replace(".json", ""))
            return timestamp_seconds

        job_files = [
            str(p)
            for p in sorted(
                job_files,
                key=fifo_order,
            )
        ]
        job = consume_job(job_files=job_files)
        if job is None:
            return None
        write_job(self.doing_dir, job)
        return job

    @beartype
    def put(self, job: FileBasedJob):
        write_job(self.todo_dir, job)

    @beartype
    def done(self, done_job: FileBasedJob):
        doing_job = consume_job(job_files=[done_job.job_file(self.doing_dir)])
        assert doing_job is not None
        write_job(self.done_dir, done_job)


@dataclass
class FileBasedWorker:
    queue_dir: str
    stop_on_error: bool = False
    wait_for_jobs: bool = True

    def run(self):
        job_queue = FileBasedJobQueue(queue_dir=self.queue_dir)
        job_queue.build()

        while True:
            job: Optional[FileBasedJob] = job_queue.get()
            error = None
            if job is None:
                if self.wait_for_jobs:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    sleep(3)
                    continue
                else:
                    break
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


if __name__ == "__main__":

    queue_dir = "/tmp/job_queue"
    data_job_queue = FileBasedJobQueue(queue_dir=queue_dir).build()
    jobs = [
        FileBasedJob(
            id=f"test-job-{k}",
            rank=k,
            task=serialize_dataclass(DummyTask(data=f"datum-{k}")),
        )
        for k in range(3)
    ]
    for j in jobs:
        data_job_queue.put(j)

    FileBasedWorker(queue_dir=queue_dir).run()

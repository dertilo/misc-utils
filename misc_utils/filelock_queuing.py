import json
import os
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional, Callable, Any, TypeVar, Union

import sys
from beartype import beartype
from filelock import FileLock
from persistqueue import Queue
from persistqueue.serializers import json as json_serializer
from time import sleep

from data_io.readwrite_files import read_file, write_file, write_json
from misc_utils.buildable import Buildable, BuildableList
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass, \
    encode_dataclass, UNDEFINED, _UNDEFINED

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
class BuildCachedDataElseWhere(Buildable):
    task: Union[_UNDEFINED, CachedData] = UNDEFINED
    queue_dir: Union[_UNDEFINED, str] = UNDEFINED

    # callback_dir: str = dataclasses.field(init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        always returns True to prevent triggering build of children
        """
        is_ready = self.task._found_and_loaded_from_cache()
        # self.callback_dir=f"{self.queue_dir}_{uuid.uuid4()}"
        if not is_ready:
            q = Queue(self.queue_dir, serializer=json_serializer)
            q.put(encode_dataclass(self.task))
        return True

    def _tear_down_self(self) -> Any:
        # q = Queue(self.callback_dir, serializer=json_serializer)
        wait_message = f"waiting for {self.task.name} {type(self.task)}"
        while not self.task._found_and_loaded_from_cache():
            print(wait_message)
            sleep(1.0)

        return super()._tear_down_self()


@dataclass
class ParallelBuildableList(BuildableList[BuildCachedDataElseWhere]):
    def _build_self(self):
        super()._build_self()
        self._tear_down()


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

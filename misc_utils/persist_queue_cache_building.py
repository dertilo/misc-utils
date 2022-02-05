from dataclasses import dataclass
from dataclasses import dataclass
from typing import Any, TypeVar, Union

from persistqueue import Queue
from persistqueue.serializers import json as json_serializer
from time import sleep

from misc_utils.buildable import Buildable, BuildableList
from misc_utils.cached_data import CachedData
from misc_utils.cached_data_specific import CachedList
from misc_utils.dataclass_utils import (
    encode_dataclass,
    UNDEFINED,
    _UNDEFINED,
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

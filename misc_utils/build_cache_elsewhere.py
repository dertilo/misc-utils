import itertools
from abc import abstractmethod
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Union, TypeVar, Generic, ClassVar

import sys
from time import sleep

from misc_utils.buildable import Buildable
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import (
    UNDEFINED,
    _UNDEFINED,
)


@dataclass
class BuildCacheElseWhere(Buildable):  # Generic[T]
    """
    elsewhere != mainprocess!
    """

    task: Union[_UNDEFINED, CachedData] = UNDEFINED
    teardown_sleep_time: float = 1.0

    # callback_dir: str = dataclasses.field(init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        always returns True to prevent triggering build of children
        """
        task_is_ready = self.task._is_ready
        # self.callback_dir=f"{self.queue_dir}_{uuid.uuid4()}"
        if not task_is_ready:
            self._put_task_in_queue()
        return True

    @abstractmethod
    def _put_task_in_queue(self):
        raise NotImplementedError

    def _tear_down_self(self) -> Any:
        # q = Queue(self.callback_dir, serializer=json_serializer)
        wait_message = f"waiting for {self.task.name} {type(self.task)}"
        # TODO: fail-case? currently in case of failure it hangs forever!
        for k in itertools.count():
            if self.task._found_and_loaded_from_cache():
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

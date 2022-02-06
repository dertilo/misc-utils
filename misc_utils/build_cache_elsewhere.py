from abc import abstractmethod
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Union

import sys
from time import sleep

from misc_utils.buildable import Buildable
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import (
    UNDEFINED,
    _UNDEFINED,
)


@dataclass
class BuildCacheElseWhere(Buildable):
    """
    elsewhere != mainprocess!
    """

    task: Union[_UNDEFINED, CachedData] = UNDEFINED

    # callback_dir: str = dataclasses.field(init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        always returns True to prevent triggering build of children
        """
        is_ready = self.task._found_and_loaded_from_cache()
        # self.callback_dir=f"{self.queue_dir}_{uuid.uuid4()}"
        if not is_ready:
            self._put_task_in_queue()
        return True

    @abstractmethod
    def _put_task_in_queue(self):
        raise NotImplementedError

    def _tear_down_self(self) -> Any:
        # q = Queue(self.callback_dir, serializer=json_serializer)
        wait_message = f"waiting for {self.task.name} {type(self.task)}"
        # TODO: fail-case? currently in case of failure it hangs forever!
        while not self.task._found_and_loaded_from_cache():
            sys.stdout.write(f"{wait_message}")
            sys.stdout.flush()
            sleep(1.0)
            wait_message = "."

        return super()._tear_down_self()

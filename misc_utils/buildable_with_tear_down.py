import dataclasses
import os
from dataclasses import dataclass, fields
from time import time
from typing import Any, Generic, TypeVar, final

from beartype import beartype

from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import (
    all_undefined_must_be_filled,
)


@dataclass
class BuildableWithTearDown(Buildable):
    # TODO(tilo): only used for very special edgecase -> I should remove this!
    _was_teared_down: bool = dataclasses.field(default=False, init=False, repr=False)

    @property
    def _is_down(self) -> bool:
        """
        if true prevents going deeper into dependency-tree/graph
        """
        return self._was_teared_down

    def _tear_down(self):
        """
        # this is completely optional
        recursively goes through graph in same order as build
        how to handle multiple tear-down calls?

        motivation: buildable as client that communicates with some worker, worker has alters its own state which is collected during tear-down
        and worker is told to tear-down itself
        """
        self._tear_down_all_chrildren()
        return self._tear_down_self()

    def _tear_down_all_chrildren(self):
        for f in fields(self):
            if hasattr(
                self, f.name
            ):  # field initialized with field() -method without default do not exist! but are listed by fields!
                obj = getattr(self, f.name)
                if isinstance(obj, Buildable):
                    # whoho! black-magic here! the child can overwrite itself and thereby shape-shift completely!
                    setattr(self, f.name, obj._tear_down())

    def _tear_down_self(self) -> Any:

        """
        # this is completely optional

        override this if you want custom tear-down behavior
        use shape-shifting here, to prevent tear-downs are triggered multiple times
        shape-shifting: Dataclass -> dict , via: encode_dataclass(self)
        """
        self._was_teared_down = True
        return self

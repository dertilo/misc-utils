import dataclasses
from abc import ABC
from dataclasses import dataclass, fields
from time import time
from typing import Any, Generic, TypeVar, final

from beartype import beartype

from misc_utils.dataclass_utils import (
    all_undefined_must_be_filled,
)
from misc_utils.final_methods import Access


@dataclass
class Buildable:
    # TODO:?
    # def __enter__(self):
    #     self.build()
    #
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     pass
    #
    _was_built: bool = dataclasses.field(default=False, init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        if true prevents going deeper into dependency-tree/graph
        """
        return self._was_built

    @beartype
    @final  # does not enforce it but at least the IDE warns you!
    def build(self) -> "Buildable":
        """
        should NOT be overwritten!
        """
        if not self._is_ready:
            all_undefined_must_be_filled(self)
            self._build_all_chrildren()
            start = time()
            o = self._build_self()
            if o is None:
                o = self
            self._was_built = True
            duration = time() - start
            if duration > 1.0:
                print(
                    f"build_self of {self.__class__.__name__} took:{duration} seconds"
                )
        else:
            print(f"not building {self.__class__.__name__}, is ready!")
            o = self
        assert o is not None  # TODO: should be done be beartype!
        return o

    def _build_all_chrildren(self):
        for f in fields(self):
            if f.init:
                obj = getattr(self, f.name)
                if isinstance(obj, Buildable):
                    # whoho! black-magic here! the child can overwrite itself and thereby shape-shift completely!
                    setattr(self, f.name, obj.build())

    def _build_self(self) -> Any:
        """
        might return the built object which not necessarily has to be "self", can be Any!
        fully optional, no need to implement this, only if needed
        """
        pass


T = TypeVar("T")


@dataclass
class BuildableContainer(Generic[T], Buildable):
    data: T

    def _build_self(self):
        print(f"triggered build for {self.__class__.__name__}")
        if isinstance(self.data, (list, tuple)):
            g = (x for x in self.data)
        elif isinstance(self.data, dict):
            g = (v for v in self.data.values())
        else:
            raise NotImplementedError

        for obj in g:
            if hasattr(obj, "build"):
                obj.build()


@dataclass
class BuildableList(Buildable, list[T]):
    """
    see: Init-only variables ->  https://docs.python.org/3/library/dataclasses.html
    """

    # TODO: beartype does not like InitVar
    # init_only_data: dataclasses.InitVar[list]=None
    data: list[T] = dataclasses.field(init=True, repr=True)

    def __post_init__(self):
        self.extend(self.data)

    def _build_self(self):
        print(f"triggered build for {self.__class__.__name__}")
        for k, obj in enumerate(self):
            if isinstance(obj, Buildable):
                self[k] = obj.build()


# @dataclass
# class BuildableWrapper(Generic[T], Buildable):
#     """
#     might be used to wrap immutables like int
#     """
#
#     data: T
#
#     def build(self) -> T:
#         return self.data

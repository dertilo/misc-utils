import dataclasses
from dataclasses import dataclass, fields
from typing import Any, Generic, TypeVar, final

from beartype import beartype
from time import time
import os
from misc_utils.dataclass_utils import (
    all_undefined_must_be_filled, )

DEBUG_MEMORY_LEAK = os.environ.get("DEBUG_MEMORY_LEAK", "False").lower() != "false"
if DEBUG_MEMORY_LEAK:
    print(f"DEBUGGING MODE in {__name__}")


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
    _was_teared_down: bool = dataclasses.field(default=False, init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        if true prevents going deeper into dependency-tree/graph
        """
        return self._was_built

    @property
    def _is_down(self) -> bool:
        """
        if true prevents going deeper into dependency-tree/graph
        """
        return self._was_teared_down

    @beartype
    @final  # does not enforce it but at least the IDE warns you!
    def build(self) -> Any:  # no restriction to "Buildable" to allow shape-shifting!
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
            # print(f"not building {self.__class__.__name__}, is ready!")
            self._was_built = True  # being ready is as good as being built
            o = self
        assert o is not None  # TODO: should be done be beartype!
        return o

    def _build_all_chrildren(self):
        for f in fields(self):
            is_argument_of_dataclasses_init_method = f.init
            if is_argument_of_dataclasses_init_method:
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

    def _tear_down_all_chrildren(self):
        if isinstance(self.data, (list, tuple)):
            self.data = [x._tear_down() for x in self.data]
        elif isinstance(self.data, dict):
            self.data = {k: v._tear_down() for k, v in self.data.items()}
        else:
            raise NotImplementedError


if DEBUG_MEMORY_LEAK:
    """
    usefull to find memory leaks when build multiple objects via BuildableList
    """
    from pympler.tracker import SummaryTracker

    tracker = SummaryTracker()


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
        if DEBUG_MEMORY_LEAK:
            tracker.print_diff()
        print(f"triggered build for {self.__class__.__name__}")
        for k, obj in enumerate(self):
            if isinstance(obj, Buildable):
                self[k] = obj.build()
                if DEBUG_MEMORY_LEAK:
                    tracker.print_diff()
                    # objgraph.show_most_common_types(limit=20)
                    # breakpoint()

    def _tear_down_all_chrildren(self):
        self.data = [x._tear_down() for x in self.data]


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

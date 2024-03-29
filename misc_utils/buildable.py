import dataclasses
import os
from dataclasses import dataclass, fields
from time import time
from typing import Any, Generic, TypeVar, final

from beartype import beartype

from misc_utils.dataclass_utils import (
    all_undefined_must_be_filled,
)

DEBUG = os.environ.get("DEBUG", "False").lower() != "false"
DEBUG_MEMORY_LEAK = os.environ.get("DEBUG_MEMORY_LEAK", "False").lower() != "false"
if DEBUG_MEMORY_LEAK:
    print(f"DEBUGGING MODE in {__name__}")


@dataclass # TODO: remove dataclass here to allow frozen=True in downstream/inheriting
class Buildable:
    """
    base-class for "buildable Dataclasses"

    key-idea: a Dataclass has fields (attributes) those can be interpreted as "dependencies"
        in order to "build" a Dataclass it is necessary to first build all ites dependencies (=children)

    the build-method essentially does 2 things:
        1. _build_all_children
        2. _build_self

    if the buildable-object "_is_ready" then it does NOT build any children and also not itself!
    """

    _was_built: bool = dataclasses.field(default=False, init=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        if true prevents going deeper into dependency-tree/graph
        """
        return self._was_built

    @beartype
    @final  # does not enforce it but at least the IDE warns you!
    def build(self) -> Any:  # no restriction to "Buildable" to allow shape-shifting!
        """
        should NOT be overwritten!
        """
        if not self._is_ready:
            all_undefined_must_be_filled(self)
            self._build_all_children()
            start = time()
            o = self._build_self()
            if o is None:
                o = self
            self._was_built = True
            duration = time() - start
            if duration > 1.0 and DEBUG:
                print(
                    f"build_self of {self.__class__.__name__} took:{duration} seconds"
                )
                # traceback.print_stack()
        else:
            # print(f"not building {self.__class__.__name__}, is ready!")
            self._was_built = True  # being ready is as good as being built
            o = self
        assert o is not None  # TODO: should be done be beartype!
        return o

    def _build_all_children(self):
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


T = TypeVar("T")


@dataclass
class BuildableContainer(Generic[T], Buildable):
    data: T

    def _build_self(self):
        # print(f"triggered build for {self.__class__.__name__}")
        if isinstance(self.data, (list, tuple)):
            g = (x for x in self.data)
        elif isinstance(self.data, dict):
            g = (v for v in self.data.values())
        else:
            raise NotImplementedError

        for obj in g:
            if hasattr(obj, "build"):
                obj.build()  # TODO: no shapeshifting here!!

    def _tear_down_all_chrildren(self):
        if isinstance(self.data, (list, tuple)):
            self.data = [x._tear_down() for x in self.data]
        elif isinstance(self.data, dict):
            self.data = {k: v._tear_down() for k, v in self.data.items()}
        else:
            raise NotImplementedError


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class BuildableDict(Generic[K, V], Buildable):
    data: dict[K, V]

    def _build_self(self):
        for k, v in self.data.items():
            if hasattr(v, "build"):
                self.data[k] = v.build()

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def values(self):
        yield from self.data.values()

    def items(self):
        yield from self.data.items()


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

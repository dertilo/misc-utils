from typing import Annotated, Generic, TypeVar

from beartype.roar import BeartypeDecorHintPep585DeprecationWarning
from warnings import filterwarnings

filterwarnings("ignore", category=BeartypeDecorHintPep585DeprecationWarning)
from misc_utils import beartyped_dataclass_patch

from dataclasses import dataclass

from misc_utils.buildable import Buildable, BuildableContainer, BuildableList
from misc_utils.dataclass_utils import (
    volatile_state_field,
    serialize_dataclass,
    deserialize_dataclass,
)

EXPECTED_STATE = "hello"


@dataclass
class AnotherTestBuildable(Buildable):
    state: str = volatile_state_field(default=None)

    def _build_self(self):
        self.state = EXPECTED_STATE


@dataclass
class TestBuildable(Buildable):
    list_of_buildable: BuildableContainer[list[AnotherTestBuildable]]

    def _build_self(self):
        pass


def test_buildable():
    buildables = [AnotherTestBuildable(), AnotherTestBuildable()]
    buildable_container = BuildableContainer[list](buildables)
    b = TestBuildable(list_of_buildable=buildable_container).build()
    assert all((x.state == EXPECTED_STATE for x in buildables))


# def test_overriding_build():
#     # only works with misc_utils/misc_utils/final_methods.py
#     # but this prevents multi-inheritance, cause one cannot inherit from multiple (unrelated) metaclasses! -> TODO!
#     error = None
#     try:
#
#         class IllegalAttemptToOverrideBuildMethod(Buildable):
#             foo: str = "bar"
#
#             def build(self) -> "Buildable":
#                 return self
#
#     except Exception as e:
#         print(f"{e}")
#         error = e
#
#     assert error is not None


def test_buildable_list():
    o = BuildableList([AnotherTestBuildable()])
    s = serialize_dataclass(o)
    print(s)
    o2 = deserialize_dataclass(s)
    assert o == o2

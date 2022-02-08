from pprint import pprint
from typing import Annotated, Generic, TypeVar, Any

from beartype.roar import BeartypeDecorHintPep585DeprecationWarning
from warnings import filterwarnings

filterwarnings("ignore", category=BeartypeDecorHintPep585DeprecationWarning)
from misc_utils import beartyped_dataclass_patch

from dataclasses import dataclass, field

from misc_utils.buildable import Buildable, BuildableContainer, BuildableList
from misc_utils.dataclass_utils import (
    serialize_dataclass,
    deserialize_dataclass,
)

EXPECTED_STATE = "hello"
was_teared_down = "was torn down"


@dataclass
class AnotherTestBuildable(Buildable):
    state: str = field(init=False)

    def _build_self(self):
        self.state = EXPECTED_STATE

    def _tear_down_self(self) -> Any:
        self.state = was_teared_down
        return super()._tear_down_self()


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


@dataclass
class TestTearDownBuildable(Buildable):
    simple_depenency: AnotherTestBuildable
    dependency: TestBuildable
    state: str = field(init=False)

    def _build_self(self):
        self.state = "was build"
        return self._tear_down()

    def _tear_down_self(self) -> Any:
        self.state = was_teared_down
        return super()._tear_down_self()


def test_tear_down():
    teared_down_object = TestTearDownBuildable(
        simple_depenency=AnotherTestBuildable(),
        dependency=TestBuildable(
            list_of_buildable=BuildableContainer[list](
                [AnotherTestBuildable(), AnotherTestBuildable()]
            )
        ),
    ).build()
    assert all(
        (
            o.state == was_teared_down
            for o in teared_down_object.dependency.list_of_buildable.data
        )
    )


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
    o = BuildableList([AnotherTestBuildable()]).build()
    s = serialize_dataclass(o)
    print(s)
    o2 = deserialize_dataclass(s)
    assert o.data[0].state == o2.data[0].state

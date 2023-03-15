from collections import namedtuple
from dataclasses import dataclass, field, fields
from typing import Annotated, NamedTuple

# from typing_extensions import Annotated
from beartype import beartype
from beartype.door import is_bearable
from beartype.roar import BeartypeCallHintPepParamException
from beartype.vale import Is, IsInstance, IsSubclass

from misc_utils.beartypes import NeStr, Dataclass
from misc_utils.dataclass_utils import encode_dataclass, decode_dataclass


@beartype
def fun(x: NeStr):
    return None


def test_NoneEmptyString():
    fun("valid")
    error = None
    try:
        fun("")
    except BeartypeCallHintPepParamException as e:
        error = e
    assert error is not None


# TODO: Annotated[object,...] is NOT working!
# StrOrBytesInstance = Annotated[object, IsInstance[str]]
#
# @beartype
# def fun_isinstance(x: StrOrBytesInstance):
#     return None
#
#
# def test_isinstance_of_str():
#     fun_isinstance("valid")
#     error = None
#     try:
#         fun_isinstance(1.0)
#     except BeartypeCallHintPepParamException as e:
#         error = e
#     assert error is not None

# @dataclass
# class DummyDataclass:
#     x:str="foo"
#
# @beartype
# def fun_is_dataclass(x: Dataclass):
#     return None
#
#
# def test_is_dataclass():
#     fun_is_dataclass("valid")
#     error = None
#     try:
#         fun_is_dataclass(1.0)
#     except BeartypeCallHintPepParamException as e:
#         error = e
#         print(error)
#     assert error is not None
#


def beartype_with_obj_is_not_working():
    StrInstance = Annotated[object, IsInstance[str]]
    ShouldBeStr = Annotated[object, Is[lambda o: IsInstance[str]]]

    @beartype
    def provoke_the_bear(obj: StrInstance):
        return obj

    provoke_the_bear(1)
    assert not is_bearable(1, StrInstance)


@dataclass
class SomeDummy:
    a: str = field(metadata={"x": 1})
    b: str = field(metadata={"x": 2})

    def run(self):
        print([getattr(self, f.name) for f in fields(self)])
        print([getattr(self, f.name) for f in fields(self) if f.metadata["x"] == 1])


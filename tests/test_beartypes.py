from dataclasses import dataclass
from typing import Annotated

# from typing_extensions import Annotated
from beartype import beartype
from beartype.roar import BeartypeCallHintPepParamException
from beartype.vale import Is

from misc_utils.beartypes import NeStr, Dataclass


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

from misc_utils import beartyped_dataclass_patch
from dataclasses import field, dataclass
from typing import Annotated, List, Optional

import numpy as np
import torch
from beartype import beartype
from beartype.roar import BeartypeCallHintPepParamException
from beartype.vale import IsAttr, IsEqual, Is
from numpy import floating
from numpy.typing import NDArray


Numpy2DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[2]]]

LengthyString = Annotated[str, Is[lambda text: 4 <= len(text) <= 40]]

TorchTensor3D = Annotated[torch.Tensor, IsAttr["ndim", IsEqual[3]]]


@dataclass
class WrappedNumpy2DArray:
    data: Numpy2DArray = field(compare=False, repr=False)

    @beartype
    def foo_fun(self, s: LengthyString):
        print(s)


@dataclass
class DataContainer(WrappedNumpy2DArray):
    lenghtly_string: LengthyString


def test_beartyped_field():
    valid_string = "x" * 5
    invalid_string = "x" * 3

    _ = DataContainer(data=np.zeros((6, 3)), lenghtly_string=valid_string)

    error = None
    try:
        _ = DataContainer(data=np.zeros((6, 3)), lenghtly_string=invalid_string)
    except BeartypeCallHintPepParamException as e:
        error = e
    assert error is not None


@dataclass
class MustHaveSameLenDataClass:
    text: str
    ints: list[int]

    def __post_init__(self):
        """
        #TODO: what do I need pydantic for if I can do the "same" with beartype+post-init ??
        """
        have_same_len = len(self.text) == len(self.ints)
        if not have_same_len:
            raise AssertionError("must have same lenght")


def test_MustHaveSameLenDataClass():
    try:
        MustHaveSameLenDataClass(text="hello", ints=list(range(4)))
    except AssertionError as e:
        print(e)


@beartype
def dummy_fun(x: TorchTensor3D):
    pass


def test_torch_tensors():
    dummy_fun(torch.zeros((1, 2, 3)))
    error = None
    try:
        dummy_fun(torch.zeros((1, 2)))
    except BeartypeCallHintPepParamException as e:
        error = e

    assert error is not None and isinstance(error, BeartypeCallHintPepParamException)


TorchTensorFirstDimAsTwo = Annotated[
    torch.Tensor, IsAttr["shape", Is[lambda shape: shape[0] == 2]]
]


@beartype
def dummy_fun_TorchTensorFirstDimAsTwo(x: TorchTensorFirstDimAsTwo):
    pass


def test_torch_tensors_TorchTensorFirstDimAsTwo():
    dummy_fun_TorchTensorFirstDimAsTwo(torch.zeros((2, 3)))
    try:
        dummy_fun_TorchTensorFirstDimAsTwo(torch.zeros((1, 2)))
    except BeartypeCallHintPepParamException as e:
        pass

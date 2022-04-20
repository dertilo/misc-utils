import os
from dataclasses import field, dataclass
from typing import Annotated, List, Optional

import pytest
from beartype import beartype
from beartype.roar import BeartypeCallHintPepParamException
from beartype.vale import IsAttr, IsEqual, Is

from misc_utils.beartypes import bear_does_roar

assert os.environ.get("BEARTYPE_DATACLASSES_BASEDIR", "") != ""
numpy_is_installed = False
try:
    import numpy as np
    from numpy import floating
    from numpy.typing import NDArray

    Numpy2DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[2]]]

    numpy_is_installed = True

except ImportError:
    Numpy2DArray = None
    print("no numpy installed!")

LengthyString = Annotated[str, Is[lambda text: 4 <= len(text) <= 40]]

torch_is_installed = False
try:
    import torch

    TorchTensor3D = Annotated[torch.Tensor, IsAttr["ndim", IsEqual[3]]]
    TorchTensorFirstDimAsTwo = Annotated[
        torch.Tensor, IsAttr["shape", Is[lambda shape: shape[0] == 2]]
    ]
    torch_is_installed = True
except:
    TorchTensor3D = None
    TorchTensorFirstDimAsTwo = None


@dataclass
class WantsNumpy2DArray:
    data: Numpy2DArray = field(compare=False, repr=False)

    @beartype
    def foo_fun(self, s: LengthyString):
        print(s)


@dataclass
class WantsALenghlyString:
    lenghtly_string: LengthyString


@pytest.mark.skipif(not numpy_is_installed, reason="numpy is not installed")
def test_WantsALenghlyString():

    _ = WantsNumpy2DArray(data=np.zeros((6, 3)))

    assert bear_does_roar(lambda: WantsNumpy2DArray(data=np.zeros((6, 3))))


def test_WantsALenghlyString():
    valid_string = "x" * 5
    invalid_string = "x" * 3

    _ = WantsALenghlyString(lenghtly_string=valid_string)

    assert bear_does_roar(lambda: WantsALenghlyString(lenghtly_string=invalid_string))


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


@pytest.mark.skipif(not torch_is_installed, reason="torch is not installed")
def test_torch_tensors():
    dummy_fun(torch.zeros((1, 2, 3)))
    error = None
    try:
        dummy_fun(torch.zeros((1, 2)))
    except BeartypeCallHintPepParamException as e:
        error = e

    assert error is not None and isinstance(error, BeartypeCallHintPepParamException)


@beartype
def dummy_fun_TorchTensorFirstDimAsTwo(x: TorchTensorFirstDimAsTwo):
    pass


@pytest.mark.skipif(not torch_is_installed, reason="torch is not installed")
def test_torch_tensors_TorchTensorFirstDimAsTwo():
    dummy_fun_TorchTensorFirstDimAsTwo(torch.zeros((2, 3)))
    try:
        dummy_fun_TorchTensorFirstDimAsTwo(torch.zeros((1, 2)))
    except BeartypeCallHintPepParamException as e:
        pass

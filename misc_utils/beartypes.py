import dataclasses
from typing import Annotated, TypeVar, Callable, Any, Type

import beartype
from beartype.roar import BeartypeCallException
from beartype.vale import IsAttr, IsEqual, Is

# -------------------------------------------------------------------------------------
# ----              NUMPY TYPES
# -------------------------------------------------------------------------------------

try:
    # TODO: looks somewhat ugly!
    from numpy import floating, int16, number, int32, float32
    from numpy.typing import NDArray

    NumpyArray = NDArray[number]
    NumpyFloat2DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[2]]]
    # brackets around multi-line conjunction, see:  https://github.com/beartype/beartype#validator-syntax
    firstdim_nonempty = lambda x: x.shape[0] > 0
    seconddim_nonempty = lambda x: x.shape[1] > 0

    NeNumpyFloat2DArray = Annotated[
        NDArray[floating],
        (IsAttr["ndim", IsEqual[2]] & Is[firstdim_nonempty] & Is[seconddim_nonempty]),
    ]
    # "Delimiting two or or more validators with commas at the top level ... is an alternate syntax for and-ing those validators with the & operator", see: https://github.com/beartype/beartype#validator-syntax
    NeNumpyFloat1DArray = Annotated[
        NDArray[floating], IsAttr["ndim", IsEqual[1]], Is[firstdim_nonempty]
    ]
    NumpyFloat1DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[1]]]
    # TODO: rename to NumpyFloatDim1, NumpyFloat32Dim1, etc
    NumpyFloat1D = NeNumpyFloat1DArray  # omitting the Ne-prefix cause 95% of situations I need non-empty array!
    NumpyFloat32_1D = Annotated[
        NDArray[float32], IsAttr["ndim", IsEqual[1]], Is[firstdim_nonempty]
    ]

    NumpyFloat2D = NeNumpyFloat2DArray
    NumpyInt16_1D = Annotated[
        NDArray[int16], (IsAttr["ndim", IsEqual[1]] & Is[firstdim_nonempty])
    ]
    Numpy1D = Annotated[
        NDArray[number], (IsAttr["ndim", IsEqual[1]] & Is[firstdim_nonempty])
    ]

    Numpy1DArray = Annotated[NDArray[number], IsAttr["ndim", IsEqual[1]]]
    NumpyInt16Dim1 = Annotated[NDArray[int16], IsAttr["ndim", IsEqual[1]]]
    NumpyInt32Dim1 = Annotated[NDArray[int32], IsAttr["ndim", IsEqual[1]]]

except ImportError:
    print("no numpy installed!")

T = TypeVar("T")

NeStr = Annotated[str, Is[lambda s: len(s) > 0]]
Dataclass = Annotated[Type, Is[lambda o: dataclasses.is_dataclass(o)]]
GenericDataclass = Annotated[T, Is[lambda o: dataclasses.is_dataclass(o)]]
# TODO: Annotated[object,...] is NOT working!
# StrOrBytesInstance = Annotated[object, IsInstance[str]]

T2 = TypeVar("T2")

NeList = Annotated[list[T], Is[lambda lst: len(lst) > 0]]
NeDict = Annotated[dict[T, T2], Is[lambda d: len(d.keys()) > 0]]
# NotNone = Annotated[Any, Is[lambda x:x is None]] # TODO: not working!

# -------------------------------------------------------------------------------------
# ----              TORCH TYPES
# -------------------------------------------------------------------------------------

try:
    import torch
    from torch import (
        float as torch_float,
        int as torch_int,
        tensor,
    )

    TorchTensor3D = Annotated[torch.Tensor, IsAttr["ndim", IsEqual[3]]]
    TorchTensor2D = Annotated[torch.Tensor, IsAttr["ndim", IsEqual[2]]]
    TorchTensor1D = Annotated[torch.Tensor, IsAttr["ndim", IsEqual[1]]]

    # https://github.com/beartype/beartype/issues/98
    # PEP-compliant type hint matching only a floating-point PyTorch tensor.
    TorchTensorFloat = Annotated[tensor, Is[lambda tens: tens.type() is torch_float]]

    TorchTensorFloat2D = Annotated[
        tensor, IsAttr["ndim", IsEqual[2]] & Is[lambda tens: tens.type() is torch_float]
    ]

    # PEP-compliant type hint matching only an integral PyTorch tensor.
    TorchTensorInt = Annotated[tensor, Is[lambda tens: tens.type() is torch_int]]

    # where is this from ?
    TorchTensorFirstDimAsTwo = Annotated[
        torch.Tensor, IsAttr["shape", Is[lambda shape: shape[0] == 2]]
    ]

except Exception as e:
    print(f"no torch!")


def bearify(obj, annotation):
    import beartype

    @beartype.beartype
    def check(o) -> annotation:
        return o

    return check(obj)


def bear_does_roar(roar_trigger_fun: Callable):
    did_roar = False
    try:
        roar_trigger_fun()
    except BeartypeCallException as e:
        did_roar = True
    return did_roar


@dataclasses.dataclass
class BearBully:
    ne_string: NeStr


from beartype.typing import TYPE_CHECKING

assert not TYPE_CHECKING, f"TYPE_CHECKING disables beartype"
assert __debug__, f"running python in optimized mode (-O) disables beartype"
# TODO: strange it ignores/overwrites? my -O !!
assert bear_does_roar(lambda: BearBully(""))

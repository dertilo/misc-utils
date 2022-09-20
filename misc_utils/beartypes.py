import dataclasses
from typing import Annotated, TypeVar, Callable

import beartype
from beartype.roar import BeartypeCallException
from beartype.vale import IsAttr, IsEqual, Is

# -------------------------------------------------------------------------------------
# ----              NUMPY TYPES
# -------------------------------------------------------------------------------------

try:
    # TODO: looks somewhat ugly!
    from numpy import floating, int16, number, int32
    from numpy.typing import NDArray

    NumpyArray = NDArray[number]
    NumpyFloat2DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[2]]]
    # brackets around multi-line conjunction, see:  https://github.com/beartype/beartype#validator-syntax
    NeNumpyFloat2DArray = Annotated[
        NDArray[floating],
        (
            IsAttr["ndim", IsEqual[2]]
            & Is[lambda x: x.shape[0] > 0]
            & Is[lambda x: x.shape[1] > 0]
        ),
    ]
    # "Delimiting two or or more validators with commas at the top level ... is an alternate syntax for and-ing those validators with the & operator", see: https://github.com/beartype/beartype#validator-syntax
    NeNumpyFloat1DArray = Annotated[
        NDArray[floating], IsAttr["ndim", IsEqual[1]], Is[lambda x: x.shape[0] > 0]
    ]
    NumpyFloat1DArray = Annotated[NDArray[floating], IsAttr["ndim", IsEqual[1]]]
    # TODO: rename to NumpyFloatDim1, NumpyFloat32Dim1, etc
    NumpyFloat1D = NeNumpyFloat1DArray  # omitting the Ne-prefix cause 95% of situations I need non-empty array!
    NumpyFloat2D = NeNumpyFloat2DArray

    Numpy1DArray = Annotated[NDArray[number], IsAttr["ndim", IsEqual[1]]]
    NumpyInt16Dim1 = Annotated[NDArray[int16], IsAttr["ndim", IsEqual[1]]]
    NumpyInt32Dim1 = Annotated[NDArray[int32], IsAttr["ndim", IsEqual[1]]]

except ImportError:
    print("no numpy installed!")

NeStr = Annotated[str, Is[lambda s: len(s) > 0]]
Dataclass = Annotated[object, Is[lambda o: dataclasses.is_dataclass(o)]]
# TODO: Annotated[object,...] is NOT working!
# StrOrBytesInstance = Annotated[object, IsInstance[str]]

T = TypeVar("T")
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

try:
    # see: https://github.com/beartype/beartype/issues/79
    from beartype import is_bearable
except ImportError:

    def is_bearable(obj, annotation):
        import beartype.roar
        import beartype

        @beartype.beartype
        def check(o) -> annotation:
            return o

        try:
            check(obj)
            reason = None
        except beartype.roar.BeartypeCallHintPepReturnException as e:
            reason = e.args[0].split("return ")[-1]
        if reason:
            raise Exception(reason)
        return True


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


assert bear_does_roar(lambda: BearBully(""))

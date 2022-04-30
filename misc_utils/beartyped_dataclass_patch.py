import dataclasses
import os
import pathlib

import sys


from beartype.roar import BeartypeDecorHintPep585DeprecationWarning
from warnings import filterwarnings

BEARTYPED_DATACLASS_PREFIXES: list[str] = []

filterwarnings("ignore", category=BeartypeDecorHintPep585DeprecationWarning)
from beartype import beartype


def beartype_all_dataclasses_of_this_files_parent(file: str):
    module_dir = str(pathlib.Path(file).parent.resolve())
    BEARTYPED_DATACLASS_PREFIXES.append(module_dir)


def beartype_all_dataclasses_in_this_dir_prefix(prefix: str):
    BEARTYPED_DATACLASS_PREFIXES.append(prefix)


# TODO: is it really necessary to copypast the entire dataclass method here?


def beartyped_dataclass(
    cls=None,
    /,
    *,
    init=True,
    repr=True,
    eq=True,
    order=False,
    unsafe_hash=False,
    frozen=False,
):
    """
    based on: .../python3.9/dataclasses.py
    Returns the same class as was passed in, with dunder methods
    added based on the fields defined in the class.

    Examines PEP 526 __annotations__ to determine fields.

    If init is true, an __init__() method is added to the class. If
    repr is true, a __repr__() method is added. If order is true, rich
    comparison dunder methods are added. If unsafe_hash is true, a
    __hash__() method function is added. If frozen is true, fields may
    not be assigned to after instance creation.
    """

    def wrap(cls):
        data_cls = dataclasses._process_class(
            cls, init, repr, eq, order, unsafe_hash, frozen
        )
        __file__ = sys.modules[cls.__module__].__file__
        is_one_of_my_own = any(
            [__file__.startswith(s) for s in BEARTYPED_DATACLASS_PREFIXES]
        )
        if is_one_of_my_own:
            data_cls.__init__ = beartype(data_cls.__init__)  # type: ignore[assignment]
        return data_cls
        # return beartyped_init(data_cls) # here is the patch!

    # See if we're being called as @dataclass or @dataclass().
    if cls is None:
        # We're called with parens.
        return wrap

    # We're called as @dataclass without parens.
    return wrap(cls)


dataclasses.dataclass = beartyped_dataclass

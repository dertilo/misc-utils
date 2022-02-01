import dataclasses
from typing import Any

from beartype import beartype
from dash import Dash

from misc_utils.dataclass_utils import (
    encode_dataclass,
    decode_dataclass,
)
from misc_utils.utils import just_try


class DashDataclasses(Dash):
    def callback_dataclassed(self, *_args, **_kwargs):
        """
        wraps the dash-decorator and putting another decorator in front of it
        1. serialize_deserialize_dataclasses_wrapper
        2. app.callback, super().callback here
        :return:
        """

        def some_decorator(fun):
            def serialize_deserialize_dataclasses_wrapper(*args, **kwargs):
                bearified_fun = beartype(fun)
                o = bearified_fun(
                    *[just_try_deserialize_or_fallback(x) for x in args],
                    **{
                        k: just_try_deserialize_or_fallback(x)
                        for k, x in kwargs.items()
                    },
                )
                if isinstance(o, (list, tuple)):
                    o = [maybe_serialize(dc) for dc in o]
                else:
                    o = maybe_serialize(o)
                return o

            return self.callback(*_args, **_kwargs)(
                serialize_deserialize_dataclasses_wrapper
            )

        return some_decorator


def maybe_serialize(dc):
    if dataclasses.is_dataclass(dc) or (
        isinstance(dc, list) and all((dataclasses.is_dataclass(x) for x in dc))
    ):
        return encode_dataclass(dc)
    else:
        return dc


def is_decodable(d):
    return isinstance(d, dict) and "_target_" in d.keys() and "_id_" in d.keys()


def just_try_deserialize_or_fallback(x: Any):
    if is_decodable(x) or (isinstance(x, list) and all((is_decodable(d) for d in x))):
        return just_try(lambda: decode_dataclass(x), default=x)
    else:
        return x

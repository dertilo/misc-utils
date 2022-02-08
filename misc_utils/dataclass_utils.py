import copy
import dataclasses
import importlib
import inspect
import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from hashlib import sha1
from typing import Any, Dict, TypeVar, Union, Optional

import omegaconf
from beartype import beartype
from omegaconf import OmegaConf, DictConfig

from misc_utils.base64_utils import Base64Decoder
from misc_utils.beartypes import Dataclass, NeStr
from misc_utils.utils import Singleton, just_try

T = TypeVar("T")


def persistable_state_field(default=dataclasses.MISSING):
    """
    TODO: can be misleading, cause the field is only persisted for objects that inherit from CachedData
    """
    return dataclasses.field(default=default, init=False, repr=True)


def volatile_state_field(default=dataclasses.MISSING):
    return dataclasses.field(default=default, init=False, repr=False)


def shallow_dataclass_from_dict(cls, dct: dict):
    """
    NO decoding of nested dicts to nested dataclasses here!!
    is used as a "factory" in instantiate_via_importlib
    dict can contain dataclasses or whatever objects!
    """
    kwargs = {
        f.name: dct[f.name]
        for f in dataclasses.fields(cls)
        if (f.init and f.name in dct.keys())
    }
    if (
        len(kwargs.keys()) == 1
    ):  # what a hack! enable inheriting from str, which does not allow keyword-arguments
        obj = cls(next(iter(kwargs.values())))
    else:
        obj = cls(**kwargs)
    set_noninit_fields(cls, dct, obj)
    return obj


def set_noninit_fields(cls, dct: dict, obj):
    state_fields = (
        f for f in dataclasses.fields(cls) if (not f.init and f.name in dct)
    )
    for f in state_fields:
        setattr(obj, f.name, dct[f.name])


def instantiate_via_importlib(d: dict[str, Any], class_key: str = "_cls_"):
    # cause hydra cannot instantiate recursively in lists
    fullpath = d.pop(class_key)
    *module_path, class_name = fullpath.split(".")
    clazz = getattr(importlib.import_module(".".join(module_path)), class_name)
    if hasattr(clazz, "create"):
        return clazz.create(**d)
    elif hasattr(clazz, dataclasses._FIELDS):
        return shallow_dataclass_from_dict(clazz, d)
    else:
        return clazz(**d)


IDKEY = "_id_"
SPECIAL_KEYS = [IDKEY, "_target_", "_cls_"]
CLASS_REF_NO_INSTANTIATE = "_python_dataclass_"  # use this to prevent instantiate_via_importlib, if one wants class-reference for documentation purposes only
UNSERIALIZABLE = "<UNSERIALIZABLE>"


def hash_dataclass(dc: Dataclass):
    skip_keys = [IDKEY, "cache_base", "cache_dir", "base_path"]
    s = serialize_dataclass(dc, skip_keys=skip_keys)
    hashed_self = sha1(s.encode("utf-8")).hexdigest()
    return hashed_self


class MyCustomEncoder(json.JSONEncoder):
    """
    # see: https://stackoverflow.com/questions/64777931/what-is-the-recommended-way-to-include-properties-in-dataclasses-in-asdict-or-se
    """

    class_reference_key = "_target_"
    skip_undefined = True
    is_special = re.compile(
        r"^__[^\d\W]\w*__\Z", re.UNICODE
    )  # Dunder name. -> field from stackoverflow
    skip_keys: Optional[list[str]] = None

    def default(self, o):
        return self._asdict(o)

    def _asdict(self, obj, *, dict_factory=dict):
        # if not (dataclasses.is_dataclass(obj) or isinstance(obj, DictConfig)):
        #     raise TypeError(f"_asdict() not working on {str(obj)} of type: {type(obj)}")
        # why not also allow non-dataclass as input?
        return self._asdict_inner(obj, dict_factory)

    def maybe_append(self, r, k, v):
        skip_this_one = self.skip_keys and k in self.skip_keys
        if not skip_this_one:
            r.append((k, v))

    def _asdict_inner(self, obj, dict_factory):
        if dataclasses.is_dataclass(obj):
            result: list[tuple[str, Any]] = []
            module = obj.__class__.__module__
            if module == "__main__":
                prefixes = os.environ["PYTHONPATH"].split(":")
                file_path = __file__.replace(".py", "")
                for p in prefixes:
                    file_path = file_path.replace(p, "")

                module = file_path.strip("/").replace("/", ".")
            _target_ = f"{module}.{obj.__class__.__name__}"
            self.maybe_append(result, self.class_reference_key, _target_)
            self.maybe_append(result, IDKEY, id(obj))
            # Get values of its fields (recursively).
            # TODO: remove f.name.startswith("_")!!
            feelds = (
                f
                for f in dataclasses.fields(obj)
                if not f.name.startswith("_") and f.repr and hasattr(obj, f.name)
            )
            # WTF: if a field has value of dataclasses.MISSING than hasattr(obj,f.name) is True!
            for f in feelds:
                value = self._asdict_inner(getattr(obj, f.name), dict_factory)
                if value is not UNDEFINED or not self.skip_undefined:
                    self.maybe_append(result, f.name, value)

            # Add values of non-special attributes which are properties.
            # tilo: fingeres crossed this does not break anything!
            is_special = self.is_special.match  # Local var to speed access.
            properties_to_be_serialized = (
                obj.__serializable_properties__
                if hasattr(obj, "__serializable_properties__")
                else []
            )
            for name, attr in vars(type(obj)).items():
                if (
                    not is_special(name)
                    and isinstance(attr, property)
                    and name in properties_to_be_serialized
                ):
                    result.append((name, attr.__get__(obj)))  # Get property's value.
            return dict_factory(result)
        elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
            # TODO: this could return any class that implements _fields method! WTF! not what I want!
            return type(obj)(*(self._asdict_inner(v, dict_factory) for v in obj))
        elif isinstance(obj, (list, tuple, omegaconf.listconfig.ListConfig)):
            if isinstance(obj, omegaconf.listconfig.ListConfig):
                obj = list(obj)
            return type(obj)(self._asdict_inner(v, dict_factory) for v in obj)
        elif isinstance(obj, (dict, omegaconf.dictconfig.DictConfig)):
            if isinstance(obj, omegaconf.dictconfig.DictConfig):
                obj = dict(obj)

            return type(obj)(
                (
                    self._asdict_inner(k, dict_factory),
                    self._asdict_inner(v, dict_factory),
                )
                for k, v in obj.items()
            )
        elif callable(obj):
            return inspect.getsource(
                obj
            )  # TODO: this is hacky! and not working for deserialization!
        else:
            obj = just_try(
                lambda: copy.deepcopy(obj),
                default=f"{UNSERIALIZABLE}{id(obj)=}{UNSERIALIZABLE}",
            )
            obj = obj._to_dict(self.skip_keys) if hasattr(obj, "_to_dict") else obj
            return obj


class MyDecoder(json.JSONDecoder):
    """
    # see https://stackoverflow.com/questions/48991911/how-to-write-a-custom-json-decoder-for-a-complex-object
    """

    def __init__(self, *args, **kwargs):

        object_registry: dict[str, Any] = {}

        def object_hook(dct: Dict):
            for k in ["_target_", "_cls_"]:
                if k in dct:
                    class_key = k
                    break
            else:
                class_key = None
            if class_key is not None and IDKEY in dct:
                # if IDKEY in dct:
                eid = dct.pop(IDKEY)
                # else:
                #     eid = None

                if eid in object_registry.keys():
                    o = object_registry[eid]
                else:
                    o = instantiate_via_importlib(dct, class_key)
                    # if eid is not None:
                    object_registry[eid] = o

            else:
                o = dct
            return o

        json.JSONDecoder.__init__(self, object_hook=object_hook, *args, **kwargs)


def to_dict(o) -> Dict:
    # def serialize_field(k, v):
    #     # not_none=v is not None
    #     if hasattr(o, k):
    #         is_valid = not (k.startswith("_") and ingore_private_fields)
    #     else:
    #         is_valid = False
    #     return is_valid
    #
    # return dataclasses.asdict(
    #     o, dict_factory=lambda x: {k: v for k, v in x if serialize_field(k, v)}
    # )
    return encode_dataclass(o, skip_keys=SPECIAL_KEYS)


@beartype
def decode_dataclass(o: Union[dict, list, tuple]) -> Dataclass:
    # TODO: there must be a better way than, json.dumps twice!
    if not isinstance(o, str):
        o = json.dumps(o)
    o = json.loads(o, cls=Base64Decoder)
    o = json.loads(json.dumps(o), cls=MyDecoder)
    return o


@beartype
def deserialize_dataclass(o: NeStr) -> Dataclass:
    o = json.loads(o, cls=Base64Decoder)
    o = json.loads(json.dumps(o), cls=MyDecoder)
    return o


def serialize_dataclass(
    d: Any,
    class_reference_key="_target_",
    skip_undefined=True,
    skip_keys: Optional[list[str]] = None,
) -> str:
    return json.dumps(
        encode_dataclass(d, class_reference_key, skip_undefined, skip_keys)
    )


@beartype
def encode_dataclass(
    d: Dataclass,
    class_reference_key="_target_",
    skip_undefined=True,
    skip_keys: Optional[list[str]] = None,
) -> Union[dict, list, tuple, set]:
    """
    encode in the sense that the dictionary representation can be decoded to the nested dataclasses object again
    """
    MyCustomEncoder.class_reference_key = class_reference_key
    MyCustomEncoder.skip_undefined = skip_undefined
    MyCustomEncoder.skip_keys = skip_keys
    return MyCustomEncoder().default(d)


@beartype
def deserialize_from_yaml(
    yaml_file: str,
) -> Dataclass:
    """
    TODO: do I really have to serialize the DictConfig before deserializing?
    """
    cfg = OmegaConf.load(yaml_file)
    return deserialize_dataclass(serialize_dataclass(cfg))


@dataclass
class _UNDEFINED(metaclass=Singleton):
    pass


UNDEFINED = _UNDEFINED()
FILLME = Union[T, _UNDEFINED]  # TODO: destroy IDE argument hint

# @dataclass
# class FILLME(Union[T,_UNDEFINED]): # TODO: ???
#     pass


def all_undefined_must_be_filled(obj):
    for f in dataclasses.fields(obj):
        if not f.name.startswith("_") and f.init:
            assert not isinstance(
                getattr(obj, f.name), _UNDEFINED
            ), f"{f.name=} of {obj.name if hasattr(obj,'name') else obj} ({type(obj)})is UNDEFINED!"


@dataclass
class FillUndefined:
    def __post_init__(self):
        all_undefined_must_be_filled(self)


@beartype
def dataclass_to_yaml(
    o: Dataclass, skip_undefined=False, skip_keys: Optional[list[str]] = None
) -> str:
    sd = serialize_dataclass(o, skip_undefined=skip_undefined, skip_keys=skip_keys)
    cfg = OmegaConf.create(sd)
    yaml = OmegaConf.to_yaml(cfg)
    # print(yaml)
    # deser_obj = deserialize_dataclass(sd)
    # deserialize_dataclass can be different due to FILLED_AT_RUNTIME values that are filtered out
    # assert str(deser_obj) == str(o),deser_obj
    return yaml

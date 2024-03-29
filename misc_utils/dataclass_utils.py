import copy
import dataclasses
import importlib
import inspect
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Dict, TypeVar, Union, Optional
from beartype import beartype

from data_io.readwrite_files import write_file, write_json
from misc_utils.base64_utils import Base64Decoder
from misc_utils.beartypes import Dataclass, NeStr
from misc_utils.utils import Singleton, just_try

got_omeagaconf = False


class ImpossibleType:
    def __new__(cls):
        raise NotImplementedError


try:
    import omegaconf
    from omegaconf import OmegaConf

    got_omeagaconf = True
    OmegaConfList = omegaconf.listconfig.ListConfig
    OmegaConfDict = omegaconf.dictconfig.DictConfig
except:
    OmegaConfList = ImpossibleType
    OmegaConfDict = ImpossibleType


T = TypeVar("T")

# TARGET_CLASS_MAPPING=defaultdict(lambda : dict)

# TODO: nice idea but IDE says NO!
# def persistable_state_field(default=dataclasses.MISSING):
#     """
#     TODO: can be misleading, cause the field is only persisted for objects that inherit from CachedData
#     """
#     return dataclasses.field(default=default, init=False, repr=True)
#
#
# def volatile_state_field(default=dataclasses.MISSING):
#     return dataclasses.field(default=default, init=False, repr=False)


def remove_if_exists(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)


def shallow_dataclass_from_dict(clazz, dct: dict):
    """
    NO decoding of nested dicts to nested dataclasses here!!
    is used as a "factory" in instantiate_via_importlib
    dict can contain dataclasses or whatever objects!
    """
    kwargs = {
        f.name: dct[f.name]
        for f in dataclasses.fields(clazz)
        if (f.init and f.name in dct.keys())
    }
    # print(f"{dct=}")
    # print(f"{kwargs=}")
    obj = just_try(
        lambda: clazz(**kwargs),
        reraise=True,
        verbose=True,
        print_stacktrace=False,
        fail_print_message_supplier=lambda: f"fail class: {clazz.__name__=}",
    )
    set_noninit_fields(clazz, dct, obj)
    return obj


def set_noninit_fields(cls, dct: dict, obj):
    state_fields = (
        f for f in dataclasses.fields(cls) if (not f.init and f.name in dct)
    )
    for f in state_fields:
        setattr(obj, f.name, dct[f.name])


def instantiate_via_importlib(
    d: dict[str, Any],
    fullpath: str,
):  # class_key: str = "_cls_"):
    # cause hydra cannot instantiate recursively in lists
    *module_path, class_name = fullpath.split(".")
    module_reference = ".".join(module_path)
    # if (
    #     class_name == "PrefixSuffix"
    #     and module_reference == "misc_utils.dataclass_utils"
    # ):
    #     # TODO: hack for backward compatibility
    #     module_reference = "misc_utils.prefix_suffix"

    clazz = getattr(importlib.import_module(module_reference), class_name)
    if hasattr(clazz, "create"):
        return clazz.create(**d)
    elif hasattr(clazz, dataclasses._FIELDS):
        return shallow_dataclass_from_dict(clazz, d)
    else:
        return clazz(**d)


IDKEY = "_id_"
CLASS_REF_KEY = "_target_"
SPECIAL_KEYS = [IDKEY, CLASS_REF_KEY, "_cls_", "_was_built"]
CLASS_REF_NO_INSTANTIATE = "_python_dataclass_"  # use this to prevent instantiate_via_importlib, if one wants class-reference for documentation purposes only
UNSERIALIZABLE = "<UNSERIALIZABLE>"


def hash_dataclass(
    dc: Dataclass,
    skip_keys=[
        IDKEY,
        "cache_base",
        "cache_dir",
        "use_hash_suffix",
        "overwrite_cache",
    ],
) -> str:
    """
    under, dunder and __exclude_from_hash__ fields are not hashed!
    """
    skip_keys += [f.name for f in dataclasses.fields(dc) if is_dunder(f.name)]
    s = serialize_dataclass(dc, skip_keys=skip_keys, encode_for_hash=True)
    hashed_self = sha1(s.encode("utf-8")).hexdigest()
    return hashed_self


def hash_dataclass_dict(
    dc: dict,
    skip_keys=[
        IDKEY,
        "cache_base",
        "cache_dir",
        "use_hash_suffix",
        "overwrite_cache",
    ],
) -> str:
    """
    under, dunder and __exclude_from_hash__ fields are not hashed!
    """
    s = serialize_dataclass(dc, skip_keys=skip_keys, encode_for_hash=True)
    hashed_self = sha1(s.encode("utf-8")).hexdigest()
    return hashed_self


def is_dunder(s):
    return s.startswith("__") and s.endswith("__")


salt = (
    uuid.uuid1()
)  # to prevent clashes when "merging" graphs built in different python processes, cause then objects-ids could clash!


def fix_module_if_class_in_same_file_as_main(obj):
    assert (
        "PYTHONPATH" in os.environ
    ), f"do export PYTHONPATH=${{PWD}} if you run script from __main__"
    prefixes = os.environ["PYTHONPATH"].split(":")
    prefixes = [  # which are not prefix of another one
        p
        for p in prefixes
        if not any((pp.startswith(p) and p != pp for pp in prefixes))
    ]
    file_path = os.path.abspath(inspect.getsourcefile(obj.__class__))
    file_path = file_path.replace(".py", "")
    assert any(
        (file_path.startswith(p) for p in prefixes)
    ), f"{file_path=}, {prefixes=}, set PYTHONPATH if you run script from __main__"
    for p in prefixes:
        file_path = file_path.replace(p, "")
    module = file_path.strip("/").replace("/", ".")
    return module


class MyCustomEncoder(json.JSONEncoder):
    """
    # see: https://stackoverflow.com/questions/64777931/what-is-the-recommended-way-to-include-properties-in-dataclasses-in-asdict-or-se
    """

    class_reference_key = CLASS_REF_KEY
    skip_undefined = True
    encode_for_hash = False
    sparse: bool = False
    is_special = re.compile(
        r"^__[^\d\W]\w*__\Z", re.UNICODE
    )  # Dunder name. -> field from stackoverflow
    skip_keys: Optional[list[str]] = None

    def sparse_dict(self, key_value: list[tuple[str, Any]]):
        """
        TODO
        """
        dct = dict(key_value)
        if IDKEY in dct.keys():
            obj_id = dct[IDKEY]
            if obj_id in self._object2node_id:
                node_id = self._object2node_id[obj_id]
            else:
                node_id = f"{len(self._id2node_.keys())}"
                self._object2node_id[obj_id] = node_id
                self._id2node_[node_id] = dct

        return {"_node_id_": node_id}

    def default(self, o):
        if self.sparse:
            raise NotImplementedError("if you want it, fix it!")
            self._object2node_id = dict()
            self._id2node_ = dict()
        dct = self._asdict(o, dict_factory=self.sparse_dict if self.sparse else dict)
        if self.sparse:
            dct["_id2node_"] = self._id2node_
        return dct

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
        """
        _UNDEFINED is a Dataclass! so it gets serialized as such!
        """
        if dataclasses.is_dataclass(obj):
            result: list[tuple[str, Any]] = []
            module = obj.__class__.__module__
            if module == "__main__":
                module = fix_module_if_class_in_same_file_as_main(obj)
            clazz_name = obj.__class__.__name__
            _target_ = f"{module}.{clazz_name}"
            self.maybe_append(result, self.class_reference_key, _target_)
            clazz_name_hash = sha1(clazz_name.encode("utf-8")).hexdigest()
            hash = f"{salt}-{id(obj)}-{clazz_name_hash}"

            self.maybe_append(result, IDKEY, f"{hash}")

            def exclude_for_hash(o, f_name: str) -> bool:
                if self.encode_for_hash and hasattr(o, "__exclude_from_hash__"):
                    return f_name in o.__exclude_from_hash__
                else:
                    return False

            feelds = (
                f
                for f in dataclasses.fields(obj)
                if f.repr
                and hasattr(obj, f.name)
                and not f.name.startswith("_")
                and not exclude_for_hash(obj, f.name)
            )
            # WTF: if a field has value of dataclasses.MISSING than hasattr(obj,f.name) is True!
            for f in feelds:
                value = getattr(obj, f.name)  # can be UNDEFINED
                if value is not UNDEFINED or not self.skip_undefined:
                    value = self._asdict_inner(value, dict_factory)
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
        elif isinstance(obj, (list, tuple, OmegaConfList)):
            if isinstance(obj, OmegaConfList):
                obj = list(obj)
            return type(obj)(self._asdict_inner(v, dict_factory) for v in obj)
        elif isinstance(obj, (dict, OmegaConfDict)):
            if isinstance(obj, OmegaConfDict):
                obj = dict(obj)

            return type(obj)(
                (
                    self._asdict_inner(k, dict_factory),
                    self._asdict_inner(v, dict_factory),
                )
                for k, v in obj.items()
                if self.skip_keys is None or k not in self.skip_keys
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
            for k in [CLASS_REF_KEY, "_cls_"]:
                if k in dct:
                    class_key = k
                    break
            else:
                class_key = None
            if class_key is not None and IDKEY in dct:
                assert class_key in dct, f"{class_key=} not in dct"
                # if IDKEY in dct:
                # TODO: maybe one should not rely on object id here, cause in different python-processes they will obviously have different ids
                # what about hash_dataclass(o) ? well actually we do not yet have dataclass yet, but dict
                eid = dct.pop(IDKEY)
                # else:
                #     eid = None

                if eid in object_registry.keys():
                    o = object_registry[eid]
                    just_for_backward_compatibility = [
                        "use_hash_suffix",
                        "overwrite_cache",
                        "limit",
                    ]
                    dct_from_obj_registry = serialize_dataclass(
                        o, skip_keys=[IDKEY] + just_for_backward_compatibility
                    )
                    dct_with_same_id = serialize_dataclass(
                        dct, skip_keys=[IDKEY] + just_for_backward_compatibility
                    )
                    if (
                        dct[CLASS_REF_KEY] != "misc_utils.prefix_suffix.PrefixSuffix"
                    ):  # no isinstance due to circular dependency
                        # PrefixSuffix does change prefix depending on BASE_PATHES
                        if dct_from_obj_registry != dct_with_same_id:
                            write_file(
                                "dct_from_obj_registry.json", dct_from_obj_registry
                            )
                            write_file("dct_with_same_id.json", dct_with_same_id)
                            assert (
                                False
                            ), f"{eid} is clashing, do: icdiff <(cat dct_from_obj_registry.json | jq . ) <(cat dct_with_same_id.json | jq . ) | less -r"

                else:
                    fullpath = dct.pop(class_key)
                    # TODO: here some try except with lookup in TARGET_CLASS_MAPPING
                    o = just_try(
                        lambda: instantiate_via_importlib(dct, fullpath),
                        reraise=True,
                        fail_print_message_supplier=lambda: f"could not instantiate: {fullpath}",
                        verbose=True,
                    )
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
def _json_loads_decode_dataclass(s: str):
    def fail_fun():
        write_file("failed_to_decode.json", s)
        f"failed to decode: {s[:1000]=}"

    # TODO: just_try just for debugging
    return just_try(
        lambda: json.loads(s, cls=MyDecoder),
        reraise=True,
        verbose=True,
        fail_print_message_supplier=fail_fun,
    )


@beartype
def decode_dataclass(o: Union[dict, list, tuple]) -> Dataclass:
    # TODO: there must be a better way than, json.dumps twice!
    if not isinstance(o, str):
        o = json.dumps(o)
    o = json.loads(o, cls=Base64Decoder)
    o = _json_loads_decode_dataclass(json.dumps(o))
    return o


@beartype
def deserialize_dataclass(o: NeStr) -> Dataclass:
    o = json.loads(o, cls=Base64Decoder)
    o = _json_loads_decode_dataclass(json.dumps(o))
    return o


def serialize_dataclass(
    d: Union[str, Dataclass],  # TODO: WTF why str?
    class_reference_key=CLASS_REF_KEY,
    skip_undefined=True,
    skip_keys: Optional[list[str]] = None,
    encode_for_hash: bool = False,
    sparse: bool = False,
) -> str:
    return json.dumps(
        encode_dataclass(
            d,
            class_reference_key,
            skip_undefined,
            skip_keys,
            sparse=sparse,
            encode_for_hash=encode_for_hash,
        ),
        ensure_ascii=False,
    )


@beartype
def encode_dataclass(
    d: Union[str, Dataclass],  # TODO: WTF why str?
    class_reference_key=CLASS_REF_KEY,
    skip_undefined: bool = True,
    skip_keys: Optional[list[str]] = None,
    sparse: bool = False,
    encode_for_hash: bool = False,
) -> Union[dict, list, tuple, set]:
    """
    encode in the sense that the dictionary representation can be decoded to the nested dataclasses object again
    """
    MyCustomEncoder.class_reference_key = class_reference_key
    MyCustomEncoder.skip_undefined = skip_undefined
    MyCustomEncoder.skip_keys = skip_keys
    MyCustomEncoder.sparse = sparse
    MyCustomEncoder.encode_for_hash = encode_for_hash
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
    """
    I guess this is a dataclass to enable serialization?
    """

    pass


UNDEFINED = _UNDEFINED()
FILLME = Union[T, _UNDEFINED]  # TODO: destroys IDE argument hint

# @dataclass
# class FILLME(Union[T,_UNDEFINED]): # TODO: ???
#     pass


def all_undefined_must_be_filled(obj, extra_field_names: Optional[list[str]] = None):
    field_names = [
        f.name for f in dataclasses.fields(obj) if not f.name.startswith("_") and f.init
    ]
    if (
        extra_field_names is not None
    ):  # property overwritten by field still not listed in dataclasses.fields!
        field_names += extra_field_names
    for f_name in field_names:
        if hasattr(obj, f_name) and getattr(obj, f_name) is UNDEFINED:
            raise AssertionError(
                f"{f_name=} of {obj.name if hasattr(obj,'name') else obj.__class__.__name__} ({type(obj)}) is UNDEFINED!"
            )


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

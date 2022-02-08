from typing import Any
from warnings import filterwarnings

from beartype.roar import BeartypeDecorHintPep585DeprecationWarning

filterwarnings("ignore", category=BeartypeDecorHintPep585DeprecationWarning)

import shutil
from dataclasses import field, dataclass

from data_io.readwrite_files import read_json
from misc_utils.cached_data import (
    CachedData,
    _CREATE_CACHE_DIR_IN_BASE_DIR,
    CREATE_CACHE_DIR_IN_BASE_DIR,
)
from misc_utils.dataclass_utils import (
    shallow_dataclass_from_dict,
    encode_dataclass,
)


@dataclass
class TestData(CachedData):
    test_field: str = "foo"
    cache_base: str = "bar"
    anything: Any = None
    _private_test_attr: str = field(default=None, init=False, repr=False)
    persistable_test_state_field: str = field(init=False, repr=True, default=None)
    persistable_non_init_state_field: str = field(default="nix", init=False, repr=True)
    volatile_test_state_field: str = field(init=False, repr=False, default=None)

    def _build_cache(self):
        self._private_test_attr = "private data"
        self.persistable_test_state_field = "this is serialized and deserialized"
        self.volatile_test_state_field = "volatile value is not serialized"
        self.persistable_non_init_state_field = "assigned during build"


@dataclass
class KnownClass:
    foo: str = "bar"


def cope_with_unknown_class_fields():
    test_datum = TestData(test_field="bar", anything=KnownClass())
    d = encode_dataclass(test_datum)


def loading_cached_dataclass_from_dict():
    # TODO: this is very ugly!
    test_datum = None
    try:
        test_datum = TestData(test_field="bar")
        test_datum.build()
        # print(f"{test_datum.cache_dir=}")
        assert test_datum._private_test_attr == "private data"
        loaded_datum = shallow_dataclass_from_dict(
            TestData, read_json(test_datum.dataclass_json)
        )
        assert loaded_datum.test_field == "bar"
        assert (
            loaded_datum.persistable_test_state_field
            == "this is serialized and deserialized"
        )
        assert (
            loaded_datum.volatile_test_state_field != "volatile value is not serialized"
        )
        assert loaded_datum._private_test_attr is None, loaded_datum._private_test_attr
        assert loaded_datum.persistable_non_init_state_field == "assigned during build"

    finally:
        if test_datum:
            shutil.rmtree(test_datum.cache_dir)


def test_CreateCacheDirInCacheBase():
    assert isinstance(CREATE_CACHE_DIR_IN_BASE_DIR, _CREATE_CACHE_DIR_IN_BASE_DIR)
    assert CREATE_CACHE_DIR_IN_BASE_DIR is _CREATE_CACHE_DIR_IN_BASE_DIR()
    assert _CREATE_CACHE_DIR_IN_BASE_DIR() is _CREATE_CACHE_DIR_IN_BASE_DIR()
    assert id(CREATE_CACHE_DIR_IN_BASE_DIR) == id(_CREATE_CACHE_DIR_IN_BASE_DIR())

import dataclasses
import multiprocessing
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Union

import sys
from beartype import beartype
from time import time, sleep

from data_io.readwrite_files import (
    write_json,
    read_file,
)
from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import (
    deserialize_dataclass,
    encode_dataclass,
    all_undefined_must_be_filled,
    hash_dataclass,
)
from misc_utils.utils import Singleton, claim_write_access


@dataclass
class _IGNORE_THIS_USE_CACHE_DIR(metaclass=Singleton):
    pass


IGNORE_THIS_USE_CACHE_DIR = _IGNORE_THIS_USE_CACHE_DIR()


@dataclass
class _CREATE_CACHE_DIR_IN_BASE_DIR(metaclass=Singleton):
    pass


CREATE_CACHE_DIR_IN_BASE_DIR = _CREATE_CACHE_DIR_IN_BASE_DIR()

DEFAULT_CACHE_BASES: dict[str, str] = {}


@dataclass
class CachedData(Buildable, ABC):
    cache_base: Union[_IGNORE_THIS_USE_CACHE_DIR, str] = IGNORE_THIS_USE_CACHE_DIR
    cache_dir: Union[_CREATE_CACHE_DIR_IN_BASE_DIR, str] = CREATE_CACHE_DIR_IN_BASE_DIR
    _json_file_name: ClassVar[int] = "dataclass.json"
    clean_on_fail: bool = dataclasses.field(default=True, repr=False)

    @property
    def dataclass_json(self):
        return self.prefix_cache_dir(self._json_file_name)

    def __post_init__(self):
        """
        overrides Buildable's __post_init__
        not checkin all_undefined_must_be_filled here!
        """
        got_cache_dir = self.cache_dir is not CREATE_CACHE_DIR_IN_BASE_DIR
        got_a_cache_base = self.cache_base is not IGNORE_THIS_USE_CACHE_DIR
        assert got_a_cache_base or got_cache_dir, f"{self.__class__}"
        if got_a_cache_base:
            os.makedirs(self.cache_base, exist_ok=True)
        if got_cache_dir and got_a_cache_base:
            assert self.cache_dir.startswith(self.cache_base)
        self.maybe_fix_cache_base()

    def maybe_fix_cache_base(self):
        """
        in case one copys cache-base to a new dir, one can replace the path suffix (cache_base) via environ variable CACHE_BASE
        """
        if "CACHE_BASE" in os.environ:
            if (
                self.cache_base is not IGNORE_THIS_USE_CACHE_DIR
                and self.cache_dir is not CREATE_CACHE_DIR_IN_BASE_DIR
                and not self.cache_dir.startswith(os.environ["CACHE_BASE"])
            ):
                assert self.cache_dir.startswith(self.cache_base)
                self.cache_dir = self.cache_dir.replace(
                    self.cache_base, os.environ["CACHE_BASE"]
                )
                assert os.path.isdir(
                    os.environ["CACHE_BASE"]
                ), f"{os.environ['CACHE_BASE']=} is not a directory!"
            else:
                pass
                """
                nodes like RglobRawCorpusFromDicts in the build-graph that are NOT build just exist in the dataclass-graph for documentation purposes
                """

    @property
    def _is_ready(self) -> bool:
        """
        looks somehow ugly, but necessary in order to prevent build of dependencies when actually already cached
        if is_ready is True does prevent complete build: not building cache not even loading caches (of nodes further up in the graph)!!
        """
        is_ready = super()._is_ready
        if not is_ready:
            is_ready = self._found_and_loaded_from_cache()

        return is_ready

    def _found_and_loaded_from_cache(self):

        if self._found_dataclass_json():
            self._load_cached()
            # print(
            #     f"LOADED cached: {self.name} ({self.__class__.__name__}) from {self.cache_dir}"
            # )
            successfully_loaded_cached = True
        else:
            successfully_loaded_cached = False

        return successfully_loaded_cached

    @property
    def name(self):
        return ""

    def prefix_cache_dir(self, path: str) -> str:
        self.maybe_fix_cache_base()
        assert isinstance(
            self.cache_dir, str
        ), f"{self} has invalid cache_dir: {self.cache_dir}"
        return f"{self.cache_dir}/{path}"

    @abstractmethod
    def _build_cache(self):
        # TODO: rename to "build_cache"
        raise NotImplementedError

    def _build_self(self):
        self.build_or_load()

    def _prepare(self, cache_dir: str) -> None:
        pass

    def __cache_creation_is_in_process(self):
        return os.path.isfile(f"{self.cache_dir}.lock")

    def _found_dataclass_json(self) -> bool:
        if self.cache_dir is CREATE_CACHE_DIR_IN_BASE_DIR:
            self.cache_dir = self.create_cache_dir_from_hashed_self()
        wait_message = f"{self.__class__.__name__}-{self.name}-{multiprocessing.current_process().name}-is waiting\n"
        while self.__cache_creation_is_in_process():
            print(wait_message)
            # sys.stdout.write(wait_message)
            # sys.stdout.flush()
            # wait_message = "."
            sleep(1)
        return os.path.isfile(self.dataclass_json)

    def _claimed_right_to_build_cache(self) -> bool:
        all_undefined_must_be_filled(self)
        if self.cache_dir is CREATE_CACHE_DIR_IN_BASE_DIR:
            self.cache_dir = self.create_cache_dir_from_hashed_self()

        should_build_cache = claim_write_access(self.cache_dir)
        if os.environ.get("NO_BUILD", "False").lower() != "false":
            assert (
                not should_build_cache
            ), f"env-var NO_BUILD is set but {self.__class__.__name__} did not find {self.dataclass_json=}"
        return should_build_cache

    def _load_cached(self) -> None:
        # not loading persistable_state_fields+complete datum here!
        # is everybodys own responsibility! -> why?
        # if I can persist it I am able to deserialize it!
        self._load_state_fields()

    @beartype
    def build_or_load(
        self,
        cache_base: Union[_IGNORE_THIS_USE_CACHE_DIR, str] = IGNORE_THIS_USE_CACHE_DIR,
    ) -> None:

        if cache_base is not IGNORE_THIS_USE_CACHE_DIR:
            self.cache_base = cache_base

        if self._claimed_right_to_build_cache():
            cadi = self.cache_dir
            shutil.rmtree(cadi, ignore_errors=True)
            os.makedirs(cadi)
            error = None
            try:
                start = time()
                sys.stdout.write(
                    f"building CACHE {self.name} ({self.__class__.__name__}) by {multiprocessing.current_process().name}"
                )
                self._build_cache()
                sys.stdout.write(
                    f"{self.name} took: {time()-start} secs; in cache-dir: {cadi} \n"
                )
                sys.stdout.flush()

                write_json(
                    self.dataclass_json,
                    encode_dataclass(self),
                )
                # sleep(1) # TODO:  WTF! sleep here seems to alleviate problem with multiprocessing
            except Exception as e:
                error = e
                if self.clean_on_fail:
                    shutil.rmtree(cadi, ignore_errors=True)
            finally:
                assert os.path.isfile(self.dataclass_json)
                os.remove(f"{self.cache_dir}.lock")
                os.remove(f"{self.cache_dir}.lock.lock")
                if error is not None:
                    raise error
        else:
            assert self._found_dataclass_json()

        start = time()
        self._load_cached()
        duration = time() - start
        if duration >= 1.0:
            print(
                f"LOADED cached: {self.name} ({self.__class__.__name__}) took: {duration} seconds from {self.cache_dir}"
            )

    def _load_state_fields(self):
        loaded_dc = deserialize_dataclass(read_file(self.dataclass_json))
        state_fields = list(
            f for f in dataclasses.fields(self) if not f.init and f.repr
        )
        for f in state_fields:
            setattr(self, f.name, getattr(loaded_dc, f.name))

    def create_cache_dir_from_hashed_self(self) -> str:
        all_undefined_must_be_filled(self)
        hashed_self = hash_dataclass(self)
        typed_self = type(self).__name__
        name = self.name.replace("/", "_")
        return f"{self.cache_base}/{typed_self}-{name}-{hashed_self}"

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
    remove_if_exists,
    serialize_dataclass,
)
from misc_utils.prefix_suffix import PrefixSuffix, BASE_PATHES
from misc_utils.utils import Singleton, just_try
from misc_utils.filelock_utils import claim_write_access


@dataclass
class _IGNORE_THIS_USE_CACHE_DIR(metaclass=Singleton):
    pass


IGNORE_THIS_USE_CACHE_DIR = _IGNORE_THIS_USE_CACHE_DIR()


@dataclass
class _CREATE_CACHE_DIR_IN_BASE_DIR(metaclass=Singleton):
    pass


CREATE_CACHE_DIR_IN_BASE_DIR = _CREATE_CACHE_DIR_IN_BASE_DIR()


def remove_make_dir(dirr):
    shutil.rmtree(dirr, ignore_errors=True)
    os.makedirs(
        dirr
    )  # TODO: WTF! this sometimes failes cause it does not get that directory was removed! some state is not flushed


export_black_list = [
    "LM_DATA",
    "RAW_DATA",
    "STABLEFORMAT_DATA",
    "ANNOTATION_DATA",
    "AUDIO_FILE_CORPORA" "PROCESSED_DATA",
    # "EVAL_DATASETS_CACHE", Vocab is cached here!
]  # TODO: WTF!! this is a hack! specific to a use-case, things like ANNOTATION_DATA should not be here


@dataclass
class CachedData(Buildable, ABC):
    # str for backward compatibility
    cache_base: Union[
        _IGNORE_THIS_USE_CACHE_DIR, PrefixSuffix
    ] = IGNORE_THIS_USE_CACHE_DIR
    cache_dir: Union[
        _CREATE_CACHE_DIR_IN_BASE_DIR, PrefixSuffix
    ] = CREATE_CACHE_DIR_IN_BASE_DIR
    use_hash_suffix: bool = dataclasses.field(init=True, repr=True, default=True)
    overwrite_cache: bool = dataclasses.field(init=True, repr=False, default=False)
    _json_file_name: ClassVar[int] = "dataclass.json"
    __exclude_from_hash__: ClassVar[list[str]] = []
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
            assert isinstance(
                self.cache_base, PrefixSuffix
            ), f"{self.cache_base=} is not instance of PrefixSuffix"
            os.makedirs(str(self.cache_base), exist_ok=True)

        if got_cache_dir and got_a_cache_base:
            assert isinstance(
                self.cache_dir, PrefixSuffix
            ), f"{self.cache_dir=} is not instance of PrefixSuffix"
            assert str(self.cache_dir).startswith(
                str(self.cache_base)
            ), f"{self.cache_dir=} does not startswith {self.cache_base=}"

    @property
    def _is_ready(self) -> bool:
        is_ready = self._cache_is_ready()
        return is_ready

    def _cache_is_ready(self):
        """
        looks somehow ugly, but necessary in order to prevent build of dependencies when actually already cached
        if is_ready is True does prevent complete build: not building cache not even loading caches (of nodes further up in the graph)!!
        # TODO: could enforce here that all runtime-dependencies are ready, concepts of build-time vs. runtime not yet implemented!!!
        """
        is_ready = self._was_built
        if not is_ready:
            is_ready = self._found_and_loaded_from_cache()
        new_cache_root = BASE_PATHES.get("EXPORT_CACHE_ROOT", None)

        if new_cache_root is not None and not self.cache_base_is_blacklisted(
            export_black_list
        ):
            # necessary to traverse deeper into graph
            # os.environ["NO_BUILD"] = "True"
            is_ready = False
        return is_ready

    def _maybe_export_cache(self):
        new_cache_root = BASE_PATHES.get("EXPORT_CACHE_ROOT", None)
        # black_list = BASE_PATHES.get("EXPORT_CACHE_cache_base_blacklist",[])
        new_cache_dir = f"{new_cache_root}/{self.cache_dir.suffix}"
        if (
            new_cache_root is not None
            and not os.path.isdir(new_cache_dir)
            and not self.cache_base_is_blacklisted(export_black_list)
        ):
            shutil.copytree(
                str(self.cache_dir),
                new_cache_dir,
            )
            print(f"copied {str(self.cache_dir)} to {new_cache_root}")

    def cache_base_is_blacklisted(self, black_list):
        return any(
            (
                str(self.cache_base).endswith(forbidden_suffix)
                for forbidden_suffix in black_list
            )
        )

    def _found_and_loaded_from_cache(self):

        found_json = self._found_dataclass_json()
        if self.overwrite_cache:
            remove_if_exists(str(self.cache_dir))
            successfully_loaded_cached = False
        elif found_json:
            just_try(
                lambda: self._pre_build_load_state_fields(),
                reraise=True,
                verbose=True,
                fail_print_message_builder=lambda: f"could not _load_state_fields for {type(self).__name__=}",
            )
            self._load_cached_data()
            self._post_build_setup()
            self._maybe_export_cache()
            # print(
            #     f"LOADED cached: {self.name} ({self.__class__.__name__}) from {self.cache_dir}"
            # )
            successfully_loaded_cached = True
        else:
            successfully_loaded_cached = False

        return successfully_loaded_cached

    @property
    @abstractmethod
    def name(self):
        """
        just to increase cached-data "readability", this is no id! no need to be "unique"
        enforcing this! readability is user-friendly!
        """
        raise NotImplementedError

    def prefix_cache_dir(self, path: str) -> str:
        assert isinstance(
            self.cache_dir, PrefixSuffix
        ), f"{self} has invalid cache_dir: {self.cache_dir}"
        return f"{self.cache_dir}/{path}"

    @abstractmethod
    def _build_cache(self):
        # TODO: rename to "build_cache"
        raise NotImplementedError

    def _build_self(self):
        self.maybe_build_cache()

        start = time()
        self._post_build_setup()
        duration = time() - start
        if duration >= 1.0:
            print(
                f"SETUP: {self.name} ({self.__class__.__name__}) took: {duration} seconds from {self.cache_dir}"
            )
        self._maybe_export_cache()

    def _prepare(self, cache_dir: str) -> None:
        pass

    def __cache_creation_is_in_process(self):
        return os.path.isfile(f"{self.cache_dir}.lock") or os.path.isfile(
            f"{self.cache_dir}.lock.lock"
        )

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

        should_build_cache = claim_write_access(self.dataclass_json)
        if os.environ.get("NO_BUILD", "False").lower() != "false":
            assert (
                not should_build_cache
            ), f"env-var NO_BUILD is set but {self.__class__.__name__} did not find {self.dataclass_json=}"
        return should_build_cache

    def _post_build_setup(self):
        """
        use this to prepare stuff, last step in build_or_load, called after build_cache and _load_cached_data
        might be building children even though loaded from cache, cannot enforce that all laoded children are "ready"
        might be that some are only "build-time" dependencies
        """
        pass

    def _load_cached_data(self):
        """
        just called when cache was found
        """
        pass

    @beartype
    def maybe_build_cache(
        self,
    ) -> None:

        if self._claimed_right_to_build_cache():
            cadi = str(self.cache_dir)
            remove_make_dir(cadi)
            error = None
            try:
                # start = time()
                # sys.stdout.write(
                #     f"building CACHE {self.name} ({self.__class__.__name__}) by {multiprocessing.current_process().name}"
                # )
                self._build_cache()
                # sys.stdout.write(
                #     f"{self.name} took: {time()-start} secs; in cache-dir: {cadi} \n"
                # )
                # sys.stdout.flush()

                write_json(self.dataclass_json, encode_dataclass(self), do_flush=True)
                # sleep(1) # TODO:  WTF! sleep here seems to alleviate problem with multiprocessing
            except Exception as e:
                error = e
                if self.clean_on_fail:
                    shutil.rmtree(cadi, ignore_errors=True)
            finally:
                remove_if_exists(f"{self.dataclass_json}.lock")
                remove_if_exists(f"{self.dataclass_json}.lock.lock")
                if error is not None:
                    raise error
        else:
            remove_if_exists(
                f"{self.dataclass_json}.lock"
            )  # failed attempt to claim lock may still create lock-file!
            remove_if_exists(f"{self.dataclass_json}.lock.lock")
            does_exist = False  # TODO wtf!
            for _ in range(3):
                if self._found_dataclass_json():
                    does_exist = True
                    break
                else:
                    sleep(1.0)

            assert does_exist, f"{self.dataclass_json=} must exist!"
            start = time()
            self._load_cached_data()
            duration = time() - start
            if duration >= 1.0:
                print(
                    f"LOADED cached: {self.name} ({self.__class__.__name__}) took: {duration} seconds from {self.cache_dir}"
                )

    def _pre_build_load_state_fields(self):
        """
        this is getting called before _build_all_children !! so a shape-shifting child gets loaded from cache before its build is called!
        """
        cache_data_json = read_file(self.dataclass_json)
        loaded_dc = deserialize_dataclass(cache_data_json)
        repr_fields = list(
            f
            for f in dataclasses.fields(self)
            if hasattr(
                loaded_dc, f.name
            )  # dataclasses.field() without default produced "missing" field
            # buildable fields also get loaded!!! that supposed to be like this! cause they could have shape-shifted!!
            # if f.repr and not isinstance(getattr(self, f.name), Buildable)
        )
        # print(
        #     f"{type(self).__name__}-{self.name}: loading state-fields: {[f.name for f in repr_fields]}"
        # )
        for f in repr_fields:
            setattr(self, f.name, getattr(loaded_dc, f.name))
        # TODO: why was I not loading input fields in the past?
        if hash_dataclass(self) != hash_dataclass(loaded_dc):
            write_json("loaded_dc.json", encode_dataclass(loaded_dc))
            write_json("self_dc.json", encode_dataclass(self))
            assert (
                False
            ), f"icdiff <(cat loaded_dc.json | jq . ) <(cat self_dc.json | jq . ) | less -r"

    def create_cache_dir_from_hashed_self(self) -> PrefixSuffix:
        assert isinstance(
            self.cache_base, PrefixSuffix
        ), f"{self.name},({type(self).__name__})"
        all_undefined_must_be_filled(self, extra_field_names=["name"])
        typed_self = type(self).__name__
        name = self.name.replace("/", "_")
        hash_suffix = f"-{hash_dataclass(self)}" if self.use_hash_suffix else ""
        return PrefixSuffix(
            prefix_key=self.cache_base.prefix_key,
            suffix=f"{self.cache_base.suffix}/{typed_self}-{name}{hash_suffix}",
        )

import dataclasses
from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable, ClassVar, Iterator, Any, TypeVar

from data_io.readwrite_files import write_jsonl, read_jsonl, read_lines, write_lines
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import (
    encode_dataclass,
    deserialize_dataclass,
)
from misc_utils.utils import just_try

T = TypeVar("T")


@dataclass
class CachedDataclasses(Iterable[T], CachedData):

    jsonl_file_name: ClassVar[str] = "data.jsonl"

    @property
    def jsonl_file(self):
        return self.prefix_cache_dir(self.jsonl_file_name)

    def _build_cache(self):
        write_jsonl(
            self.jsonl_file,
            self.generate_dicts_to_cache(),
        )

    @abstractmethod
    def generate_dataclasses_to_cache(self) -> Iterator[T]:
        raise NotImplementedError

    def generate_dicts_to_cache(self) -> Iterator[dict]:
        yield from (encode_dataclass(o) for o in self.generate_dataclasses_to_cache())

    def __iter__(self) -> Iterator[T]:
        yield from (deserialize_dataclass(s) for s in read_lines(self.jsonl_file))


@dataclass
class ContinuedCachedData(CachedData):
    clean_on_fail: bool = dataclasses.field(default=False, repr=False)

    @property
    def _is_ready(self) -> bool:
        """
        falling back to buildables _is_ready logic that "_was_built" flag must be set
        so even though cached data was found still wants to be built -> and thereby build/load its children!
        """
        return self._was_built

    def _build_cache(self):
        print(f"start from scratch in {self.cache_dir}")

    def _post_build_setup(self):
        print(f"continue in {self.cache_dir}")
        self.continued_build_cache()
        # TODO: why did I want to just_try here?
        # just_try(lambda: self.continued_build_cache(), verbose=True)

    @abstractmethod
    def continued_build_cache(self) -> None:
        raise NotImplementedError


@dataclass
class ContinuedCachedDicts(ContinuedCachedData, Iterable[dict]):
    append_jsonl: ClassVar[str] = True
    jsonl_file_name: ClassVar[str] = "data.jsonl"

    @property
    def jsonl_file(self):
        return self.prefix_cache_dir(self.jsonl_file_name)

    @abstractmethod
    def generate_dicts_to_cache(self) -> Iterator[dict]:
        """
        in case of append_jsonl==True yield only new data here!
        """
        raise NotImplementedError

    @abstractmethod
    def build_state_from_cached_data(self) -> None:
        raise NotImplementedError

    def continued_build_cache(self) -> None:
        self.build_state_from_cached_data()

        write_jsonl(
            self.jsonl_file,
            self.generate_dicts_to_cache(),
            mode="ab" if self.append_jsonl else "wb",
        )

    def __iter__(self) -> Iterator[dict]:
        yield from read_jsonl(self.jsonl_file)


@dataclass
class ContinuedCachedDataclasses(ContinuedCachedDicts):
    @abstractmethod
    def generate_dataclasses_to_cache(self) -> Iterator[T]:
        """
        in case of append_jsonl==True yield only new data here!
        """
        raise NotImplementedError

    def generate_dicts_to_cache(self) -> Iterator[dict]:
        """
        in case of append_jsonl==True yield only new data here!
        """
        yield from (encode_dataclass(o) for o in self.generate_dataclasses_to_cache())

    def __iter__(self) -> Iterator[T]:
        yield from (deserialize_dataclass(s) for s in read_lines(self.jsonl_file))


@dataclass
class CachedList(CachedData, list):
    # TODO: make generic!

    @property
    def _is_ready(self) -> bool:
        return len(self) > 0

    @property
    def data_file(self):
        return self.prefix_cache_dir("data.txt.gz")

    @abstractmethod
    def _build_data(self) -> list[Any]:
        raise NotImplementedError

    def _build_cache(self):
        write_lines(self.data_file, self._build_data())

    def _post_build_setup(self) -> None:
        assert len(self) == 0, f"CachedList {self} must be empty when before loading"
        self.extend(read_lines(self.data_file))

import os
from abc import abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Any, Iterable, Iterator

from data_io.readwrite_files import read_jsonl, write_jsonl
from misc_utils.beartypes import Dataclass, GenericDataclass
from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import UNDEFINED
from misc_utils.prefix_suffix import PrefixSuffix, BASE_PATHES


@dataclass
class BuildableData(Buildable):
    """
    use this for long-lived cache!
    """

    base_dir: PrefixSuffix = field(default_factory=lambda: BASE_PATHES["raw_data"])

    @property
    def data_dir(self) -> str:
        return f"{self.base_dir}/{self.name}"

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    def _is_ready(self) -> bool:
        is_valid = self._is_data_valid
        if is_valid:
            self._load_data()
        return is_valid

    def _load_data(self):
        """
        if you want this to be run "always" also after _build_data do call it there yourself!
        """
        pass

    @property
    @abstractmethod
    def _is_data_valid(self) -> bool:
        """
        check validity of data
        """
        raise NotImplementedError

    def _build_self(self) -> Any:
        o = self._build_data()
        return self if o is None else o

    @abstractmethod
    def _build_data(self) -> Any:
        """
        build/write data
        """
        raise NotImplementedError


@dataclass
class BuildableDataClasses(BuildableData, Iterable[GenericDataclass]):
    @property
    def jsonl_file(self) -> str:
        return f"{self.data_dir}/data.jsonl"

    @property
    def _is_data_valid(self) -> bool:
        return os.path.isfile(self.jsonl_file)

    @abstractmethod
    def _generate_dataclasses(self) -> Iterator[GenericDataclass]:
        raise NotImplementedError

    def _build_data(self) -> Any:
        os.makedirs(self.data_dir)
        write_jsonl(
            self.jsonl_file,
            (
                dc.to_dict() if hasattr(dc, "to_dict") else asdict(dc)
                for dc in self._generate_dataclasses()
            ),
        )

    def __iter__(self) -> Iterator[GenericDataclass]:
        # TODO: looks like black-magic!! is it allowed?
        clazz = self.__orig_bases__[0].__args__[0]
        create_fun = (
            clazz.from_dict if hasattr(clazz, "from_dict") else lambda x: clazz(**x)
        )
        yield from (create_fun(d) for d in read_jsonl(self.jsonl_file))

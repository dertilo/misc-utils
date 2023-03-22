import os
from abc import abstractmethod
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable, Iterator, TypeVar, Annotated

from beartype.vale import Is
from slugify import slugify

from data_io.readwrite_files import read_jsonl, write_jsonl
from misc_utils.beartypes import Dataclass, NeStr
from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import UNDEFINED
from misc_utils.prefix_suffix import PrefixSuffix, BASE_PATHES


def is_sluggy(s: NeStr) -> bool:
    regex_pattern_to_allow_underscores = r"[^-a-z0-9_]+"
    return slugify(s, regex_pattern=regex_pattern_to_allow_underscores) == s


SlugStr = Annotated[NeStr, Is[is_sluggy]]


def is_cased_sluggy(s: NeStr) -> bool:
    regex_pattern_to_allow_underscores = r"[^-A-Za-z0-9_]+"
    return (
        slugify(s, regex_pattern=regex_pattern_to_allow_underscores, lowercase=False)
        == s
    )


CasedSlugStr = Annotated[NeStr, Is[is_cased_sluggy]]


@dataclass
class BuildableData(Buildable):
    """
    just some helper-methods / "convenience logic" like:
        - defining a "data_dir" that consists of a "base_dir" and a folder-name (the "name"-property here)
        - checking if data is valid (_is_data_valid) if so loading it (_load_data)
        - reminding you to implement a "_is_data_valid"- and "_build_data" method

    """

    base_dir: PrefixSuffix = field(default_factory=lambda: BASE_PATHES["raw_data"])

    # TODO: can I do it like this? probably not cause here the BASE_PATHES["raw_data"] get evaluated/executed earlier!
    # base_dir: ClassVar[PrefixSuffix] =  BASE_PATHES["raw_data"]

    @property
    def data_dir(self) -> str:
        data_dir = f"{self.base_dir}/{self.name}"
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        return data_dir

    @property
    def data_dir_prefix_suffix(self) -> PrefixSuffix:
        return PrefixSuffix(
            self.base_dir.prefix_key, f"{self.base_dir.suffix}/{self.name}"
        ).build()

    @property
    @abstractmethod
    def name(self) -> SlugStr:
        raise NotImplementedError

    @property
    def _is_ready(self) -> bool:
        is_valid = self._is_data_valid
        # if is_valid: # TODO: no loading here! but think  about it, hopefully this does not break things elsewhere!
        #     self._load_data() # this is not really explicit!
        return is_valid

    def __enter__(self):
        """
        use to load the model into memory, prepare things
        """
        pass

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        """
        use as tear-down, to free memory, unload model
        """
        pass

    # def _load_data(self):
    #     """
    #     TODO: this gets called in _is_ready!!
    #     if you want this to be run "always" also after _build_data do call it there yourself!
    #     """
    #     pass

    @property
    @abstractmethod
    def _is_data_valid(self) -> bool:
        """
        check validity of data
        """
        raise NotImplementedError

    def _build_self(self) -> Any:
        o = self._build_data()
        assert (
            self._is_data_valid
        ), f"{self.__class__.__name__}: {self.name} failed to build data in {self.data_dir=}"
        return self if o is None else o

    @abstractmethod
    def _build_data(self) -> Any:
        """
        build/write data
        """
        raise NotImplementedError


SomeDataclass = TypeVar(
    "SomeDataclass"
)  # cannot use beartype here cause pycharm wont get it


@dataclass
class BuildableDataClasses(BuildableData, Iterable[SomeDataclass]):
    @property
    def jsonl_file(self) -> str:
        return f"{self.data_dir}/data.jsonl.gz"

    @property
    def _is_data_valid(self) -> bool:
        return os.path.isfile(self.jsonl_file)

    @abstractmethod
    def _generate_dataclasses(self) -> Iterator[SomeDataclass]:
        raise NotImplementedError

    def _build_data(self) -> Any:
        os.makedirs(self.data_dir, exist_ok=True)
        write_jsonl(
            self.jsonl_file,
            (
                dc.to_dict() if hasattr(dc, "to_dict") else asdict(dc)
                for dc in self._generate_dataclasses()
            ),
        )

    def __iter__(self) -> Iterator[SomeDataclass]:
        # TODO: looks like black-magic!! is it allowed?
        clazz = self.__orig_bases__[0].__args__[0]
        create_fun = (
            clazz.from_dict if hasattr(clazz, "from_dict") else lambda x: clazz(**x)
        )
        yield from (create_fun(d) for d in read_jsonl(self.jsonl_file))

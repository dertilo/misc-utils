import dataclasses
from dataclasses import dataclass
from typing import Union, ClassVar, Any

from misc_utils.buildable import Buildable

BASE_PATHES: dict[str, Union[str, "PrefixSuffix"]] = {}


@dataclass
class PrefixSuffix(Buildable):
    prefix_key: str
    suffix: str

    prefix: str = dataclasses.field(init=False)
    __exclude_from_hash__: ClassVar[list[str]] = ["prefix"]

    def __set_prefix(self):
        self.prefix = BASE_PATHES[self.prefix_key]
        # assert len(self.prefix) > 0, f"base_path is empty!"

    def _build_self(self) -> Any:
        """
        more lazy than post_init, "builds" prefix, only needed in case one newer calls str()
        """
        return self.__set_prefix()

    def __repr__(self) -> str:
        """
        base_path may not exist no constraints here!
        """
        self.__set_prefix()
        return f"{self.prefix}/{self.suffix}"

import dataclasses
from dataclasses import dataclass
from typing import Union, ClassVar, Any

from misc_utils.buildable import Buildable

BASE_PATHES: dict[str, Union[str, "PrefixSuffix"]] = {}
BASE_PATHES["pwd"] = "."


@dataclass
class PrefixSuffix(Buildable):
    prefix_key: str
    suffix: str

    prefix: str = dataclasses.field(init=False)
    __exclude_from_hash__: ClassVar[list[str]] = ["prefix"]

    def _set_prefix(self):
        self.prefix = BASE_PATHES[self.prefix_key]
        # assert len(self.prefix) > 0, f"base_path is empty!"

    def _build_self(self) -> Any:
        """
        more lazy than post_init, "builds" prefix, only needed in case one newer calls str()
        """
        return self._set_prefix()

    def __repr__(self) -> str:
        """
        base_path may not exist no constraints here!
        """
        if self.prefix_key in BASE_PATHES:
            self._set_prefix()
            repr = f"{self.prefix}/{self.suffix}"
        else:
            """
            inspect calls the __repr__ method before BASE_PATHES was initialized!!

            File "python3.9/inspect.py", line 2593, in __str__
            formatted = '{} = {}'.format(formatted, repr(self._default))
            File "misc-utils/misc_utils/prefix_suffix.py", line 35, in __repr__
            self.__set_prefix()
            File "misc-utils/misc_utils/prefix_suffix.py", line 22, in __set_prefix
            
            """
            this_is_only_used_for_hashing=f"{self.prefix_key}/{self.suffix}"
            repr=this_is_only_used_for_hashing
        return repr

    def from_str_same_prefix(self, path: str):
        assert str(path).startswith(self.prefix)
        file_suffix = str(path).replace(f"{self.prefix}/", "")
        return PrefixSuffix(self.prefix_key, file_suffix)

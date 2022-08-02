from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any

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
    def name(self):
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
        return self._build_data()

    @abstractmethod
    def _build_data(self) -> Any:
        """
        build/write data
        """
        raise NotImplementedError

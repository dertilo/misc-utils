from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

from misc_utils.buildable import Buildable
from misc_utils.dataclass_utils import UNDEFINED
from misc_utils.prefix_suffix import PrefixSuffix


@dataclass
class BuildableData(Buildable):
    """
    use this for long-lived cache!
    """

    base_dir: PrefixSuffix = UNDEFINED

    @property
    def data_dir(self):
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

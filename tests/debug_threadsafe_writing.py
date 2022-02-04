import multiprocessing
import os
import random
import shutil
from dataclasses import dataclass
from typing import Union

from time import sleep

from data_io.readwrite_files import write_file, read_file
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import UNDEFINED, _UNDEFINED
from misc_utils.processing_utils import process_with_threadpool
from misc_utils.utils import claim_write_access


@dataclass
class DataToBeCached(CachedData):
    input: Union[_UNDEFINED, str] = UNDEFINED
    state: str = "blank"

    def _build_cache(self):
        self.state = "some state"


def compete_writing(k):
    # delay = random.uniform(0, 1)
    # sleep(delay)
    access = claim_write_access("tests/resources/test")
    return access
    # print(f"{k}-{delay=},{ access=}")

    # data=DataToBeCached(cache_base="tests/resources",input="foo").build()
    # return f"{k}-{delay=},{claim_write_access('tests/resources/test')=}"


def rm_if_exists(f):
    if os.path.isfile(f):
        os.remove(f)


def main():

    rm_if_exists("tests/resources/test.lock")
    rm_if_exists("tests/resources/test.lock.lock")
    n = 5
    data = [k for k in range(n)]
    # print(list(process_with_threadpool(data, compete_writing, 3)))

    with multiprocessing.Pool(processes=n) as p:
        result = list(p.imap_unordered(compete_writing, data))
    only_one_got_access = sum(result) == 1
    assert only_one_got_access


if __name__ == "__main__":
    main()
    main()
    main()
    main()

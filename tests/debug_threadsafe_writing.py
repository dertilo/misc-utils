import multiprocessing
import os
import random
import shutil
from dataclasses import dataclass, field
from typing import Union

from time import sleep

from data_io.readwrite_files import write_file, read_file, read_json
from misc_utils.cached_data import CachedData
from misc_utils.dataclass_utils import UNDEFINED, _UNDEFINED
from misc_utils.processing_utils import process_with_threadpool
from misc_utils.utils import claim_write_access


@dataclass
class DataToBeCached(CachedData):
    input: Union[_UNDEFINED, str] = UNDEFINED
    state: str = field(init=False)

    def _build_cache(self):
        sleep(0.2)  # caching must take longer than wait in FileLock!
        # otherwise finally in CachedData removes the locks another one crate lock-file that get never removed and prevents everyone from reading!
        self.state = multiprocessing.current_process().name


def compete_writing(k):
    # delay = random.uniform(0, 1)
    # sleep(delay)
    me = multiprocessing.current_process().name

    access = claim_write_access("tests/resources/test")
    print(f"{me=},{access=}")
    return access
    # print(f"{k}-{delay=},{ access=}")

    # data=DataToBeCached(cache_base="tests/resources",input="foo").build()
    # return f"{k}-{delay=},{claim_write_access('tests/resources/test')=}"


def compete_caching(k):
    # delay = random.uniform(0, 1)
    # sleep(delay)
    # print(f"{k}-{delay=},{ access=}")

    data = DataToBeCached(cache_base="tests/resources/cache", input="foo")
    me = multiprocessing.current_process().name
    data.build()
    print(f"{me=},{data.state=}")
    was_build_by_me = me == data.state
    return was_build_by_me


def rm_if_exists(f):
    if os.path.isfile(f):
        os.remove(f)


def main():
    cache_dir = "tests/resources/cache"
    shutil.rmtree(cache_dir, ignore_errors=True)
    os.makedirs(cache_dir)
    rm_if_exists("tests/resources/test.lock")
    rm_if_exists("tests/resources/test.lock.lock")
    n = 9
    data = [k for k in range(n)]
    # print(list(process_with_threadpool(data, compete_writing, 3)))

    with multiprocessing.Pool(processes=n) as p:
        result = list(p.imap_unordered(compete_caching, data))
    only_one_got_access = sum(result) == 1
    assert only_one_got_access, f"{result=}"


if __name__ == "__main__":
    main()
    main()
    main()
    main()
#

import collections
import multiprocessing
import os
from datetime import datetime

import filelock
import itertools
import random
import sys
import traceback
from dataclasses import dataclass, field
from hashlib import sha1

from filelock import FileLock
from time import time, sleep
from typing import Iterable, Callable, TypeVar, Optional, Union, Iterator, Any, Generic

from beartype import beartype

from data_io.readwrite_files import write_file, read_file

T = TypeVar("T")
T_default = TypeVar("T_default")


@beartype  # bear makes actually not sense here!?
def just_try(
    supplier: Callable[[], T],
    default: T_default = None,
    reraise: bool = False,
    verbose: bool = False,
    fail_message_builder: Optional[Callable[..., Any]] = None,
) -> Union[T, T_default]:
    try:
        return supplier()
    except Exception as e:
        if verbose:
            print(f"\ntried and failed with: {e}\n")
            traceback.print_exc(file=sys.stderr)
        if reraise:
            raise e
        if fail_message_builder is not None:
            return fail_message_builder(error=e, sys_stderr=sys.stderr)
        else:
            return default


def just_try_for_each(
    input_it: Iterable[T],
    default: T_default = None,
    break_on_failure: bool = False,
    # reraise:bool=False,
    verbose: bool = False,
) -> Iterator[Union[T, T_default]]:
    it = iter(input_it)
    while True:
        # resp=just_try(lambda: next(it),default=default,reraise=reraise,verbose=verbose)
        try:
            resp = next(it)
        except StopIteration:
            break
        except Exception as e:
            if verbose:
                print(f"\ntried and failed with: {e}\n")
                traceback.print_exc(file=sys.stderr)
            if break_on_failure:
                break
            else:
                resp = default

        yield resp


@beartype
def buffer_shuffle(
    data: Iterable[T], buffer_size: int, verbose: bool = False
) -> Iterator[T]:
    """
    based on : https://github.com/pytorch/pytorch/commit/96540e918c4ca3f0a03866b9d281c34c65bd76a4#diff-425b66e1ff01d191679c386258a7156dfb5aacd64a8e0947b24fbdebcbee8529
    """
    it = iter(data)
    start = time()
    buf = [next(it) for _ in range(buffer_size)]
    if verbose:
        print(f"filling shuffle-buffer of size {len(buf)} took: {time()-start} seconds")

    for x in it:
        idx = random.randint(0, buffer_size - 1)
        yield buf[idx]
        buf[idx] = x

    random.shuffle(buf)
    while buf:
        yield buf.pop()


def get_dict_paths(paths, root_path, my_dict):
    if not isinstance(my_dict, dict):
        paths.append(root_path)
        return root_path
    for k, v in my_dict.items():
        path = root_path + [k]
        get_dict_paths(paths, path, v)


class Singleton(type):
    """
    see: https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass
class _NOT_EXISTING(metaclass=Singleton):
    pass


NOT_EXISTING = _NOT_EXISTING()


@beartype
def get_val_from_nested_dict(d: dict, path: list[str]) -> Union[Any, _NOT_EXISTING]:
    for key in path:
        if key in d.keys():
            d = d[key]
        else:
            d = NOT_EXISTING
            break
    return d


@dataclass
class TimedIterable(Generic[T]):
    iterable: Iterable[T]
    duration: float = field(default=0.0, init=False)
    outcome: float = field(default=0.0, init=False)
    # overall_duration_only:bool
    weight_fun: Callable[[Any], float] = lambda x: 1.0

    def __iter__(self) -> Iterator[T]:
        last_time = time()
        for x in self.iterable:
            self.duration += time() - last_time
            self.outcome += self.weight_fun(x)
            yield x
            last_time = time()

    @property
    def speed(self):
        return self.outcome / self.duration


def sanitize_hexappend_filename(filename: str) -> str:
    """
    see: https://stackoverflow.com/questions/7406102/create-sane-safe-filename-from-any-unsafe-string
    """
    sane = "".join([c for c in filename if c.isalpha() or c.isdigit()]).rstrip()
    if sane != filename:
        hex_dings = sha1(filename.encode("utf-8")).hexdigest()
        sane = f"{sane}_{hex_dings}"
    return sane


def flatten_nested_dict(
    d: collections.MutableMapping, key_path: Optional[list[str]] = None, sep="_"
) -> list[tuple[list[str], Any]]:
    items = []
    for k, v in d.items():
        new_key = key_path + [k] if key_path is not None else [k]
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_nested_dict(v, new_key, sep=sep))
        else:
            items.append((new_key, v))
    return items


@beartype
def set_val_in_nested_dict(d: dict, path: list[str], value: Any):
    for i, key in enumerate(path):
        if key not in d.keys():
            d[key] = {}

        if i == len(path) - 1:
            d[key] = value
        else:
            d = d[key]


def nest_flattened_dict(flattened: list[tuple[list[str], Any]]):
    nested_dict = {}
    for path, value in flattened:
        set_val_in_nested_dict(nested_dict, path, value)
    return nested_dict


def collapse_sequence(input: Iterable, merge_fun: Callable, get_key_fun: Callable):
    def collapse(g):
        l = list(g)
        key, _ = l[0]
        assert len(set([c for c, d in l])) == 1
        return key, merge_fun([x for _, x in l])

    return list(collapse(g) for _, g in itertools.groupby(input, key=get_key_fun))


def count_many(d: dict, counters):
    for k, v in d.items():
        counters[k].update({v: 1})


def claim_write_access(file_or_dir) -> bool:
    """
    if false, someone else already claimed it!
    """
    claimed_rights_to_write = False
    lock_file = f"{file_or_dir}.lock"
    lock_lock_file = f"{file_or_dir}.lock.lock"
    me = multiprocessing.current_process().name

    def already_existent():
        return os.path.isfile(file_or_dir) or os.path.isdir(file_or_dir)

    while not os.path.isfile(lock_file) and not already_existent():
        try:
            with FileLock(lock_file, timeout=0.1):
                print(f"{me=}: {datetime.now()}")
                # TODO: not working like this!
                # even though I manually force flushing, still seems to be some buffering/delay in writing to the file, -> writing to file not "real-time", not thread-safe
                # if len(read_file(lock_file))==0:
                #     write_file(lock_file, me,do_flush=True) #
                #     # sleep(1.0)
                #     claimed_rights_to_write=True
                #     print(f"{me=}: {datetime.now()}")

                if not os.path.isfile(lock_lock_file) and not already_existent():
                    write_file(lock_lock_file, me)
                    # sleep(1)
                    claimed_rights_to_write = True
                break
        except filelock._error.Timeout:
            # pass
            print("retry claim_write_access")
            # sys.stdout.write(fail_message)
            # sys.stdout.flush()
            # fail_message = "."
    if not claimed_rights_to_write and already_existent():
        os.remove(
            lock_file
        )  # must be own lock-file of unsuccessful attempt to claim rigts
    return claimed_rights_to_write

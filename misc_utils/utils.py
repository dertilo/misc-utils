import asyncio
import threading
from collections.abc import MutableMapping

import itertools
import random
import sys
import traceback
from dataclasses import dataclass, field
from hashlib import sha1

from time import time, sleep
from typing import (
    Iterable,
    Callable,
    TypeVar,
    Optional,
    Union,
    Iterator,
    Any,
    Generic,
    AsyncIterator,
)

from beartype import beartype

T = TypeVar("T")
T_default = TypeVar("T_default")


@beartype  # bear makes actually not sense here!?
def just_try(
    supplier: Callable[[], T],
    default: T_default = None,
    reraise: bool = False,
    verbose: bool = False,
    print_stacktrace: bool = True,
    fail_return_message_builder: Optional[Callable[..., Any]] = None,
    fail_print_message_builder: Optional[Callable[..., Any]] = None,
) -> Union[T, T_default]:
    try:
        return supplier()
    except Exception as e:
        if verbose:
            m = (
                fail_print_message_builder()
                if fail_print_message_builder is not None
                else ""
            )
            print(f"\ntried and failed with: {e}\n{m}\n")
            if print_stacktrace:
                traceback.print_exc(file=sys.stderr)
        if reraise:
            raise e
        if fail_return_message_builder is not None:
            return fail_return_message_builder(error=e, sys_stderr=sys.stderr)
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
    durations: list[float] = field(default_factory=lambda: [], init=False)
    outcomes: list[float] = field(default_factory=lambda: [], init=False)
    # overall_duration_only:bool
    weight_fun: Callable[[Any], float] = lambda x: 1.0

    def __iter__(self) -> Iterator[T]:
        """TODO: whatabout async iter?"""
        last_time = time()
        for x in self.iterable:
            dur = time() - last_time
            self.durations.append(dur)
            self.duration += dur
            out = self.weight_fun(x)
            self.outcomes.append(out)
            self.outcome += out
            yield x
            last_time = time()

    @property
    def speed(self):
        return self.outcome / self.duration if self.duration > 0 else -1

    @property
    def avg_duration(self):
        return 1 / self.speed

    def __repr__(self):
        import pandas

        return (
            f"{pandas.DataFrame(self.durations).describe(percentiles=[0.5]).to_dict()}"
        )


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
    d: MutableMapping, key_path: Optional[list[str]] = None, sep="_"
) -> list[tuple[list[str], Any]]:
    items = []
    for k, v in d.items():
        new_key = key_path + [k] if key_path is not None else [k]
        if isinstance(v, MutableMapping):
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


def retry(
    fun,
    num_retries=3,
    wait_time=1.0,
    increase_wait_time=False,
    do_raise=True,
    default=None,
    fail_message=None,
):
    exception = None
    for k in range(1 + num_retries):  # if num_retries==0 should still try once!
        try:
            return fun()
        except Exception as e:
            exception = e
            print(f"retry failure:\n{exception=}\n")
            if increase_wait_time:
                waiting_time = wait_time * 2 ** k
            else:
                waiting_time = wait_time
            sleep(waiting_time)
    print(f"retry failed {num_retries} times!")
    if do_raise:
        if fail_message:
            print(fail_message)
        raise exception
    else:
        return default


@beartype
def build_markdown_table(
    rows: list[list[float]],
    row_name: str,
    col_name: str,
    row_names: list[str],
    col_names: list[str],
):
    def rounded_percentage(v: float) -> str:
        return f"{100 * v:.1f}%"

    rows_s = [[rounded_percentage(v) for v in r] for r in rows]
    header = " | ".join([f"{row_name} \ {col_name}"] + col_names)
    line = " | ".join(["---" for _ in range(len(col_names) + 1)])
    rows = [" | ".join([name] + cols) for name, cols in zip(row_names, rows_s)]
    return "\n".join([header, line] + rows)


def format_table_cell(
    v,
):
    if isinstance(v, float):
        v = f"{v:.3f}"
    return f"{v}"


@beartype
def build_markdown_table_from_dicts(
    dicts: list[dict],
    col_title: Optional[str] = None,
    col_names: Optional[list[str]] = None,
    format_fun: Callable[[Any], str] = format_table_cell,
) -> str:
    if col_names is None:
        col_names = list(dicts[0].keys())

    row_title = col_names[0]
    rows_s = [[format_fun(d[c]) for c in col_names] for d in dicts]
    col_title = f" \ {col_title}" if col_title is not None else ""
    header = " | ".join([f"{row_title}{col_title}"] + col_names[1:])
    line = " | ".join(["---" for _ in range(len(col_names))])
    rows = [" | ".join(row) for row in rows_s]
    return "\n".join([header, line] + rows)


def async_wrap_iter(it: Iterable) -> AsyncIterator:
    """
    Wrap blocking iterator into an asynchronous one
    copypasted from: https://stackoverflow.com/questions/62294385/synchronous-generator-in-asyncio
    TODO: don't use in production code!!  not sure how suboptimal this solution is!
    """
    loop = asyncio.get_event_loop()
    q = asyncio.Queue(1)
    exception = None
    _END = object()

    async def yield_queue_items():
        while True:
            next_item = await q.get()
            if next_item is _END:
                break
            yield next_item
        if exception is not None:
            # the iterator has raised, propagate the exception
            raise exception

    def iter_to_queue():
        nonlocal exception
        try:
            for item in it:
                # This runs outside the event loop thread, so we
                # must use thread-safe API to talk to the queue.
                asyncio.run_coroutine_threadsafe(q.put(item), loop).result()
        except Exception as e:
            exception = e
        finally:
            asyncio.run_coroutine_threadsafe(q.put(_END), loop).result()

    threading.Thread(target=iter_to_queue).start()
    return yield_queue_items()


T = TypeVar("T")


@beartype
def iterable_to_chunks(
    seq: Iterable[T], is_yieldable_chunk=lambda x: len(x) > 1
) -> Iterator[list[T]]:
    """
    batches normally refer to fixed-size list of things
    chunks more relaxed/liberal, whatever size
    """
    chunk = []
    for k in seq:
        if is_yieldable_chunk(chunk):
            yield chunk
            chunk = []
        chunk.append(k)

    if len(chunk) > 0:
        yield chunk

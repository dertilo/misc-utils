# pylint: skip-file
import bz2
import csv
import gzip
import itertools
import json
import locale
import os
import re
import sys
import tarfile
from typing import Dict, Iterator, Callable, Optional, Tuple, TypeVar, Any, Union
from typing import Iterable

from beartype import beartype

assert locale.getpreferredencoding(False) == "UTF-8"


def write_jsonl(
    file: str,
    data: Iterable[Union[dict, list, tuple]],
    mode="wb",
    do_flush: bool = False,
):
    file = str(file)

    def process_line(d: Dict):
        line = json.dumps(d, skipkeys=True, ensure_ascii=False)
        line = line + "\n"
        if "b" in mode:
            line = line.encode("utf-8")
        return line

    with gzip.open(file, mode=mode) if file.endswith("gz") else open(
        file, mode=mode
    ) as f:
        f.writelines(process_line(d) for d in data)
        if do_flush:
            f.flush()


def write_json(file: str, datum: Dict, mode="wb", do_flush=False):
    file = str(file)
    with gzip.open(file, mode=mode) if file.endswith("gz") else open(
        file, mode=mode
    ) as f:
        line = json.dumps(datum, skipkeys=True, ensure_ascii=False)
        if "b" in mode:
            line = line.encode("utf-8")
        f.write(line)
        if do_flush:
            f.flush()


@beartype
def write_file(file, s: str, mode="wb", do_flush=False):
    file = str(file)
    with gzip.open(file, mode=mode) if file.endswith(".gz") else open(
        file, mode=mode
    ) as f:
        f.write(s.encode("utf-8"))
        if do_flush:
            f.flush()


@beartype
def read_file(file: str, encoding="utf-8") -> str:
    file_io = (
        gzip.open(file, mode="r", encoding=encoding)
        if file.endswith(".gz")
        else open(file, mode="r", encoding=encoding)
    )
    with file_io as f:
        return f.read()


def write_lines(file, lines: Iterable[str], mode="wb"):
    file = str(file)

    def process_line(line):
        line = line + "\n"
        if "b" in mode:  # useful for "text"-mode "t" which uses line-wise buffering
            line = line.encode("utf-8")
        return line

    with gzip.open(file, mode=mode) if file.endswith(".gz") else open(
        file, mode=mode
    ) as f:
        f.writelines(process_line(l) for l in lines)


@beartype
def write_csv(
    file,
    data: Iterable[list[Any]],
    header: Optional[list[str]] = None,
    delimiter: str = "\t",
):
    file = str(file)
    write_lines(
        file,
        itertools.chain(
            [delimiter.join(header)] if header is not None else [],
            (build_csv_row(d, delimiter=delimiter) for d in data),
        ),
    )


@beartype
def read_csv(
    file_path,
    delimiter: str = "\t",
    encoding="utf-8",
    use_json_loads: bool = True,
    process_row: Callable[[list[Any]], list[Any]] = lambda x: x,
    keys: Optional[list[str]] = None,
) -> Iterable[dict]:  # TODO: json.loads should recognize int/float
    lines = read_lines(file_path, encoding=encoding)
    yield from read_csv_lines(
        lines,
        delimiter,
        use_json_loads=use_json_loads,
        process_row=process_row,
        keys=keys,
    )


@beartype
def read_csv_lines(
    lines: Iterable[str],
    delimiter: str,
    use_json_loads: bool = True,
    process_row: Callable[[list[Any]], list[Any]] = lambda x: x,
    keys: Optional[list[str]] = None,
) -> Iterable[dict]:
    if use_json_loads:

        def process_fun(row):
            s = f'[{",".join(row)}]'
            return json.loads(s)

        process_row = process_fun

    it = iter(lines)
    header = [h for h in next(it).replace("\r", "").split(delimiter) if len(h) > 0]
    if keys is not None:
        assert len(header) == len(keys)
        header = keys
    for l in it:
        l = l.replace("\r", "")
        row = l.split(delimiter)
        row = process_row(row)
        assert len(row) == len(header), f"{header=}, {row=}"
        yield {col: row[k] for k, col in enumerate(header)}


def build_csv_row(datum: list[Any], delimiter: str = "\t"):
    line = (
        json.dumps(datum, ensure_ascii=False)
        .replace("[", "")
        .replace(
            "]",
            "",
        )
    )
    cols = [s.strip(" ") for s in line.split(",") if len(s) > 0]
    csv_row = delimiter.join(cols)
    return csv_row


@beartype
def write_dicts_to_csv(
    file,
    data: Iterable[dict[str, Any]],
    header: Optional[list[str]] = None,
    delimiter: str = "\t",
):
    def gen_rows(header: Optional[list[str]]):
        if header is not None:
            yield delimiter.join(header)
        for datum in data:
            if header is None:
                header = list(datum.keys())
                yield delimiter.join(header)
            csv_row = build_csv_row([datum.get(k, None) for k in header])
            yield csv_row

    write_lines(file, gen_rows(header))


def read_lines_from_files(path: str, mode="b", encoding="utf-8", limit=None):
    path = str(path)
    g = (
        line
        for file in os.listdir(path)
        for line in read_lines(os.path.join(path, file), mode, encoding)
    )
    for c, line in enumerate(g):
        if limit and (c >= limit):
            break
        yield line


def read_lines(file, encoding="utf-8", limit=None, num_to_skip=0) -> Iterator[str]:
    file = str(file)
    mode = "rb"
    open_methods = {
        "gz": lambda f: gzip.open(f, mode=mode),
        "bz2": lambda f: bz2.open(f, mode=mode),
    }
    file_io = open_methods.get(
        file.split(".")[-1].lower(), lambda f: open(f, mode=mode)
    )(file)

    with file_io as f:
        _ = [next(f) for _ in range(num_to_skip)]
        for counter, line in enumerate(f):
            if limit is not None and (counter >= limit):
                break
            if "b" in mode:
                line = line.decode(encoding)
            line = line.replace("\n", "").replace("\r", "")
            yield line


def read_jsonl(
    file, encoding="utf-8", limit=None, num_to_skip=0
) -> Iterator[dict[str, Any]]:
    for l in read_lines(file, encoding, limit, num_to_skip):
        yield json.loads(l)


def read_json(file, mode="b") -> dict:
    file = str(file)
    with gzip.open(file, mode="r" + mode) if file.endswith("gz") else open(
        file, mode="r" + mode
    ) as f:
        s = f.read()
        s = s.decode("utf-8") if mode == "b" else s  # type: ignore
        return json.loads(s)


@beartype  # TODO: why was this commented out?
def filter_gen_targz_members(
    targz_file: str,
    is_of_interest_fun: Callable[[tarfile.TarInfo], bool],
    start: Optional[int] = None,
    stop: Optional[int] = None,
    verbose=False,
) -> Iterator[
    Tuple[tarfile.TarInfo, tarfile.ExFileObject]
]:  # TODO(tilo): am I sure about IO as type?
    with tarfile.open(targz_file, "r:gz") as tar:
        for k, member in enumerate(itertools.islice(tar, start, stop)):
            if verbose and k % 10_000 == 0:
                print(f"at position {k} in {targz_file}\n")
            member: tarfile.TarInfo
            if is_of_interest_fun(member):
                f: Optional[tarfile.ExFileObject] = tar.extractfile(member)  # type: ignore
                # https://stackoverflow.com/questions/37474767/read-tar-gz-file-in-python
                # tarfile.extractfile() can return None if the member is neither a file nor a link.
                neither_file_nor_link = f is None
                if not neither_file_nor_link:
                    yield (member, f)

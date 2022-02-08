# pylint: skip-file
import csv
import gzip
import itertools
import json
import locale
import os
import re
import tarfile
from typing import Dict, Iterator, Callable, Optional, Tuple, TypeVar, Any
from typing import Iterable

from beartype import beartype

assert locale.getpreferredencoding(False) == "UTF-8"


def write_jsonl(file: str, data: Iterable[Dict], mode="wb", do_flush: bool = False):
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


def write_file(file, s: str, mode="wb", do_flush=False):
    file = str(file)
    with gzip.open(file, mode=mode) if file.endswith(".gz") else open(
        file, mode=mode
    ) as f:
        f.write(s.encode("utf-8"))
        if do_flush:
            f.flush()


def read_file(file, encoding="utf-8"):
    file = str(file)
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
    file, data: Iterable[list[Any]], header: list[str], delimiter: str = "\t"
):
    file = str(file)
    write_lines(
        file,
        itertools.chain(
            [delimiter.join(header)],
            (build_csv_row(d, delimiter=delimiter) for d in data),
        ),
    )


@beartype
def read_csv(file_path, delimiter: str = "\t") -> Iterable[dict[str, str]]:
    lines = read_lines(file_path)
    it = iter(lines)
    header = next(it).split(delimiter)
    for l in it:
        s = f'[{",".join(l.split(delimiter))}]'
        row = json.loads(s)
        assert len(row) == len(header), f"{header=}, {row=}"
        yield {col: row[k] for k, col in enumerate(header)}


def build_csv_row(datum: list[Any], delimiter: str = "\t"):
    line = json.dumps(datum).replace("[", "").replace("]", "")
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
    path=str(path)
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
    file_io = (
        gzip.open(file, mode=mode)
        if file.endswith(".gz")
        else open(file, mode=mode)  # pylint: disable=consider-using-with
    )
    with file_io as f:
        _ = [next(f) for _ in range(num_to_skip)]
        for counter, line in enumerate(f):
            if limit is not None and (counter >= limit):
                break
            if "b" in mode:
                line = line.decode(encoding)
            yield line.replace("\n", "")


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


# @beartype
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
                print(f"at position {k} in {targz_file}")
            member: tarfile.TarInfo
            if is_of_interest_fun(member):
                f: tarfile.ExFileObject = tar.extractfile(member)  # type: ignore
                # https://stackoverflow.com/questions/37474767/read-tar-gz-file-in-python
                # tarfile.extractfile() can return None if the member is neither a file nor a link.
                neither_file_nor_link = f is None
                if not neither_file_nor_link:
                    yield (member, f)

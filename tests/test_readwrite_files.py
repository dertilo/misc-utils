import json
import os.path
import shutil
from pprint import pprint
from typing import Optional, Any, Iterable, Callable

from data_io.readwrite_files import (
    read_jsonl,
    write_lines,
    read_lines,
    write_csv,
    read_csv,
    write_dicts_to_csv,
)
from misc_utils.utils import flatten_nested_dict, sanitize_hexappend_filename


def test_reading_jsonl():
    data = list(read_jsonl("tests/resources/test.jsonl", limit=3, num_to_skip=2))
    expected = [
        {"a": 1.2, "b": "foo-2", "k": 2},
        {"a": 1.2, "b": "foo-3", "k": 3},
        {"a": 1.2, "b": "foo-4", "k": 4},
    ]
    assert all((e == o for e, o in zip(expected, data)))


def flatten_write_to_csvs(
    directory: str,
    data: Iterable[dict],
    get_id_fun: Optional[Callable[[dict], str]] = None,
):
    """
    data: iterable of nested dicts
    get_id_fun=lambda d:d["id"]
    """
    os.makedirs(directory, exist_ok=False)

    path2filename_file = f"{directory}/path2filename.csv"
    path2filename = {}
    for i, d in enumerate(data):
        eid = get_id_fun(d) if get_id_fun is not None else str(i)
        path_values = flatten_nested_dict(d)
        for path, value in path_values:
            path_s = json.dumps(path)
            filename = sanitize_hexappend_filename(path_s)

            if path_s not in path2filename:
                path2filename[path_s] = filename
                write_lines(path2filename_file, [f"{path_s}\t{filename}"], mode="ab")

            csv_file = f"{directory}/{filename}.csv"
            write_lines(csv_file, [f"{eid}\t{value}"], mode="ab")


# def read_flattened_csvs(directory: str):
#     g = (l.split("\t") for l in read_lines(f"{directory}/path2filename.csv"))
#     path2filename = {path_s: filename for path_s, filename in g}
#     filename2path = {
#         filename: json.loads(path_s) for path_s, filename in path2filename.items()
#     }
# TODO:


def test_dict_to_csvs():
    expected = [
        {"a": 1.2, "b": "foo-2", "k": 2},
        {
            "a": {"a": 1.2, "b": "foo-4", "k": 4},
            "b": {"a": 1.2, "b": {"a": 1.2, "b": "foo-4", "k": 4}, "k": 4},
            "k": 3,
        },
        {"a": 1.2, "b": {"a": 1.2, "b": "foo-4", "k": 4}, "k": 4},
    ]
    test_dir = "/tmp/test_csvs"
    shutil.rmtree(test_dir, ignore_errors=True)
    flatten_write_to_csvs(test_dir, expected)


def test_read_csv():
    expected_data: list[dict[str, str]] = list(
        {"foo": f"foo-{k}", "bar": k} for k in range(3)
    )
    header = ["foo", "bar"]
    test_file = "/tmp/test.csv"
    write_csv(
        test_file,
        data=(tuple([d[col] for col in header]) for d in expected_data),
        header=header,
    )
    data = list(read_csv(test_file))
    assert data == expected_data


def test_write_dicts_to_csv():
    expected_data: list[dict[str, str]] = list(
        {"foo": f"foo-{k}", "bar": k} for k in range(3)
    )
    test_file = "/tmp/test.csv"
    write_dicts_to_csv(
        test_file,
        data=expected_data,
    )
    data = list(read_csv(test_file))
    assert data == expected_data

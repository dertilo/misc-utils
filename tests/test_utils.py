from pprint import pprint

from misc_utils.utils import (
    just_try,
    buffer_shuffle,
    sanitize_hexappend_filename,
    flatten_nested_dict,
    nest_flattened_dict,
)


def test_just_try():
    """
    TODO: seems as if there is nothing to be tested
    """

    o = just_try(lambda: 42, default="foo")
    assert o == 42

    o = just_try(lambda: 1 / 0, default="foo")
    assert o == "foo"

    def foo_fun(x) -> str:
        if x == "str":
            return "bar"
        else:
            return 42

    o = just_try(lambda: foo_fun("str"), default="default")
    assert isinstance(o, str)
    o = just_try(lambda: foo_fun("not str"), default="default")
    assert isinstance(o, int)


def test_buffer_shuffle():
    """
    TODO: seems as if there is nothing to be tested
    """
    data = list(buffer_shuffle([1, "2", 3], buffer_size=2))
    assert "2" == next(filter(lambda x: isinstance(x, str), data))


def test_file_name():
    filename = "ad\n bla/'{-+\)_(ç?"
    sane_filename = sanitize_hexappend_filename(filename)
    print(sane_filename)


def test_flatten_nested_dict():
    expected = {
        "a": {"a": 1.2, "b": "foo-4", "k": 4},
        "b": {"a": 1.2, "b": {"a": 1.2, "b": "foo-4", "k": 4}, "k": 4},
        "k": 3,
    }
    flattened_dict = flatten_nested_dict(expected)
    nested_dict = nest_flattened_dict(flattened_dict)
    assert nested_dict == expected, f"{nested_dict}!={expected}"

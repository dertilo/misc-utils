from misc_utils.dataclass_utils import encode_dataclass, BASE_PATHES
from misc_utils.dummy_task import DummyTask

if __name__ == "__main__":
    BASE_PATHES["debug"] = "/tmp/debug"
    o = DummyTask(input="foo")
    import sys

    m = sys.modules[__name__]
    print(m)
    print(o.__class__.__module__)
    s = encode_dataclass(o)
    print(s)

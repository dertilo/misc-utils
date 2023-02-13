import dataclasses
import json
import uuid
from dataclasses import dataclass
from pprint import pprint
from typing import Union, Any, Optional, Iterator, ClassVar

from beartype import beartype

from data_io.readwrite_files import write_lines, write_file
from misc_utils.beartypes import Dataclass
from misc_utils.cached_data import (
    _IGNORE_THIS_USE_CACHE_DIR,
    _CREATE_CACHE_DIR_IN_BASE_DIR,
)
from misc_utils.dataclass_utils import (
    MyCustomEncoder,
    serialize_dataclass,
    encode_dataclass,
    _UNDEFINED,
    SPECIAL_KEYS,
)
from misc_utils.prefix_suffix import PrefixSuffix

CLASSES_BLACKLIST = [
    _CREATE_CACHE_DIR_IN_BASE_DIR.__name__,
    _IGNORE_THIS_USE_CACHE_DIR.__name__,
    _UNDEFINED.__name__,
    PrefixSuffix.__name__,
]


@dataclass
class Node:
    id: str
    full_module_name: str
    params: Optional[Any] = None
    display_params: ClassVar[bool] = True

    @property
    def class_name(self):
        return self.full_module_name.split(".")[-1]

    def __repr__(self):
        if self.params is None or not self.display_params:
            text = self.class_name
        else:
            params_kv = (
                json.dumps(self.params, indent=4)
                .replace("{", "")
                .replace("}", "")
                .replace('"', "'")
                .replace(":", "=")
            )
            text = f"{self.class_name}({params_kv})"
        return f'{self.id}["{text}"]'


@beartype
def build_node(obj: Any, dc_only: bool = False) -> tuple[Optional[Node], list[str]]:
    assert obj is not None
    if isinstance(obj, dict):

        def is_param(pp):
            return isinstance(pp, (str, int, float))
            # if isinstance(x,dict):
            #     keys=set(x.keys())
            #     is_a_param= len(set(SPECIAL_KEYS).intersection(keys))==0
            # elif :

        d: dict[str, Any] = obj
        params = [k for k, v in d.items() if is_param(v) and k not in SPECIAL_KEYS]
        dependencies = [
            k for k, v in d.items() if k not in params and k not in SPECIAL_KEYS
        ]

        if "_target_" in obj.keys():
            # node = Node(str(x["_id_"]), x["_target_"], params={k: d[k] for k in params})
            node = Node(str(obj["_id_"]), obj["_target_"], params={})
        else:
            if len(params) > 0 and not dc_only:
                node = Node(
                    str(id(obj)),
                    "dict",
                    params={k: d[k] for k in params},
                )
            else:
                node = None
            # node = Node(str(id(x)), "dict", params={})
    else:
        # uuid cause I don't want builtin object to be concentrated in single node
        node = Node(f"{uuid.uuid1()}", type(obj).__name__, params=obj)
        dependencies = []
    return node, dependencies


@beartype
def generate_mermaid_triples(
    d: dict,
    set_of_triple_ids: Optional[list[str]] = None,
) -> Iterator[tuple[Node, str, Node]]:
    dc_only = True

    if set_of_triple_ids is None:
        set_of_triple_ids = []

    hack_for_BuildableContainer = d.get("_target_", "").endswith("BuildableContainer")
    if hack_for_BuildableContainer:
        list_to_be_dictified = isinstance(d["data"], list)
        if list_to_be_dictified:
            d["data"] = {f"{i}": e for i, e in enumerate(d["data"])} | {
                "_target_": "list",
                "_id_": f"{uuid.uuid4()}",
            }

    hack_for_buildable_list = d.get("_target_", "").endswith("BuildableList")
    if hack_for_buildable_list:
        list_to_be_dictified = isinstance(d["data"], list)
        if list_to_be_dictified:
            d["data"] = {f"{i}": e for i, e in enumerate(d["data"])} | {
                "_target_": "list",
                "_id_": f"{uuid.uuid4()}",
            }

    node_from, dependencies = build_node(d, dc_only=dc_only)

    def is_good_dep(couldbedep):
        not_none = couldbedep is not None
        blacklisted = (
            isinstance(couldbedep, dict)
            and couldbedep.get("_target_", "").split(".")[-1] in CLASSES_BLACKLIST
        )
        # is_dataclass = (
        #     isinstance(couldbedep, dict) and "_target_" in couldbedep.keys()
        #     or dataclasses.is_dataclass(couldbedep)
        # )
        return not_none and not blacklisted

    good_deps = [(k, d[k]) for k in dependencies if is_good_dep(d[k])]
    for k, v in good_deps:
        node_to, _ = build_node(v, dc_only=dc_only)
        triple = node_from, k, node_to
        triple_id = "-".join([f"{x}" for x in triple])
        if triple_id not in set_of_triple_ids and node_to is not None:
            yield triple
            set_of_triple_ids.append(triple_id)
            if isinstance(v, dict):
                yield from generate_mermaid_triples(v, set_of_triple_ids)


def process_node_name(n):
    return n.replace("__main__.", "")


@beartype
def write_dataclass_to_mermaid(
    file: str, o: Dataclass, additional_skipkeys: Optional[list[str]] = None
):
    flow_chart = mermaid_flowchart(o, additional_skipkeys)
    write_file(file, f"```mermaid\n\n{flow_chart}```")


@beartype
def mermaid_flowchart(
    o: Dataclass, additional_skipkeys: Optional[list[str]] = None
) -> str:
    skip_keys = ["cache_dir", "cache_base"]
    if additional_skipkeys is not None:
        skip_keys += additional_skipkeys

    d = encode_dataclass(o, skip_keys=skip_keys)
    flow_chart = dict_to_mermaid(d)
    return flow_chart


def dict_to_mermaid(d):
    edges = "\n".join(
        [
            f"{node_from} --> | {param_name} | {node_to}"
            for node_from, param_name, node_to in generate_mermaid_triples(d)
        ]
    )
    flow_chart = f"flowchart TD\n\n{edges}\n"
    return flow_chart

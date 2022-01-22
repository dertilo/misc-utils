import json
import uuid
from dataclasses import dataclass
from pprint import pprint
from typing import Union, Any, Optional, Iterator

from beartype import beartype

from data_io.readwrite_files import write_lines, write_file
from misc_utils.dataclass_utils import (
    MyCustomEncoder,
    serialize_dataclass,
    encode_dataclass,
)


@dataclass
class AnotherTestDataClass:
    data: float = 1.0


@dataclass
class TestDataClass:
    data: AnotherTestDataClass


@dataclass
class Node:
    id: str
    full_module_name: str
    params: Optional[Any] = None

    @property
    def class_name(self):
        return self.full_module_name.split(".")[-1]

    def __repr__(self):
        if self.params is None:
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


def build_node(x: Any):
    if isinstance(x, dict):
        d = x
        params = [
            k
            for k, v in d.items()
            if "_target_" not in json.dumps(v) and k not in ["_target_", "_id_"]
        ]

        dependencies = [
            k for k, v in d.items() if k not in params and k not in ["_target_", "_id_"]
        ]

        if "_target_" in x.keys():
            node = Node(str(x["_id_"]), x["_target_"], params={k: d[k] for k in params})
        else:
            node = Node(str(id(x)), "dict", params={k: d[k] for k in params})
    else:
        # uuid cause I don't want builtin object to be concentrated in single node
        node = Node(f"{uuid.uuid1()}", type(x).__name__, params=x)
        dependencies = []
    return node, dependencies


@beartype
def generate_mermaid_triples(
    d: dict, set_of_triple_ids: Optional[list[str]] = None
) -> Iterator[tuple[Node, str, Node]]:
    if set_of_triple_ids is None:
        set_of_triple_ids = []

    hack_for_BuildableContainer = d.get("_target_", "").endswith("BuildableContainer")
    if hack_for_BuildableContainer:
        list_to_be_dictified = isinstance(d["data"], list)
        if list_to_be_dictified:
            d["data"] = {f"{i}": e for i, e in enumerate(d["data"])} | {
                "_target_": "list",
                "_id_": f"1234",
            }

    node_from, dependencies = build_node(d)
    for k in dependencies:
        v = d[k]
        node_to, _ = build_node(v)
        triple = node_from, k, node_to
        triple_id = "-".join([f"{x}" for x in triple])
        if triple_id not in set_of_triple_ids:
            yield triple
            set_of_triple_ids.append(triple_id)
            if isinstance(v, dict):
                yield from generate_mermaid_triples(v, set_of_triple_ids)


def process_node_name(n):
    return n.replace("__main__.", "")


if __name__ == "__main__":
    o = TestDataClass(data=AnotherTestDataClass())
    d = encode_dataclass(o)
    edges = "\n".join(
        [
            f"{node_from} --> | {param_name} | {node_to}"
            for node_from, param_name, node_to in generate_mermaid_triples(d)
        ]
    )
    write_file("diagram.md", f"```mermaid\n\nflowchart LR\n\n{edges}\n```")

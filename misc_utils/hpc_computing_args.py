# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass


@dataclass
class ClusterArgs:
    num_nodes: int = 1
    num_gpus: int = 1
    output_dir: str = "some-output_dir"


@dataclass
class HPCArgs(ClusterArgs):
    run_name: str = "debug"
    env_dir: str = "/some_where/foo/pypoetry/virtualenvs/bla_-py3.9"  #
    code_src_dir: str = "some-base-dir"
    num_processed_per_node: int = 8
    mem_in_gb: int = 60
    max_wall_time_hours: int = 24 * 7

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
    conda_env_name: str = "hfwav2vev2finetune"
    code_src_dir: str = "some-base-dir"
    num_processed_per_node: int = 8
    mem_in_gb: int = 60
    max_wall_time_hours: int = 24 * 7

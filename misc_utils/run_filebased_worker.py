import sys

from misc_utils.prefix_suffix import BASE_PATHES, PrefixSuffix
from misc_utils.filelock_queuing import FileBasedWorker

if __name__ == "__main__":

    base_path = sys.argv[1]
    queue_dir = sys.argv[2]
    run_name = sys.argv[3] if len(sys.argv) > 3 else "noname"
    wandb_project_name = sys.argv[4] if len(sys.argv) > 4 else None

    BASE_PATHES["base_path"] = base_path
    cache_root = f"{base_path}/data/cache"
    BASE_PATHES["cache_root"] = cache_root
    BASE_PATHES["nix"] = ""
    FileBasedWorker(
        run_name=run_name,
        # TODO: why PrefixSuffix here?
        queue_dir=PrefixSuffix("nix", queue_dir),
        wandb_project=wandb_project_name,
    ).run()

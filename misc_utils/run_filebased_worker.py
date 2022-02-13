import sys

from misc_utils.prefix_suffix import BASE_PATHES, PrefixSuffix
from misc_utils.filelock_queuing import FileBasedWorker

if __name__ == "__main__":

    base_path = sys.argv[1]  # /nfs-storage
    queue_dir = sys.argv[2]

    BASE_PATHES["base_path"] = base_path
    cache_root = f"{base_path}/data/cache"
    BASE_PATHES["cache_root"] = cache_root
    BASE_PATHES["nix"] = ""
    FileBasedWorker(
        # TODO: why PrefixSuffix here?
        queue_dir=PrefixSuffix("nix", queue_dir),
    ).run()

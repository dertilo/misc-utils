import random

from time import sleep
from typing import Optional

import sys
from pathlib import Path

from data_io.readwrite_files import write_file
from misc_utils.filelock_queuing import consume_file

if __name__ == "__main__":

    """
    tested with 2 machines on NFS seems to be working! could also be simply luck! no collisions due to random sleep
    """

    path = sys.argv[1]
    name = sys.argv[2]

    print([str(p) for p in Path(path).glob("*.txt")])

    def get_file() -> Optional[str]:
        sleep(random.uniform(0, 1))
        files = [str(p) for p in Path(path).glob("*.txt")]
        if len(files) > 0:
            return files[0]
        else:
            return None

    while True:
        file, test_datum = consume_file(get_file, break_if_no_file=False)
        write_file(f"{path}/{name}_{Path(file).stem}.done", test_datum)

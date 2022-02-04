import os
import random

import sys

import filelock
from filelock import FileLock
from time import time, sleep

from data_io.readwrite_files import write_file, read_file
from misc_utils.processing_utils import process_with_threadpool


def free_to_write(
    file, fail_message=f"could not claim lock, some-one else is holding it"
) -> bool:
    claimed_rights_to_write = False
    while not os.path.isfile(f"{file}.lock") and not os.path.isfile(file):
        try:
            with FileLock(f"{file}.lock", timeout=1):
                claimed_rights_to_write = True
                break
        except filelock._error.Timeout:
            sys.stdout.write(fail_message)
            sys.stdout.flush()
            fail_message = "."
    return claimed_rights_to_write
    # os.remove(f"{file}.lock")


def main():
    start = time()
    file = "test.txt"

    def compete_writing(k):
        delay = random.uniform(0, 1)
        sleep(delay)
        if free_to_write(file, f"{k}-is waiting"):
            o=f"{k} wrote"
            sleep(3)
            write_file(file,o)
            os.remove(f"{file}.lock")
        else:
            while os.path.isfile(f"{file}.lock"):
                sleep(1)
            o=read_file(file)
        return f"{k}-{delay=}: {o}"

    data = [k for k in range(3)]
    print(list(process_with_threadpool(data, compete_writing, 3)))


if __name__ == "__main__":
    main()

import os
import random

from time import sleep

from data_io.readwrite_files import write_file, read_file
from misc_utils.processing_utils import process_with_threadpool
from misc_utils.utils import claim_write_access


def main():
    file = "test.txt"

    def compete_writing(k):
        delay = random.uniform(0, 1)
        sleep(delay)
        if claim_write_access(file, f"{k}-is waiting"):
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

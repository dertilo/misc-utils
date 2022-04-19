import multiprocessing
import os
from pathlib import Path
from time import sleep

import filelock
from filelock import FileLock

from data_io.readwrite_files import write_file


def claim_write_access(file_or_dir: str) -> bool:
    """
    if false, someone else already claimed it!
    """
    claimed_rights_to_write = False
    lock_file = f"{file_or_dir}.lock"
    lock_lock_file = f"{file_or_dir}.lock.lock"
    lock_path = Path(lock_file).parent
    lock_path.mkdir(parents=True, exist_ok=True)
    me = multiprocessing.current_process().name

    def already_existent():
        return os.path.isfile(file_or_dir) or os.path.isdir(file_or_dir)

    while not os.path.isfile(lock_file) and not already_existent():
        try:
            filelock_timeout = 0.1
            with FileLock(lock_file, timeout=filelock_timeout):
                # print(f"{me=}: {datetime.now()}")
                # TODO: not working like this!
                # even though I manually force flushing, still seems to be some buffering/delay in writing to the file, -> writing to file not "real-time", not thread-safe
                # if len(read_file(lock_file))==0:
                #     write_file(lock_file, me,do_flush=True) #
                #     # sleep(1.0)
                #     claimed_rights_to_write=True
                #     print(f"{me=}: {datetime.now()}")

                if not os.path.isfile(lock_lock_file) and not already_existent():
                    write_file(lock_lock_file, me)
                    sleep(2 * filelock_timeout)  # enforce other FileLocks to time-out!
                    claimed_rights_to_write = True
                break
        except filelock._error.Timeout:
            # pass
            print("retry claim_write_access")
            # sys.stdout.write(fail_message)
            # sys.stdout.flush()
            # fail_message = "."
    return claimed_rights_to_write

import hashlib
from typing import List


# BUF_SIZE is totally arbitrary, change for your app!
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


def hash_file(file, hash_algo="sha1", buf_size=BUF_SIZE):
    """
    see: https://stackoverflow.com/questions/22058048/hashing-a-file-in-python
    :return: sha1, md5 or whatever supported by hashlib
    """

    hashh = getattr(hashlib, hash_algo)()

    with open(file, "rb") as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            hashh.update(data)

    return hashh.hexdigest()


def hash_list_of_strings(l: list[str]):
    return hashlib.sha1("_".join(l).encode("utf-8")).hexdigest()

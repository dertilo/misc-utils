import re

import os
from typing import Optional, Callable

from beartype import beartype

from misc_utils.processing_utils import exec_command


@beartype
def download_data(
    base_url: str,
    file_name: str,
    data_dir: str,
    verbose: bool = False,
    unzip_it: bool = False,
    do_raise: bool = True,
    remove_zipped: bool = False,
) -> Optional[str]:
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)

    url = base_url + "/" + file_name
    file = data_dir + "/" + file_name

    try:
        if unzip_it:
            suffixes = [".zip", ".ZIP", ".tar.gz", ".tgz", ".gz", ".GZ", ".tar", ".TAR"]
            regex = r"|".join([f"(?:{s})" for s in suffixes])
            extract_folder = re.sub(regex, "", file)
            assert extract_folder != file

            if not os.path.isdir(extract_folder):
                wget_file(url, data_dir, verbose)
                os.makedirs(extract_folder, exist_ok=True)
                extract_file(file, extract_folder, get_build_extract_command_fun(file))
                if remove_zipped:
                    os.remove(file)
            return extract_folder
        else:
            if not os.path.isfile(file):
                wget_file(url, data_dir, verbose)
    except FileNotFoundError as e:
        if do_raise:
            raise e


def get_build_extract_command_fun(file: str):
    if any(file.endswith(suf) for suf in [".zip", ".ZIP"]):

        def fun(dirr, file):
            return f"unzip -d {dirr} {file}"

    elif any(file.endswith(suf) for suf in [".tar.gz", ".tgz"]):

        def fun(dirr, file):
            return f"tar xzf {file} -C {dirr}"

    elif any(file.endswith(suf) for suf in [".tar", ".TAR"]):

        def fun(dirr, file):
            return f"tar xf {file} -C {dirr}"

    elif any(file.endswith(suf) for suf in [".gz", ".GZ"]):

        def fun(dirr, file):
            return f"gzip -dc {file} {dirr}"

    else:
        raise NotImplementedError
    return fun


def extract_file(file, extract_folder, build_extract_command_fun: Callable):
    cmd = build_extract_command_fun(extract_folder, file)
    _, stderr = exec_command(cmd)
    assert len(stderr) == 0, f"{cmd=}: {stderr=}"


@beartype
def wget_file(
    url: str,
    data_folder: str,
    verbose=False,
    user: Optional[str] = None,
    password: Optional[str] = None,
):
    # TODO(tilo): wget.download cannot continue ??
    passw = f" --password {password} " if password is not None else ""
    user = f' --user "{user}" ' if user is not None else ""
    quiet = " -q " if not verbose else ""
    file_name = url.split("/")[-1]
    file = f"{data_folder}/{file_name}"
    if os.path.isfile(file):
        cmd = f"wget -O {file} -c -N{quiet}{passw}{user} -P {data_folder} {url}"
    else:
        cmd = f"wget -O {file} -c {quiet}{passw}{user} -P {data_folder} {url}"

    print(f"{cmd=}")
    os.system(cmd)
    # TODO: why is subprocess not working?
    # download_output = exec_command(cmd)
    # if err_code != 0:
    #     raise FileNotFoundError(f"could not download {url}")


def main():
    file_name = "/test-other.tar.gz"
    base_url = "http://www.openslr.org/resources/12"
    download_data(base_url, file_name, "/tmp/test_data", unzip_it=True, verbose=True)


if __name__ == "__main__":
    main()

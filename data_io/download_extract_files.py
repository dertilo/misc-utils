import re

import os
from typing import Optional

from beartype import beartype


@beartype
def download_data(
    base_url: str,
    file_name: str,
    data_folder: str,
    verbose: bool = False,
    unzip_it: bool = False,
    do_raise: bool = True,
    remove_zipped: bool = False,
):
    if not os.path.exists(data_folder):
        os.makedirs(data_folder, exist_ok=True)

    url = base_url + "/" + file_name
    file = data_folder + "/" + file_name

    def extract(extract_folder, file, build_command):
        assert os.system(build_command(extract_folder, file)) == 0

    try:
        if unzip_it:
            suffixes = [".zip", ".ZIP", ".tar.gz", ".tgz", ".gz", ".GZ", ".tar", ".TAR"]
            regex = r"|".join([f"(?:{s})" for s in suffixes])
            extract_folder = re.sub(regex, "", file)
            assert extract_folder != file

            if any(file.endswith(suf) for suf in [".zip", ".ZIP"]):

                def build_command(dirr, file):
                    return f"unzip -d {dirr} {file}"

            elif any(file.endswith(suf) for suf in [".tar.gz", ".tgz"]):

                def build_command(dirr, file):
                    return f"tar xzf {file} -C {dirr}"

            elif any(file.endswith(suf) for suf in [".tar", ".TAR"]):

                def build_command(dirr, file):
                    return f"tar xf {file} -C {dirr}"

            elif any(file.endswith(suf) for suf in [".gz", ".GZ"]):

                def build_command(dirr, file):
                    return f"gzip -dc {file} {dirr}"

            else:
                raise NotImplementedError

            if not os.path.isdir(extract_folder):
                wget_file(url, data_folder, verbose)
                os.makedirs(extract_folder, exist_ok=True)
                extract(extract_folder, file, build_command)
                if remove_zipped:
                    os.remove(file)

        else:
            if not os.path.isfile(file):
                wget_file(url, data_folder, verbose)
    except FileNotFoundError as e:
        if do_raise:
            raise e


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
    cmd = f"wget -c -N{quiet}{passw}{user}-P {data_folder} {url}"
    print(f"{cmd=}")
    err_code = os.system(cmd)
    if err_code != 0:
        raise FileNotFoundError(f"could not download {url}")


def main():
    file_name = "/test-other.tar.gz"
    base_url = "http://www.openslr.org/resources/12"
    download_data(base_url, file_name, "/tmp/test_data", unzip_it=True, verbose=True)


if __name__ == "__main__":
    main()

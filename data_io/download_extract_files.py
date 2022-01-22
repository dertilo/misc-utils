import re

import os


def download_data(
    base_url,
    file_name,
    data_folder,
    verbose=False,
    unzip_it=False,
    do_raise=True,
    remove_zipped=False,
):
    if not os.path.exists(data_folder):
        os.makedirs(data_folder, exist_ok=True)

    url = base_url + "/" + file_name
    file = data_folder + "/" + file_name

    def extract(extract_folder, file, build_command):
        assert os.system(build_command(extract_folder, file)) == 0

    try:
        if unzip_it:
            suffixes = [".zip", ".ZIP", ".tar.gz", ".tgz", ".gz", ".GZ"]
            regex = r"|".join([f"(?:{s})" for s in suffixes])
            extract_folder = re.sub(regex, "", file)

            if any(file.endswith(suf) for suf in [".zip", ".ZIP"]):

                def build_command(dirr, file):
                    return f"unzip -d {dirr} {file}"

            elif any(file.endswith(suf) for suf in [".tar.gz", ".tgz"]):

                def build_command(dirr, file):
                    return f"tar xzf {file} -C {dirr}"

            elif any(file.endswith(suf) for suf in [".gz", ".GZ"]):

                def build_command(dirr, file):
                    return f"gzip -dc {file} {dirr}"

            else:
                raise NotImplementedError

            if not os.path.isdir(extract_folder):
                wget_file(url, data_folder, verbose)
                assert os.system(f"mkdir {extract_folder}") == 0
                extract(extract_folder, file, build_command)
                if remove_zipped:
                    os.remove(file)

        else:
            if not os.path.isfile(file):
                wget_file(url, data_folder, verbose)
    except FileNotFoundError as e:
        if do_raise:
            raise e


def wget_file(url, data_folder, verbose=False):
    # TODO(tilo): wget.download cannot continue ??
    err_code = os.system(
        f"wget -c -N{' -q' if not verbose else ''} -P {data_folder} {url}"
    )
    if err_code != 0:
        raise FileNotFoundError(f"could not downloaded {url.split('/')[-1]}")


def main():
    file_name = "/test-other.tar.gz"
    base_url = "http://www.openslr.org/resources/12"
    download_data(base_url, file_name, "/tmp/test_data", unzip_it=True, verbose=True)


if __name__ == "__main__":
    main()

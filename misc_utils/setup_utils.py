import os

SKIP_THIS = None

GIT_REPO_URL = "GIT_REPO_URL"
GIT_REPO_URL_PLACEHOLDER = f"${{{GIT_REPO_URL}}}"


def git_repo_url(s: str):

    if GIT_REPO_URL_PLACEHOLDER in s:
        if GIT_REPO_URL in os.environ:
            git_repo_url = os.environ[GIT_REPO_URL]
            assert git_repo_url.startswith("https://"), git_repo_url
            assert git_repo_url.endswith(".git"), git_repo_url
            o = s.replace(GIT_REPO_URL_PLACEHOLDER, git_repo_url)
        else:
            o = SKIP_THIS
    else:
        o = s
    return o


def build_install_requires():
    with open("requirements.txt") as f:
        reqs = f.read()

    reqs = [git_repo_url(s) for s in reqs.strip().split("\n") if not s.startswith("#")]
    reqs = list(filter(lambda x: x is not SKIP_THIS, reqs))
    assert all((GIT_REPO_URL_PLACEHOLDER not in r for r in reqs))
    return reqs

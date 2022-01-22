from setuptools import find_packages
from setuptools import setup
from misc_utils.setup_utils import (
    build_install_requires,
)  # is this creepy? that it needs its own code to build?

with open("requirements.txt") as f:
    reqs = f.read()

with open("README.md") as f:
    readme = f.read()

setup(
    name="misc_utils",
    version="0.1",
    packages=find_packages(),
    license="MIT License",
    long_description=readme,
    install_requires=build_install_requires(),
)

from codecs import open
from os import path

import pfsim

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")

install_requires = [x.strip() for x in all_reqs if "git+" not in x]
dependency_links = [x.strip().replace("git+", "") for x in all_reqs
                    if x.startswith("git+")]

setup(
    name="pfsim",
    version=pfsim.__VERSION__,
    description="A packet flow simulator for dynamically reconfigurable"
                " interconnects",
    long_description=long_description,
    url="https://github.com/shimojo-lab/pfsim",
    license="MIT",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    keywords="",
    packages=find_packages(exclude=["docs", "tests*"]),
    entry_points={
        "console_scripts": [
            "pfsim = pfsim.__init__:main"
        ],
    },
    author="Keichi Takahashi",
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email="keichi.t@me.com"
)

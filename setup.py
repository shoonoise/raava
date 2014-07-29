#!/usr/bin/env python


import setuptools


##### Main #####
if __name__ == "__main__":
    setuptools.setup(
        name="raava",
        version="0.18",
        url="https://github.com/yandex-sysmon/raava",
        license="GPLv3",
        author="Devaev Maxim",
        author_email="mdevaev@gmail.com",
        description="Distributed events processor, based on stackless technology of PyPy3",
        platforms="any",

        packages=[
            "raava",
        ],

        classifiers=[ # http://pypi.python.org/pypi?:action=list_classifiers
            "Development Status :: 2 - Pre-Alpha",
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
            "Operating System :: POSIX :: Linux",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: System :: Distributed Computing",
        ],

        install_requires=[
            "kazoo.yandex >= 2.0.1.1",
            "contextlog >= 0.2",
        ],
    )

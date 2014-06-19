#!/usr/bin/env python


import setuptools
import raava.const


##### Main #####
if __name__ == "__main__":
    setuptools.setup(
        name="raava",
        version=raava.const.VERSION,
        url=raava.const.UPSTREAM_URL,
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
            "kazoo >= 1.3.1",
        ],
        dependency_links=[
            "https://github.com/mdevaev/kazoo/archive/invoke-timeout.zip#egg=kazoo-2.0",
        ]
    )

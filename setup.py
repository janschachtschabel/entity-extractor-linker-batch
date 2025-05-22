#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="entityextractor",
    version="1.0.0",
    author="Jan Schachtschabel",
    author_email="jan.schachtschabel@example.com",
    description="A tool for extracting, generating and linking entities to knowledge bases with relationship inference",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/janschachtschabel/entity-extractor-linker",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'entityextractor': ['cache/*', 'cache/README.md'],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "entityextractor=entityextractor.main:main",
        ],
    },
)

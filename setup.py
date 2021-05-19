#!/usr/bin/env python3
"""BEMServer core"""

from setuptools import setup, find_packages

# Get the long description from the README file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bemserver-core",
    version="0.1",
    description="BEMServer core",
    long_description=long_description,
    # url="",
    author="Nobatek/INEF4",
    author_email="jlafrechoux@nobatek.com",
    # license="",
    # keywords=[
    # ],
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    install_requires=[
        "python-dotenv>=0.9.0",
        "psycopg2>=2.8.0",
        "sqlalchemy>=1.4.0",
        "pandas>=1.2.3",
        "passlib>=1.7.4",
        "argon2_cffi>=20.1.0",
    ],
    packages=find_packages(exclude=["tests*"]),
)

#!/usr/bin/env python3
"""BEMServer core"""

from setuptools import setup, find_packages


# Get the long description from the README file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bemserver-core",
    version="0.0.1",
    description="BEMServer core",
    long_description=long_description,
    url="https://github.com/BEMServer/bemserver-core",
    author="Nobatek/INEF4",
    author_email="jlafrechoux@nobatek.inef4.com",
    license="AGPLv3+",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        (
            "License :: OSI Approved :: "
            "GNU Affero General Public License v3 or later (AGPLv3+)"
        ),
    ],
    python_requires=">=3.7",
    install_requires=[
        "psycopg2>=2.8.0",
        "sqlalchemy>=1.4.0",
        "pandas>=1.2.3",
        "passlib>=1.7.4",
        "argon2_cffi>=20.1.0",
        "oso>=0.26.0,<0.27",
        "alembic>=1.7.7",
        "click>=8.1.3",
    ],
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "bemserver_setup_db = bemserver_core.commands:setup_db_cmd",
            "bemserver_create_user = bemserver_core.commands:create_user_cmd",
        ],
    },
)

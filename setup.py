#!/usr/bin/env python3
"""BEMServer core"""

from setuptools import setup, find_packages


# Get the long description from the README file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bemserver-core",
    version="0.5.0",
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
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        (
            "License :: OSI Approved :: "
            "GNU Affero General Public License v3 or later (AGPLv3+)"
        ),
    ],
    python_requires=">=3.9",
    install_requires=[
        "psycopg2>=2.9.0",
        "sqlalchemy>=1.4.0",
        "pandas>=1.5.0",
        "passlib>=1.7.4",
        "argon2_cffi>=21.3.0",
        "oso>=0.26.0,<0.27",
        "alembic>=1.8.0",
        "click>=8.1.3",
        "celery>=5.2.0",
        "redis>=4.3.4",
    ],
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "bemserver_setup_db = bemserver_core.commands:setup_db_cmd",
            "bemserver_create_user = bemserver_core.commands:create_user_cmd",
            "bemserver_db_current = bemserver_core.commands:db_current_cmd",
            "bemserver_db_upgrade = bemserver_core.commands:db_upgrade_cmd",
            "bemserver_db_downgrade = bemserver_core.commands:db_downgrade_cmd",
        ],
    },
)

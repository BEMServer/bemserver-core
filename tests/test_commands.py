"""Commands tests"""
import os
import subprocess

import sqlalchemy as sqla


class TestCommands:
    def test_setup_db_cmd(self, timescale_db):
        """Check bemserver_setup_db runs without error

        Also check at least one table is created
        """
        with sqla.create_engine(timescale_db).connect() as connection:
            assert not list(
                connection.execute(
                    "select table_name from information_schema.tables "
                    "where table_schema='public';"
                )
            )

        env = {**os.environ, "SQLALCHEMY_DATABASE_URI": timescale_db}
        proc = subprocess.run(["bemserver_setup_db"], env=env, stdout=subprocess.PIPE)
        assert proc.returncode == 0

        with sqla.create_engine(timescale_db).connect() as connection:
            assert list(
                connection.execute(
                    "select * from information_schema.tables "
                    "where table_schema='public';"
                )
            )

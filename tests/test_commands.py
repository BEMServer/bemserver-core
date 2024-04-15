"""Commands tests"""

import pytest

import sqlalchemy as sqla

from click.testing import CliRunner

from bemserver_core.commands import create_user_cmd, setup_db_cmd


class TestCommands:
    @pytest.mark.usefixtures("config")
    def test_setup_db_cmd(self, postgresql_db):
        """Check bemserver_setup_db runs without error

        Also check at least one table is created
        """

        # Check there are no tables in DB
        with sqla.create_engine(postgresql_db).connect() as connection:
            assert not list(
                connection.execute(
                    sqla.text(
                        "select table_name from information_schema.tables "
                        "where table_schema='public';"
                    )
                )
            )

        # Run command
        runner = CliRunner()
        result = runner.invoke(setup_db_cmd)
        assert result.exit_code == 0

        # Check tables are created
        with sqla.create_engine(postgresql_db).connect() as connection:
            assert list(
                connection.execute(
                    sqla.text(
                        "select * from information_schema.tables "
                        "where table_schema='public';"
                    )
                )
            )

    @pytest.mark.usefixtures("bemservercore")
    def test_create_user_cmd(self, database):
        # Check there is no user in DB
        with sqla.create_engine(database).connect() as connection:
            assert not list(connection.execute(sqla.text("select * from users;")))

        # Run command
        runner = CliRunner()
        result = runner.invoke(
            create_user_cmd,
            [
                "--name",
                "Chuck",
                "--email",
                "chuck@test.com",
                "--admin",
                "--inactive",
            ],
            input="p@ssword\np@ssword\n",
        )
        assert result.exit_code == 0

        # Check user is created
        with sqla.create_engine(database).connect() as connection:
            assert list(connection.execute(sqla.text("select * from users;")))

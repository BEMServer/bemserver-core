"""Commands tests"""
import sqlalchemy as sqla
from click.testing import CliRunner

from bemserver_core.commands import create_user_cmd, setup_db_cmd


class TestCommands:
    def test_setup_db_cmd(self, timescale_db):
        """Check bemserver_setup_db runs without error

        Also check at least one table is created
        """

        # Check there are no tables in DB
        with sqla.create_engine(timescale_db).connect() as connection:
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
        result = runner.invoke(
            setup_db_cmd,
            env={"SQLALCHEMY_DATABASE_URI": timescale_db},
        )
        assert result.exit_code == 0

        # Check tables are created
        with sqla.create_engine(timescale_db).connect() as connection:
            assert list(
                connection.execute(
                    sqla.text(
                        "select * from information_schema.tables "
                        "where table_schema='public';"
                    )
                )
            )

    def test_create_user_cmd(self, database, bemservercore):

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
            env={"SQLALCHEMY_DATABASE_URI": database},
        )
        assert result.exit_code == 0

        # Check user is created
        with sqla.create_engine(database).connect() as connection:
            assert list(connection.execute(sqla.text("select * from users;")))

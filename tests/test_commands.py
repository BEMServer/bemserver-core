"""Commands tests"""
import sqlalchemy as sqla

from click.testing import CliRunner

from bemserver_core.commands import setup_db_cmd


class TestCommands:
    def test_setup_db_cmd(self, timescale_db):
        """Check bemserver_setup_db runs without error

        Also check at least one table is created
        """

        # Check there are no tables in DB
        with sqla.create_engine(timescale_db).connect() as connection:
            assert not list(
                connection.execute(
                    "select table_name from information_schema.tables "
                    "where table_schema='public';"
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
                    "select * from information_schema.tables "
                    "where table_schema='public';"
                )
            )

from textwrap import dedent

import sqlalchemy as sa


def gen_ddl_trigger_ro(table_name, col_name):
    return sa.DDL(
        dedent(
            f"""
            CREATE TRIGGER
                {table_name}_trigger_update_readonly_{col_name}
            BEFORE UPDATE
                OF {col_name} ON {table_name}
            FOR EACH ROW
                WHEN (
                    NEW.{col_name} IS DISTINCT FROM OLD.{col_name}
                )
                EXECUTE FUNCTION column_update_not_allowed({col_name});
            """
        )
    )

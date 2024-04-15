"""Databases: SQLAlchemy database access"""

from functools import wraps
from itertools import chain
from textwrap import dedent

import sqlalchemy as sqla
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

# https://alembic.sqlalchemy.org/en/latest/naming.html
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


SESSION_FACTORY = sessionmaker()
DB_SESSION = scoped_session(SESSION_FACTORY)


class Base(DeclarativeBase):
    """Custom base class"""

    metadata = sqla.MetaData(naming_convention=NAMING_CONVENTION)

    @classmethod
    def _query(cls, **kwargs):
        return db.session.query(cls).filter_by(**kwargs)

    @classmethod
    def _apply_sort_query_filter(cls, query, sort_field, *, nulls_last=False):
        """Add order_by instruction to query.

        sort_field is a field name, optionally prefixed with "+" or "-".
            No prefix is equivalent to "+", which means "ascending" order.
        """
        if sort_field[0] == "-":
            sort_direction = sqla.desc
        else:
            sort_direction = sqla.asc
        if sort_field[0] in {"-", "+"}:
            sort_field = sort_field[1:]
        order = sort_direction(getattr(cls, sort_field))
        if nulls_last:
            order = sqla.nulls_last(order)
        return query.order_by(order)

    @classmethod
    def _add_sort_query_filter(cls, func):
        """Add sort argument to query function

        sort must be a list of fields to sort upon, by order of priority
            (the first field is the first sort key). Each field is a field
            name, optionally prefixed with "+" or "-". No prefix is equivalent
            to "+", which means "ascending" order.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            sort = kwargs.pop("sort", None)
            query = func(*args, **kwargs)
            if sort:
                for sort_field in sort:
                    query = cls._apply_sort_query_filter(query, sort_field)
            return query

        return wrapper

    @classmethod
    def _add_min_max_query_filters(cls, func):
        """Add min/max query filters feature to query function"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract min / max filters
            min_filters = {}
            max_filters = {}
            for key in list(kwargs.keys()):
                if key.endswith("_min"):
                    min_filters[key[:-4]] = kwargs.pop(key)
                elif key.endswith("_max"):
                    max_filters[key[:-4]] = kwargs.pop(key)

            # Base query
            query = func(*args, **kwargs)

            # Apply min / max filters
            for key, val in min_filters.items():
                query = query.filter(getattr(cls, key) >= val)
            for key, val in max_filters.items():
                query = query.filter(getattr(cls, key) <= val)

            return query

        return wrapper

    @classmethod
    def _add_in_field_search_query_filters(cls, func):
        """Add "in field search" feature to query function

        This is a "SQL like" filter, used to search if a string is contained in any of
        the column (field) values, insensitive case.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract "in field search" filters
            in_filters = {}
            for key in list(kwargs.keys()):
                if key.startswith("in_"):
                    in_filters[key[3:]] = kwargs.pop(key)

            # Base query
            query = func(*args, **kwargs)

            # Apply "in field search" filters
            for key, val in in_filters.items():
                query = query.filter(getattr(cls, key).ilike(f"%{val}%"))

            return query

        return wrapper

    @classmethod
    def _filter_bool_none_as_false(cls, query, field, value):
        """Filter query by boolean value where Null is considered False

        :param sqlalchemy.orm.Query query: Query to filter
        :param sqlalchemy.schema.Column: Field to filter on
        :param boolean value: Whether to get True or False/Null values

        Returns filtered query.
        """
        if value is True:
            return query.filter(field.is_(True))
        return query.filter(sqla.or_(field.is_(False), field.is_(None)))

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        return cls._add_sort_query_filter(
            cls._add_min_max_query_filters(
                cls._add_in_field_search_query_filters(cls._query)
            )
        )(**kwargs)

    @classmethod
    def new(cls, **kwargs):
        """Create new object"""
        item = cls(**kwargs)
        db.session.add(item)
        return item

    @classmethod
    def get_by_id(cls, item_id):
        """Get object by ID"""
        return db.session.get(cls, item_id)

    def update(self, **kwargs):
        """Update object with kwargs"""
        for key, value in kwargs.items():
            setattr(self, key, value)

    def delete(self):
        """Delete object"""
        db.session.delete(self)

    def _before_flush(self):
        """Hook executed before DB session flush"""


# Inspired by https://stackoverflow.com/a/36732359
@sqla.event.listens_for(DB_SESSION, "before_flush")
def receive_before_flush(session, flush_context, instances):
    """Listen for the `before_flush` event"""
    # Call _before_flush method for each modified object instance in session.
    for obj in chain(session.new, session.dirty):
        obj._before_flush()


class DBConnection:
    """Database accessor"""

    def __init__(self):
        self.engine = None

    def set_db_url(self, db_url):
        """Set DB URL"""
        self.engine = sqla.create_engine(
            db_url,
            # Set UTC for all connections
            connect_args={"options": "-c timezone=utc"},
        )
        SESSION_FACTORY.configure(bind=self.engine)
        # Remove any existing session from registry
        DB_SESSION.remove()

    @property
    def session(self):
        return DB_SESSION

    @property
    def url(self):
        return self.engine.url if self.engine else None

    def create_all(self, **kwargs):
        """Create all tables

        :param kwargs: Keyword parameters passed to metadata.create_all
        """
        Base.metadata.create_all(bind=self.engine, **kwargs)


db = DBConnection()


def init_db_functions():
    """Create functions for triggers...

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """

    # SQL function to raise an exception when trying to update a "read-only" column.
    db.session.execute(
        sqla.DDL(
            dedent(
                """\
                CREATE FUNCTION column_update_not_allowed()
                    RETURNS TRIGGER AS
                $func$
                    DECLARE
                        col_name text := TG_ARGV[0]::text;
                    BEGIN
                        RAISE EXCEPTION USING
                            MESSAGE = col_name || ' cannot be modified',
                            ERRCODE = 'integrity_constraint_violation';
                    END;
                $func$
                LANGUAGE plpgsql STRICT;\
                """
            )
        )
    )


def _generate_ddl_trigger_read_only(table_name, col_name):
    """Generate the SQL statement that creates an "update read-only trigger"
    on a specific column for a table.

    :param str table_name: The name of the table concerned by the trigger.
    :param str col_name: The name of the column to protect from update.
    :returns sqlalchemy.DDL: An instance of DDL statement that creates the trigger.
    """
    return sqla.DDL(
        dedent(
            f"""\
            CREATE TRIGGER
                {table_name}_trigger_update_readonly_{col_name}
            BEFORE UPDATE
                OF {col_name} ON {table_name}
            FOR EACH ROW
                WHEN (
                    NEW.{col_name} IS DISTINCT FROM OLD.{col_name}
                )
                EXECUTE FUNCTION column_update_not_allowed({col_name});\
            """
        )
    )


def make_columns_read_only(*fields):
    """Make table columns read-only

    :param list fields: List of model Columns
    """
    for field in fields:
        db.session.execute(
            _generate_ddl_trigger_read_only(field.class_.__table__, field.name)
        )

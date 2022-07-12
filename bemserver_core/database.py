"""Databases: SQLAlchemy database access"""
from functools import wraps
from itertools import chain

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


# https://alembic.sqlalchemy.org/en/latest/naming.html
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base:
    """Custom base class"""

    @classmethod
    def _query(cls, **kwargs):
        return db.session.query(cls).filter_by(**kwargs)

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
                for field in sort:
                    if field[0] == "-":
                        direction = sqla.desc
                    else:
                        direction = sqla.asc
                    if field[0] in {"-", "+"}:
                        field = field[1:]
                    query = query.order_by(direction(getattr(cls, field)))
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
        pass


SESSION_FACTORY = sessionmaker()
DB_SESSION = scoped_session(SESSION_FACTORY)
metadata = sqla.MetaData(naming_convention=NAMING_CONVENTION)
Base = declarative_base(metadata=metadata, cls=Base)


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
            future=True,
        )
        SESSION_FACTORY.configure(bind=self.engine)

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


# Inspired by https://dba.stackexchange.com/a/61304 for the use of `to_json` function.
def init_db_functions():
    """Create functions for triggers...

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """

    # SQL function to raise an exception when trying to update a "read-only" column.
    db.session.execute(
        sqla.DDL(
            """CREATE FUNCTION column_update_not_allowed()
    RETURNS TRIGGER AS
$func$
    DECLARE
        col_name text := TG_ARGV[0]::text;
        row_name text := to_json(OLD) ->> TG_ARGV[1];
        message_text text;
    BEGIN
        message_text := col_name || ' cannot be modified';
        IF row_name IS NOT NULL THEN
            message_text := message_text || ' for "' || row_name || '"';
        END IF;

        RAISE EXCEPTION USING
            MESSAGE = message_text,
            ERRCODE = 'integrity_constraint_violation';
    END;
$func$
LANGUAGE plpgsql STRICT;"""
        )
    )
    db.session.commit()

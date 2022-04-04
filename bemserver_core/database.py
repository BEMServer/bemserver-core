"""Databases: SQLAlchemy database access"""
from functools import wraps

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


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
    def get(cls, **kwargs):
        """Get objects"""
        return cls._add_sort_query_filter(cls._add_min_max_query_filters(cls._query))(
            **kwargs
        )

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


SESSION_FACTORY = sessionmaker()
DB_SESSION = scoped_session(SESSION_FACTORY)
Base = declarative_base(cls=Base)


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

    def create_all(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)

    def dispose(self):
        self.engine.dispose()
        self.engine = None


db = DBConnection()

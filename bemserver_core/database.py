"""Databases: SQLAlchemy database access"""
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


class Base:
    """Custom base class"""

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        query = db.session.query(cls).filter_by(**kwargs)
        return query

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


SESSION_FACTORY = sessionmaker(autocommit=False, autoflush=False)
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

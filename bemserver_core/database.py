"""Databases: SQLAlchemy database access"""

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


SESSION_FACTORY = sessionmaker(autocommit=False, autoflush=False)
DB_SESSION = scoped_session(SESSION_FACTORY)
Base = declarative_base()


class DBConnection:
    """Database accessor"""

    def __init__(self):
        self.engine = None

    def set_db_url(self, db_url):
        """Set DB URL"""
        self.engine = sqla.create_engine(db_url, future=True)
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


class BaseMixin:

    @property
    def _db_state(self):
        return sqla.orm.util.object_state(self)

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def _verify_consistency(self):
        return True

    def _make_transient(self):
        sqla.orm.make_transient(self)

    def save(self, *, refresh=False, raise_errors=True):
        """Write the data to the database.

        :param bool refresh: (optional, default False)
            Force to refresh this object data after commit.
        :param bool raise_errors: (optional, default True)
            Raise catched errors if True. Else errors are silently ignored.
        """
        self._verify_consistency()
        # This object was deleted and is detached from session.
        if self._db_state.was_deleted:
            # Set the object transient (session rollback of the deletion).
            self._make_transient()
        db.session.add(self)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError as exc:
            db.session.rollback()
            if raise_errors:
                raise exc
        else:
            if refresh:
                db.session.refresh(self)

    def delete(self, *, raise_errors=True):
        """Delete the item from the database.

        :param bool raise_errors: (optional, default True)
            Raise catched errors if True. Else errors are silently ignored.
        """
        # Verfify that object is not deleted yet to avoid a warning.
        if not self._db_state.was_deleted:
            db.session.delete(self)
            try:
                db.session.commit()
            except sqla.exc.IntegrityError as exc:
                db.session.rollback()
                if raise_errors:
                    raise exc

    @classmethod
    def get_by_id(cls, args, *, raise_errors=True):
        """Find an item stored in database, by its unique ID.

        The ID can be single or composed (multiple foreign keys).

        :param args: Unique ID of the item to find.
        :param bool raise_errors: (optional, default True)
            Raise catched errors if True. Else errors are silently ignored.
        :returns cls: Instance of item found.
        """
        try:
            return db.session.get(cls, args)
        except (sqla.orm.exc.NoResultFound,
                sqla.orm.exc.ObjectDeletedError,) as exc:
            if raise_errors:
                raise exc
        return None

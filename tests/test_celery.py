"""Celery task manager tests"""
import pytest

from bemserver_core.model import User
from bemserver_core.authorization import auth, get_current_user, OPEN_BAR
from bemserver_core.celery import BEMServerCoreCelery, BEMServerCoreTask
from bemserver_core.exceptions import BEMServerCoreError


# Dummy DB URI. Enough to pass engine creation. Faster than DB creation fixture.
DUMMY_DB_URI = "sqlite+pysqlite:///:memory:"


class TestCelery:
    def test_celery_base_task_open_bar(self, monkeypatch):
        """Check base task authorizations

        Check authorizations are set on task init
        Check task runs with all permissions (OpenBar mode)
        """
        monkeypatch.setenv("SQLALCHEMY_DATABASE_URI", DUMMY_DB_URI)

        celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)

        @celery.task
        def dummy_task():
            assert OPEN_BAR.get()
            auth.authorized_query(get_current_user(), "read", User)

        dummy_task()

    def test_celery_base_task_db_uri_not_set(self):
        """Check tasks raises if DB URI not set"""

        celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)

        @celery.task
        def dummy_task():
            pass

        with pytest.raises(
            BEMServerCoreError,
            match="SQLALCHEMY_DATABASE_URI environment variable not set",
        ):
            celery.finalize()

    def test_celery_lazy_add_periodic_task(self, monkeypatch):
        """Checks tasks lazy registration"""

        celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)

        @celery.task
        def dummy_task_1():
            pass

        @celery.task
        def dummy_task_2():
            pass

        # Tasks are generally added at import time, before DB is configured
        # Without lazy registration, this would fail because task init sets up
        # DB and therefore requires DB URI env var
        celery.lazy_add_periodic_task(1, dummy_task_1)

        # Simulate celery app finalization in an env with DB URI en var set
        # Check task init works and task is correctly registered
        monkeypatch.setenv("SQLALCHEMY_DATABASE_URI", DUMMY_DB_URI)
        celery.finalize()
        assert "tests.test_celery.dummy_task_1()" in celery.conf.beat_schedule

        # Check lazy registration also works if app is already configured
        celery.lazy_add_periodic_task(1, dummy_task_2)
        assert "tests.test_celery.dummy_task_2()" in celery.conf.beat_schedule

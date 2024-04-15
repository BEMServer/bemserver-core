"""Celery task manager tests"""

import pytest

import sqlalchemy as sqla

from bemserver_core.authorization import OPEN_BAR, auth, get_current_user
from bemserver_core.celery import BEMServerCoreCelery, BEMServerCoreTask
from bemserver_core.database import db
from bemserver_core.model import User


class TestCelery:
    @pytest.mark.usefixtures("bemservercore")
    def test_celery_base_task_open_bar(self):
        """Check base task authorizations

        Check task runs with all permissions (OpenBar mode)
        """
        celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)

        @celery.task
        def dummy_task():
            assert OPEN_BAR.get()
            auth.authorized_query(get_current_user(), "read", User)

        result = dummy_task.apply()

        assert result.state == "SUCCESS"

    @pytest.mark.usefixtures("bemservercore")
    def test_celery_base_task_rollback(self):
        """Check base task rollback

        Check transaction is rolled back at end of task if left uncommitted
        """
        celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)

        @celery.task
        def failure_session_error():
            User.new(name="Chuck")
            db.session.commit()

        @celery.task
        def success_session_error():
            User.new(name="Chuck")
            try:
                db.session.commit()
            except sqla.exc.IntegrityError:
                pass

        @celery.task
        def success_session_ok_1():
            user_1 = User.new(
                name="John",
                email="john@doe.com",
                is_admin=True,
                is_active=True,
            )
            user_1.set_password("D0e")
            db.session.commit()

        @celery.task
        def success_session_ok_2():
            user_2 = User.new(
                name="Jane",
                email="jane@doe.com",
                is_admin=True,
                is_active=True,
            )
            user_2.set_password("D0e")
            db.session.commit()

        result = failure_session_error.apply()
        assert result.state == "FAILURE"
        result = success_session_ok_1.apply()
        assert result.state == "SUCCESS"
        result = success_session_error.apply()
        assert result.state == "SUCCESS"
        result = success_session_ok_2.apply()
        assert result.state == "SUCCESS"

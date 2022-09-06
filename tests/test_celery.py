"""Celery task manager tests"""
import pytest

from bemserver_core.model import User
from bemserver_core.authorization import auth, get_current_user, OPEN_BAR
from bemserver_core.celery import BEMServerCoreCelery, BEMServerCoreTask


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

        dummy_task.apply()

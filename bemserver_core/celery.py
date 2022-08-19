"""Celery application

Manages scheduled and asynchronous tasks
"""
import os
from functools import wraps

from celery import Celery, Task
from celery.utils.log import get_task_logger

from bemserver_core.database import db
from bemserver_core.authorization import OpenBar
from bemserver_core.exceptions import BEMServerCoreError


logger = get_task_logger(__name__)


class DefaultCeleryConfig:
    """Default Celery configuration"""

    broker_url = "redis://"
    result_backend = "redis://"
    task_default_queue = "bemserver_core"
    task_track_started = True
    task_send_sent_event = True


def open_bar_wrap(func):
    """Wrap function in OpenBar context"""

    @wraps(func)
    def wrapper(*func_args, **func_kwargs):
        with OpenBar():
            return func(*func_args, **func_kwargs)

    return wrapper


class BEMServerCoreTask(Task):
    """Celery Task override

    - Setup BEMServerCore on init
    - Wrap tasks in OpenBar context
    """

    def __init__(self):

        # Setup BEMServerCore (avoid circular import)
        from bemserver_core import BEMServerCore

        db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
        if db_url is None:
            raise BEMServerCoreError(
                "SQLALCHEMY_DATABASE_URI environment variable not set"
            )
        db.set_db_url(db_url)
        bsc = BEMServerCore()
        bsc.init_auth()

        # This seems to be the recommended way to wrap a task run method
        # https://github.com/celery/celery/issues/1282
        self.run = open_bar_wrap(self.run)


class BEMServerCoreCelery(Celery):
    """Celery app class override

    - Lazy registration of periodic tasks
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._periodic_tasks = []
        self.on_after_configure.connect(self.on_after_configure_cb)

    def lazy_add_periodic_task(self, *args, **kwargs):
        """Register periodic task to be added when Celery is configured

        Same arguments as Celery.add_periodic_task.
        """
        if self.configured:
            self.add_periodic_task(*args, **kwargs)
        else:
            self._periodic_tasks.append((args, kwargs))

    def on_after_configure_cb(self, *args, **kwargs):
        """Callback called when Celery is configured.

        - Add periodic tasks
        """
        for args, kwargs in self._periodic_tasks:
            self.add_periodic_task(*args, **kwargs)


celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)
celery.config_from_object(DefaultCeleryConfig)

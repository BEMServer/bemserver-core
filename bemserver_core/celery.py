"""Celery application

Manages scheduled and asynchronous tasks
"""
import os
from functools import wraps

from celery import Celery, Task, signals
from celery.utils.log import get_task_logger
from celery.exceptions import WorkerShutdown

from bemserver_core.database import db
from bemserver_core.authorization import OpenBar


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

    - Wrap tasks in OpenBar context
    """

    def __init__(self):
        # This seems to be the recommended way to wrap a task run method
        # https://github.com/celery/celery/issues/1282
        self.run = open_bar_wrap(self.run)


class BEMServerCoreCelery(Celery):
    """Celery app class override

    In case we need to override someday so we don't have to fix imports everywhere
    """


@signals.worker_process_init.connect
def worker_process_init_cb(**kwargs):
    """Callback executed at worker init

    - Setup BEMServerCore
    """
    # Setup BEMServerCore (avoid circular import)
    from bemserver_core import BEMServerCore

    db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
    if db_url is None:
        logger.critical("SQLALCHEMY_DATABASE_URI environment variable not set")
        raise WorkerShutdown()
    db.set_db_url(db_url)
    bsc = BEMServerCore()
    bsc.init_auth()


celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)
celery.config_from_object(DefaultCeleryConfig)

"""Celery application

Manages scheduled and asynchronous tasks
"""

import os
from functools import wraps

from celery import Celery, Task, signals
from celery.exceptions import WorkerShutdown
from celery.utils.log import get_task_logger

from bemserver_core import utils
from bemserver_core.authorization import OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreSettingsError

logger = get_task_logger(__name__)


class DefaultCeleryConfig:
    """Default Celery configuration"""

    broker_url = "redis://"
    result_backend = "redis://"
    task_default_queue = "bemserver_core"
    task_track_started = True
    task_send_sent_event = True


def task_wrapper(func):
    """Wrap function in OpenBar context"""

    @wraps(func)
    def wrapper(*func_args, **func_kwargs):
        with OpenBar():
            return func(*func_args, **func_kwargs)

    return wrapper


@signals.task_postrun.connect
def remove_session(sender=None, headers=None, body=None, **kwargs):
    """Close session on end of task to rollback transaction"""
    db.session.remove()


class BEMServerCoreTask(Task):
    """Celery Task override

    - Wrap tasks in OpenBar context
    """

    def __init__(self):
        # This seems to be the recommended way to wrap a task run method
        # https://github.com/celery/celery/issues/1282
        self.run = task_wrapper(self.run)


class BEMServerCoreCelery(Celery):
    """Celery app class override

    In case we need to override someday so we don't have to fix imports everywhere
    """


celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)
celery.config_from_object(DefaultCeleryConfig)
celery_cfg_file = os.environ.get("BEMSERVER_CELERY_SETTINGS_FILE")
if celery_cfg_file is not None:
    celery.conf.update(utils.get_dict_from_pyfile(celery_cfg_file))


@signals.worker_process_init.connect
def worker_process_init_cb(**kwargs):
    """Callback executed at worker init to setup BEMServerCore"""
    # Avoid circular import issue
    from bemserver_core import BEMServerCore

    try:
        BEMServerCore()
    except BEMServerCoreSettingsError as exc:
        logger.critical(str(exc))
        raise WorkerShutdown() from exc

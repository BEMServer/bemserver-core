"""Celery application

Manages scheduled and asynchronous tasks
"""

import abc
import enum
from copy import deepcopy
from functools import wraps
from zoneinfo import ZoneInfo

from celery import Celery, Task, signals
from celery.exceptions import WorkerShutdown
from celery.utils.log import get_task_logger

from bemserver_core import model
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreSettingsError, BEMServerCoreTaskError

logger = get_task_logger(__name__)


class TaskStateEnum(enum.Enum):
    PENDING = "pending"
    SENT = "sent"
    STARTED = "started"
    PROGRESS = "progress"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    REVOKED = "revoked"


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
    """BEMServerCore task base class"""

    def set_progress(self, done, total):
        self.update_state(state="PROGRESS", meta={"done": done, "total": total})

    @property
    def name(self):
        return self.__class__.__name__


class BEMServerCoreSystemTask(BEMServerCoreTask):
    """BEMServerCore system task

    - Wrap tasks in OpenBar context
    """

    def __init__(self):
        # This seems to be the recommended way to wrap a task run method
        # https://github.com/celery/celery/issues/1282
        self.run = task_wrapper(self.run)


class BEMServerCoreAsyncTask(BEMServerCoreTask, Task, abc.ABC):
    DEFAULT_PARAMETERS = {}

    def run(self, user_id, campaign_id, start_dt, end_dt, **kwargs):
        logger.info("Start")

        with OpenBar():
            user = model.User.get_by_id(user_id)
        if user is None:
            raise BEMServerCoreTaskError(f"Unknown user ID {user_id}")

        with CurrentUser(user):
            campaign = model.Campaign.get_by_id(campaign_id)
            if campaign is None:
                raise BEMServerCoreTaskError(f"Unknown campaign ID {campaign_id}")

            return self.do_run(
                campaign,
                start_dt.astimezone(ZoneInfo(campaign.timezone)),
                end_dt.astimezone(ZoneInfo(campaign.timezone)),
                **{**deepcopy(self.DEFAULT_PARAMETERS), **kwargs},
            )

    @abc.abstractmethod
    def do_run(self, campaign, start_dt, end_dt):
        """Task implementation"""


class BEMServerCoreScheduledTask(BEMServerCoreSystemTask, abc.ABC):
    DEFAULT_PARAMETERS = {}
    # Equivalent asynchronous task
    ASYNC_TASK = None

    def run(self):
        logger.info("Start")

        for tbc in model.TaskByCampaign.get(task_name=self.name, is_enabled=True):
            start_dt, end_dt = tbc.make_interval()

            self.do_run(
                tbc.campaign,
                start_dt,
                end_dt,
                **{**deepcopy(self.DEFAULT_PARAMETERS), **tbc.parameters},
            )

    @abc.abstractmethod
    def do_run(self, campaign, start_dt, end_dt):
        """Task implementation"""


class BEMServerCoreCelery(Celery):
    """Celery app class override"""

    def init_app(self, bsc):
        """Init Celery app with BEMServerCore instance"""
        self.bsc = bsc
        self.conf.update(bsc.config["CELERY_CONFIG"])


celery = BEMServerCoreCelery("BEMServer Core")
celery.config_from_object(DefaultCeleryConfig)


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


# https://stackoverflow.com/questions/9824172/find-out-whether-celery-task-exists
@signals.before_task_publish.connect
def cb_set_sent_state(sender=None, headers=None, **kwargs):
    """Set SENT custom status when the task is enqueued"""
    task = celery.tasks.get(sender)
    backend = task.backend
    backend.store_result(headers["id"], None, "SENT")

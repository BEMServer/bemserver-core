"""Celery application

Manages scheduled and asynchronous tasks
"""

from functools import wraps

from celery import Celery, Task, signals
from celery.exceptions import WorkerShutdown
from celery.utils.log import get_task_logger

from bemserver_core import model
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreSettingsError, BEMServerCoreTaskError

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


class BEMServerCoreSystemTask(Task):
    """Celery Task override

    - Wrap tasks in OpenBar context
    """

    def __init__(self):
        # This seems to be the recommended way to wrap a task run method
        # https://github.com/celery/celery/issues/1282
        self.run = task_wrapper(self.run)


class BEMServerCoreClassBasedTaskMixin:
    @property
    def name(self):
        return self.__class__.__name__


class BEMServerCoreAsyncTask(BEMServerCoreClassBasedTaskMixin, Task):
    TASK_FUNCTION = None
    DEFAULT_PARAMETERS = {}

    def run(self, user_id, campaign_id, start_td, end_dt, **kwargs):
        logger.info("Start")

        with OpenBar():
            user = model.User.get_by_id(user_id)
        if user is None:
            raise BEMServerCoreTaskError(f"Unknown user ID {user_id}")

        with CurrentUser(user):
            campaign = model.Campaign.get_by_id(campaign_id)
            if campaign is None:
                raise BEMServerCoreTaskError(f"Unknown campaign ID {campaign_id}")

            # Function is bound at init. Use __func__ to avoid passing self
            self.TASK_FUNCTION.__func__(
                campaign,
                start_td,
                end_dt,
                **{**self.DEFAULT_PARAMETERS, **kwargs},
            )


class BEMServerCoreScheduledTask(
    BEMServerCoreClassBasedTaskMixin, BEMServerCoreSystemTask
):
    TASK_FUNCTION = None
    DEFAULT_PARAMETERS = {}

    def run(self):
        logger.info("Start")

        for tbc in model.TaskByCampaign.get(task_name=self.name, is_enabled=True):
            start_dt, end_dt = tbc.make_interval()

            # Function is bound at init. Use __func__ to avoid passing self
            self.TASK_FUNCTION.__func__(
                tbc.campaign,
                start_dt,
                end_dt,
                **{**self.DEFAULT_PARAMETERS, **tbc.parameters},
            )


class BEMServerCoreCelery(Celery):
    """Celery app class override

    In case we need to override someday so we don't have to fix imports everywhere
    """

    SCHEDULED_TASKS_NAME_SUFFIX = "Scheduled"

    def init_app(self, bsc):
        """Init Celery app with BEMServerCore instance"""
        self.bsc = bsc
        self.conf.update(bsc.config["CELERY_CONFIG"])

    def register_task(self, task, **options):
        """Register task

        When registering an AsyncTask, also register corresponding Scheduled task
        """
        task = super().register_task(task, **options)
        if isinstance(task, BEMServerCoreAsyncTask):
            scheduled_task = type(
                f"{task.__name__}{self.SCHEDULED_TASKS_NAME_SUFFIX}",
                (BEMServerCoreScheduledTask,),
                {
                    "TASK_FUNCTION": task.TASK_FUNCTION,
                    "DEFAULT_PARAMETERS": task.DEFAULT_PARAMETERS,
                },
            )
            super().register_task(scheduled_task, **options)
        return task


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

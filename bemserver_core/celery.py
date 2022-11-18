"""Celery application

Manages scheduled and asynchronous tasks
"""
import os
from pathlib import Path
from functools import wraps
import types
import errno

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


# The two functions below are meant to load Celery configuration from an
# external file by passing the path to that file as an environment variable.
# Those functions, adapted from Flask code, were copied from Celery bugtracker.
# https://github.com/celery/celery/issues/5303


def config_from_envvar(celery_app, var_name, silent=False):
    """
    Load celery config from an envvar that points to a python config file.

    Basically this:

        config_from_pyfile(os.environ['YOUR_APP_SETTINGS'])

    Example:
        >>> os.environ['CELERY_CONFIG_FILE'] = './some_dir/config_file.cfg'
        >>> config_from_envvar(celery, 'CELERY_CONFIG_FILE')

    Arguments:
        celery_app (Celery app instance): The celery app to update
        var_name (str): The env var to use.
        silent (bool): If true then import errors will be ignored.

    Shamelessly taken from Flask. Like, almost exactly. Thanks!
    https://github.com/pallets/flask/blob/74691fbe0192de1134c93e9821d5f8ef65405670/flask/config.py#L88
    """
    rv = os.environ.get(var_name)
    if not rv:
        if silent:
            return False
        raise RuntimeError(
            "The environment variable %r is not set"
            " and as such configuration could not be"
            " loaded. Set this variable to make it"
            " point to a configuration file." % var_name
        )
    return config_from_pyfile(celery_app, rv, silent=silent)


def config_from_pyfile(celery_app, filename, silent=False):
    """
    Mimics Flask's config.from_pyfile()

    Allows loading a separate, perhaps non `.py`, file into Celery.

    Example:
        >>> config_from_pyfile(celery, './some_dir/config_file.cfg')

    Arguments:
        celery_app (Celery app instance): The celery app to update
        filename (str): The file to load.
        silent (bool): If true then import errors will be ignored.

    Also shamelessly taken from Flask:
    https://github.com/pallets/flask/blob/74691fbe0192de1134c93e9821d5f8ef65405670/flask/config.py#L111
    """
    filename = str(Path(filename).resolve())
    d = types.ModuleType("config")
    d.__file__ = filename

    try:
        with open(filename, "rb") as config_file:
            exec(compile(config_file.read(), filename, "exec"), d.__dict__)
    except OSError as e:
        if silent and e.errno in (errno.ENOENT, errno.EISDIR, errno.ENOTDIR):
            return False
        e.strerror = "Unable to load config file (%s)" % e.strerror
        raise

    # Remove any "hidden" attributes: __ and _
    for k in list(d.__dict__.keys()):
        if k.startswith("_"):
            del d.__dict__[k]

    celery_app.conf.update(d.__dict__)
    return True


celery = BEMServerCoreCelery("BEMServer Core", task_cls=BEMServerCoreTask)
celery.config_from_object(DefaultCeleryConfig)
config_from_envvar(celery, "BEMSERVER_CELERY_SETTINGS_FILE", silent=True)

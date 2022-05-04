import sys
import importlib
import argparse
import json
import logging
from functools import wraps

from apscheduler.schedulers.background import BlockingScheduler

from bemserver_core import BEMServerCore
from bemserver_core.database import db
from bemserver_core.authorization import OpenBar


logger = logging.getLogger("bemserver-scheduler")


def open_bar_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with OpenBar():
            return func(*args, **kwargs)

    return wrapper


class BEMServerCoreScheduler:
    def __init__(self, db_url, plugins=None):
        self.scheduler = BlockingScheduler()
        db.set_db_url(db_url)
        self.bemservercore = BEMServerCore()
        for plugin in plugins:
            self.bemservercore.load_plugin(plugin)
        # Init processors
        self.processors = {p.STR_ID: p() for p in self.bemservercore.processor_classes}
        for str_id, processor in self.processors.copy().items():
            if processor.db_processor is None:
                logger.warning(f"{processor.NAME} installed but not in DB")
                del self.processors[str_id]
                continue
            self.schedule(processor)

        # TODO: Check DB + log error if processor in DB but not installed?

    def start(self):
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()

    def schedule(self, processor):
        logger.info(f"Add job {processor.NAME}")
        self.scheduler.add_job(
            open_bar_wrapper(processor.run),
            processor.SCHEDULE_TYPE,
            **processor.SCHEDULE_KWARGS,
            id=processor.STR_ID,
            name=processor.NAME,
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run BEMServerCore scheduler")
    parser.add_argument(
        "-c", dest="config_file", required=True, help="Configuration file"
    )
    args = parser.parse_args()

    try:
        with open(args.config_file) as f:
            config = json.load(f)
    except OSError as e:
        print(f"Config file error: {e}")
        sys.exit()

    db_url = config["SQLALCHEMY_DATABASE_URI"]
    logging.basicConfig(level=config["LOGGING"]["LEVEL"])

    plugins = []
    for path in config["PLUGINS"]:
        mod, cls = path.rsplit(".", 1)
        plugin = getattr(importlib.import_module(mod), cls)()
        plugins.append(plugin)

    bsc_scheduler = BEMServerCoreScheduler(db_url, plugins)
    bsc_scheduler.start()

from bemserver_core.model import Processor
from bemserver_core.database import db

from .base import BEMServerCoreProcessor  # noqa


# Processors bundled with BEMServerCore
PROCESSOR_CLASSES = []


def init_db_processors():
    """Create a row for each processor

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    for processor in PROCESSOR_CLASSES:
        db.session.add(Processor(id=processor.STR_ID))
    db.session.commit()

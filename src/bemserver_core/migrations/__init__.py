"""DB migrations"""

from argparse import Namespace
from pathlib import Path

from alembic import command
from alembic.config import Config

ALEMBIC_CFG_PATH = Path(__file__).parent.parent / "alembic.ini"


def current(verbose=False):
    command.current(Config(ALEMBIC_CFG_PATH), verbose=verbose)


def upgrade(revision="head"):
    command.upgrade(Config(ALEMBIC_CFG_PATH), revision)


def downgrade(revision):
    command.downgrade(Config(ALEMBIC_CFG_PATH), revision)


def revision(message, rev_id):
    # https://github.com/sqlalchemy/alembic/discussions/1089
    alembic_cfg = Config(ALEMBIC_CFG_PATH, cmd_opts=Namespace(autogenerate=True))
    command.revision(alembic_cfg, message, rev_id=rev_id, autogenerate=True)

"""Commands

This module provides commands made available as CLI commands.
"""
import os

import click

from bemserver_core import model, database, migrations
from bemserver_core.exceptions import BEMServerCoreError


def _set_db_url():
    db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
    if db_url is None:
        raise BEMServerCoreError("SQLALCHEMY_DATABASE_URI environment variable not set")
    database.db.set_db_url(db_url)


def setup_db():
    """Create initial DB data

    Create tables and initial data in a clean database.

    This function assumes DB URI is set.
    """
    database.db.create_all(checkfirst=False)
    database.init_db_functions()
    model.events.init_db_events_triggers()
    model.events.init_db_events()
    model.campaigns.init_db_campaigns_triggers()
    model.timeseries.init_db_timeseries_triggers()
    model.timeseries.init_db_timeseries()
    model.sites.init_db_structural_elements_triggers()
    model.energy.init_db_energy()


@click.command()
def setup_db_cmd():
    """Create tables and initial data in a clean database.

    This command is meant to be used for dev setups.
    Production setups should rely on migration scripts.
    """
    _set_db_url()
    setup_db()


@click.command()
@click.option("--name", required=True, help="User name")
@click.option("--email", required=True, help="User email address")
@click.option("--admin", is_flag=True, default=False, help="User is admin")
@click.option("--inactive", is_flag=True, default=False, help="User is inactive")
@click.option(
    "--password",
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    required=True,
    help="User password",
)
def create_user_cmd(name, email, admin, inactive, password):
    """Create a new user"""
    _set_db_url()
    user = model.User(
        name=name,
        email=email,
        _is_admin=admin,
        _is_active=not inactive,
        password=model.users.hash_password(password),
    )
    database.db.session.add(user)
    database.db.session.commit()


@click.command()
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose mode")
def db_current_cmd(verbose):
    """Display current database revision"""
    migrations.current(verbose)


@click.command()
@click.option("-r", "--revision", default="head", help="Revision target")
def db_upgrade_cmd(revision):
    """Upgrade to a later database revision"""
    migrations.upgrade(revision)


@click.command()
@click.option("-r", "--revision", required=True, help="Revision target")
def db_downgrade_cmd(revision):
    """Revert to a previous database revision"""
    migrations.downgrade(revision)

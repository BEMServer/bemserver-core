"""Plugin tests"""
import sqlalchemy as sqla

import pytest

from bemserver_core import BEMServerCore, BEMServerCorePlugin, model
from bemserver_core.database import Base, db
from bemserver_core.authorization import auth, AuthMixin
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestPlugin:
    @pytest.mark.usefixtures("database")
    def test_load_plugin(self, tmp_path):

        # Define and create plugin table
        class TestLoadPlugin(AuthMixin, Base):
            __tablename__ = "test_load_plugin"

            id = sqla.Column(sqla.Integer, primary_key=True)
            active = sqla.Column(sqla.Boolean, nullable=False)

        # Create polar file defining authorizations for plugin resource
        polar_file = tmp_path / "auth.polar"
        with open(polar_file, "w") as polar_f:
            polar_f.write(
                "resource TestLoadPlugin {\n"
                '   permissions = ["read", "write"];\n'
                '   roles = ["user"];\n'
                '   "read" if "user";\n'
                "}\n"
            )

        # Create plugin
        class Plugin(BEMServerCorePlugin):
            AUTH_MODEL_CLASSES = [
                TestLoadPlugin,
            ]
            AUTH_POLAR_FILES = [
                polar_file,
            ]

        # Load plugin and init BEMServerCore
        bsc = BEMServerCore()
        bsc.load_plugin(Plugin())
        bsc.init_auth()
        db.create_all()

        # Create dummy user and dummy row in TestLoadPlugin
        # Skip authorization layer as we don't have a user yet
        user = model.User(
            name="Test",
            email="test@test.com",
            _is_admin=False,
            _is_active=True,
            password="...",
        )
        db.session.add(user)
        tlp = TestLoadPlugin(active=True)
        db.session.add(tlp)
        db.session.flush()

        # Check user can read but not write, as specified in polar file
        auth.authorize(user, "read", tlp)
        with pytest.raises(BEMServerAuthorizationError):
            auth.authorize(user, "write", tlp)

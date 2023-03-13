"""BEMServerCore tests"""

import pytest

from bemserver_core import BEMServerCore
from bemserver_core.exceptions import BEMServerCoreSettingsError


class TestBEMServerCore:
    def test_missing_settings_file_env_var(self):
        with pytest.raises(
            BEMServerCoreSettingsError,
            match="Missing BEMSERVER_CORE_SETTINGS_FILE environment variable",
        ):
            BEMServerCore()

    def test_wrong_settings_file_path(self, monkeypatch):
        monkeypatch.setenv("BEMSERVER_CORE_SETTINGS_FILE", "dummy_path")
        with pytest.raises(
            BEMServerCoreSettingsError,
            match='Unable to load file "No such file or directory":',
        ):
            BEMServerCore()

    def test_wrong_settings_file(self, tmp_path, monkeypatch):
        cfg_file = tmp_path / "config.py"
        cfg_file.write_text("dummy 42")
        monkeypatch.setenv("BEMSERVER_CORE_SETTINGS_FILE", str(cfg_file))
        with pytest.raises(BEMServerCoreSettingsError, match="invalid syntax"):
            BEMServerCore()

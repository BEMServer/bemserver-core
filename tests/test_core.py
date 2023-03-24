"""BEMServerCore tests"""

import pytest

from bemserver_core import BEMServerCore
from bemserver_core.exceptions import BEMServerCoreSettingsError


class TestBEMServerCore:
    def test_missing_settings_file_env_var(self, monkeypatch):
        monkeypatch.delenv("BEMSERVER_CORE_SETTINGS_FILE", raising=False)
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

    @pytest.mark.parametrize(
        "config",
        ({"test": 1, "_test": 2, "__test": 3},),
        indirect=True,
    )
    @pytest.mark.usefixtures("config")
    def test_underscore_variables_ignored_in_config_file(self):
        bsc = BEMServerCore()
        assert bsc.config["test"] == 1
        assert "_test" not in bsc.config
        assert "__test" not in bsc.config

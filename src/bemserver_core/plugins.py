import importlib
import sys
from pathlib import Path


def init_core(bsc):
    """Load Core plugins

    Loading a plugin adds its parent directory to sys.path. It is wise to store plugins
    in dedicated directories.
    """
    for plugin_path in bsc.config.get("PLUGIN_PATHS"):
        plugin_path = Path(plugin_path)
        name = plugin_path.stem
        sys.path.append(str(plugin_path.parent))
        importlib.import_module(name)

import importlib
from pathlib import Path


def load_package(path):
    """Import package at a given path

    :param Path path: Package (file or directory) to import
    """
    name = path.stem
    if path.is_dir():
        path /= "__init__.py"

    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def init_core(bsc):
    """Load Core plugins"""
    plugin_paths = bsc.config.get("PLUGIN_PATHS")

    for plugin_path in plugin_paths:
        plugin_path = Path(plugin_path)
        load_package(plugin_path)

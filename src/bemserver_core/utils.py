"""Utils"""

import functools
import types
from contextlib import AbstractContextManager
from pathlib import Path


class ContextVarManager(AbstractContextManager):
    """Set context variable for context"""

    def __init__(self, context_var, value):
        self._context_var = context_var
        self._token = None
        self._value = value

    def __enter__(self):
        self._token = self._context_var.set(self._value)

    def __exit__(self, *args, **kwargs):
        self._context_var.reset(self._token)


def make_context_var_manager(context_var):
    """Create context variable manager for a context variable"""
    return functools.partial(ContextVarManager, context_var)


# Adapted from Flask config code
def get_dict_from_pyfile(file_path):
    """Turn python module into a dict"""
    filename = str(Path(file_path).resolve())
    mod = types.ModuleType("config")
    mod.__file__ = filename

    try:
        with open(filename, "rb") as config_file:
            exec(compile(config_file.read(), filename, "exec"), mod.__dict__)
    except OSError as exc:
        exc.strerror = f'Unable to load file "{exc.strerror}"'
        raise

    return {k: v for k, v in mod.__dict__.items() if not k.startswith("_")}

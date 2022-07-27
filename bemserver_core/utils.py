"""Utils"""
import functools
from contextlib import AbstractContextManager


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

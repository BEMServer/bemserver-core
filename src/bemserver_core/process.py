"""Execute function in a separate process"""

from functools import wraps
from multiprocessing import Process, Queue

from bemserver_core.exceptions import BEMServerCoreProcessTimeoutError

# Timeout in seconds
TIMEOUT = 5


def process(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()

        def wrap_func(*args, **kwargs):
            try:
                return 0, func(*args, **kwargs)
            except Exception as exc:
                return 1, exc

        def queue_func():
            q.put(wrap_func(*args, **kwargs))

        p = Process(target=queue_func)
        p.start()
        p.join(timeout=TIMEOUT)
        if p.is_alive():
            p.terminate()
            p.join()
            raise BEMServerCoreProcessTimeoutError

        code, result = q.get(block=False)
        if code != 0:
            raise result
        return result

    return wrapper

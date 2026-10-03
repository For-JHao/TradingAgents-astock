"""Small process-local guards for shared public-data resources."""
from contextlib import contextmanager
from pathlib import Path
import functools, os, tempfile, threading, time

_LOCKS = {}
_GUARD = threading.Lock()
_EASTMONEY_LOCK = threading.RLock()
_EASTMONEY_LAST = 0.0

def resource_lock(key):
    with _GUARD:
        return _LOCKS.setdefault(str(key), threading.RLock())

def synchronized(key):
    def decorate(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            with resource_lock(key(*args, **kwargs) if callable(key) else key):
                return function(*args, **kwargs)
        return call
    return decorate

@contextmanager
def eastmoney_budget(interval):
    global _EASTMONEY_LAST
    with _EASTMONEY_LOCK:
        delay = interval - (time.monotonic() - _EASTMONEY_LAST)
        if delay > 0:
            time.sleep(delay)
        try:
            yield
        finally:
            _EASTMONEY_LAST = time.monotonic()

def atomic_csv(frame, path, **kwargs):
    target = Path(path)
    fd, name = tempfile.mkstemp(prefix=".data-cache-", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding=kwargs.pop("encoding", "utf-8"), newline="") as stream:
            frame.to_csv(stream, **kwargs)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, target)
    finally:
        Path(name).unlink(missing_ok=True)

"""Short critical sections around native Markdown memory operations."""
from contextlib import contextmanager
from pathlib import Path
import functools, threading
import fcntl
from tradingagents.agents.utils.memory import TradingMemoryLog
_LOCKS = {}
_GUARD = threading.Lock()

class SynchronizedMemoryLog(TradingMemoryLog):
    @contextmanager
    def _locked(self):
        if self._log_path is None:
            yield
            return
        key = str(self._log_path.resolve())
        with _GUARD:
            lock = _LOCKS.setdefault(key, threading.RLock())
        with lock:
            # Nested native methods reuse the existing logical-resource lock.
            if getattr(self, "_lock_depth", 0):
                yield
                return
            self._lock_depth = 1
            try:
                with open(key + ".lock", "a") as stream:
                    fcntl.flock(stream, fcntl.LOCK_EX)
                    try:
                        yield
                    finally:
                        fcntl.flock(stream, fcntl.LOCK_UN)
            finally:
                self._lock_depth = 0

def _synchronized(name):
    native = getattr(TradingMemoryLog, name)
    @functools.wraps(native)
    def operation(self, *args, **kwargs):
        with self._locked():
            return native(self, *args, **kwargs)
    return operation

for _name in ("store_decision", "load_entries", "get_pending_entries", "get_past_context", "batch_update_with_outcomes", "update_with_outcome"):
    if hasattr(TradingMemoryLog, _name):
        setattr(SynchronizedMemoryLog, _name, _synchronized(_name))

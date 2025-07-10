import os
import fcntl  # On Unix-like systems only

class SQLiteWriteLock:
    def __init__(self, lock_file="crawler.lock"):
        self.lock_file = lock_file
        self.fd = None

    def __enter__(self):
        self.fd = open(self.lock_file, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # Non-blocking
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        self.fd.close()

    def is_locked(self):
        try:
            with open(self.lock_file, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(f, fcntl.LOCK_UN)
            return False  # Not locked
        except BlockingIOError:
            return True   # Locked by another process
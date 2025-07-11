import os
import fcntl  # On Unix-like systems only

# my imports
import config as cfg

class LockFile:
    """
    Context manager to ensure only one crawler process runs per working directory by using file-based locking.

    This class uses POSIX advisory file locking (fcntl) on a lock file located inside the
    crawler's working directory. It prevents concurrent processes from operating on the same
    dataset (e.g., database, output files, logs).

    - Locking is non-blocking: if the file is already locked, a BlockingIOError is raised.
    - The class is a context manager, releasing the lock automatically on exit.
    - Supports checking the lock status using `is_locked()`.

    Multiple crawler instances can run concurrently as long as each uses a separate working
    directory and lock file. This ensures safe parallel crawling across independent targets.

    Usage:
        with LockFile():
            # Only one crawler per WORKDIR will proceed beyond this point
    """    

    def __init__(self, lock_file=cfg.LOCK_FILE):
        """
        Initialize the LockFile with a path to the lock file.

        Args:
            lock_file (str): Path to the lock file. Defaults to cfg.LOCK_FILE.
        """
        self.lock_file = lock_file
        self.fd = None

    def __enter__(self):
        """
        Acquire an exclusive, non-blocking lock on the lock file.

        Raises:
            BlockingIOError: If the lock is already held by another process.

        Returns:
            LockFile: The instance itself for context management.
        """
        self.fd = open(self.lock_file, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # Non-blocking
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Release the lock and close the file descriptor upon exiting the context.
        """
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        self.fd.close()

    def is_locked(self):
        """
        Check if the lock file is currently locked by another process.

        Returns:
            bool: True if the lock is held by another process, False otherwise.
        """
        try:
            with open(self.lock_file, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(f, fcntl.LOCK_UN)
            return False  # Not locked
        except BlockingIOError:
            return True   # Locked by another process
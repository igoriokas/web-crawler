from datetime import datetime
import sqlite3
import logging

# my imports
import config as cfg

logger = logging.getLogger('crawler.state')


def now():
    """
    Return current date and time in consistent format
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class CrawlerState:
    """
    Manages persistent crawl state using a SQLite database.

    Tracks URLs to be visited(queued), visited pages, failed pages, retry counts,
    and aggregated word statistics across pages. Supports efficient resumption,
    deduplication, and progress tracking of the crawling process.
    
    Pages are stored with metadata such as depth, status, and timestamps.
    Word counts are maintained in a separate table and updated incrementally.
    
    The class is intended to be used as a context manager (with CrawlerState(): ...),
    ensuring proper connection management.
    """

    def __init__(self, db_path=None):
        """
        Initialize the database connection and create required tables if they do not exist.

        Parameters:
            db_path (str): Path to the SQLite database file.
        """
        db_path = cfg.DB_PATH if db_path is None else db_path
        self.conn = sqlite3.connect(db_path, isolation_level='DEFERRED', timeout=5)
        logger.info(f"connection open, isolation_level=[{self.conn.isolation_level}]")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self):
        """
        Create tables and relevant indexes if they don't exist.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                sid INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                depth INTEGER,
                status TEXT CHECK(status IN ('queued', 'visited', 'failed')) DEFAULT 'queued',
                attempts INTEGER DEFAULT 0,
                inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_attempt TEXT,
                error TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS attempts (
                sid INTEGER,
                url TEXT,
                depth INTEGER,
                attempt INTEGER,
                status INTEGER,
                duration REAL,
                attempt_time TEXT,
                error TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS words (
                word TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0
            );
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON pages(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status_retries ON pages(status, attempts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON pages(url)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_word ON words(word);")
        self.conn.commit()

    def enqueue_url(self, url, depth):
        """
        Add a URL to the crawl queue if it is not already present.

        Parameters:
            url (str): The URL to enqueue.
            depth (int): The depth level of the URL in the crawl hierarchy.
        """
        self.conn.execute("""
            INSERT OR IGNORE INTO pages (url, depth, inserted_at) VALUES (?, ?, ?)
        """, (url, depth, now()))
        self.conn.commit()

    def log_attempt(self, sid, url, depth, attempt, status, duration, error):
        self.conn.execute("""
            INSERT INTO attempts (sid, url, depth, attempt, status, duration, attempt_time, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (sid, url, depth, attempt, status, float(f'{duration:.3f}'), now(), error))
        self.conn.commit()

    def peek_url(self):
        """
        Retrieve the next URL to fetch.

        Returns:
            tuple: (sid, url, depth, attempts) or None if the queue is empty.
        """
        cur = self.conn.execute("""
            SELECT sid, url, depth, attempts FROM pages
            WHERE status = 'queued'
            ORDER BY depth, attempts DESC, inserted_at
            LIMIT 1
        """)
        return cur.fetchone()

    def mark_attempt(self, url):
        """
        Record an attempt to fetch the given URL by incrementing its attempt count
        and updating the last attempt timestamp.

        Parameters:
            url (str): The URL being attempted.
        """
        self.conn.execute("""
            UPDATE pages SET attempts = attempts + 1, last_attempt = ? WHERE url = ?
        """, (now(), url))
        self.conn.commit()

    def decrease_attempt(self, url):
        """
        Undo a recorded attempt for a URL (e.g., in case of a controlled failure).
        
        Parameters:
            url (str): The URL to adjust.
        """
        self.conn.execute("""
            UPDATE pages SET attempts = attempts - 1, last_attempt = ? WHERE url = ?
        """, (now(), url))
        self.conn.commit()

    def mark_success(self, url, commit=True):
        """
        Mark a URL as successfully visited.

        Parameters:
            url (str): The URL to mark.
            commit (bool): Whether to immediately commit the change.
        """
        self.conn.execute("""
            UPDATE pages SET status = 'visited' WHERE url = ?
        """, (url,))
        if commit:
            self.conn.commit()

    def mark_failure(self, url, error='unknown error'):
        """
        Mark a URL as failed with an error message and update last attempt time.

        Parameters:
            url (str): The URL to mark.
            error (str): A short error description.
        """
        self.conn.execute("""
            UPDATE pages SET status = 'failed', last_attempt = ?, error = ? WHERE url = ?
        """, (now(), error, url))
        self.conn.commit()

    def update_word_counts(self, word_counter, commit=True):
        """
        Update global word frequency counts using the provided Counter.

        Parameters:
            word_counter (Counter): A mapping of word -> frequency.
            commit (bool): Whether to commit immediately.
        """
        if not word_counter:
            return    

        for word, count in word_counter.items():
            self.conn.execute("""
                INSERT INTO words (word, count) VALUES (?, ?)
                ON CONFLICT(word) DO UPDATE SET count = count + ?
            """, (word, count, count))
        if commit:
            self.conn.commit()

    def update_word_counts_mark_success(self, word_counter, url):
        """
        Atomically update word counts and mark the corresponding URL as visited.

        Parameters:
            word_counter (Counter): A mapping of word -> frequency.
            url (str): The URL to mark as successfully processed.
        """
        with self.conn:
            self.update_word_counts(word_counter, commit=False)
            self.mark_success(url, commit=False)

    def len(self):
        """
        Get the total number of records in the pages table.

        Returns:
            int: Total number of tracked pages.
        """
        cur = self.conn.execute("SELECT COUNT(*) FROM pages")
        return cur.fetchone()[0]

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()
        logger.info("connection closed")

    def __enter__(self):
        """
        Support for use as a context manager.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures database connection is closed when exiting context.
        """
        self.close()

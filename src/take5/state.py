from datetime import datetime
import sqlite3
import logging


logger = logging.getLogger('crawler.state')


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class CrawlerState:
    def __init__(self, db_path="state.db"):
        self.conn = sqlite3.connect(db_path, isolation_level='DEFERRED', timeout=5)
        logger.info(f"connection open, isolation_level=[{self.conn.isolation_level}]")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self):
        # sid must be PRIMARY KEY to have AUTOINCREMENT feature
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
        self.conn.execute("""
            INSERT OR IGNORE INTO pages (url, depth, inserted_at) VALUES (?, ?, ?)
        """, (url, depth, now()))
        self.conn.commit()

    def peek_url(self):
        cur = self.conn.execute("""
            SELECT sid, url, depth, attempts FROM pages
            WHERE status = 'queued'
            ORDER BY depth, attempts DESC, inserted_at
            LIMIT 1
        """)
        return cur.fetchone()

    def mark_attempt(self, url):
        self.conn.execute("""
            UPDATE pages SET attempts = attempts + 1, last_attempt = ? WHERE url = ?
        """, (now(), url))
        self.conn.commit()

    def decrease_attempt(self, url):
        self.conn.execute("""
            UPDATE pages SET attempts = attempts - 1, last_attempt = ? WHERE url = ?
        """, (now(), url))
        self.conn.commit()

    def mark_success(self, url, commit=True):
        self.conn.execute("""
            UPDATE pages SET status = 'visited' WHERE url = ?
        """, (url,))
        if commit:
            self.conn.commit()

    def mark_failure(self, url, error='unknown error'):
        self.conn.execute("""
            UPDATE pages SET status = 'failed', last_attempt = ?, error = ? WHERE url = ?
        """, (now(), error, url))
        self.conn.commit()

    def update_word_counts(self, word_counter, commit=True):
        for word, count in word_counter.items():
            self.conn.execute("""
                INSERT INTO words (word, count) VALUES (?, ?)
                ON CONFLICT(word) DO UPDATE SET count = count + ?
            """, (word, count, count))
        if commit:
            self.conn.commit()

    def update_word_counts_mark_success(self, word_counter, url):
        with self.conn:
            self.update_word_counts(word_counter, commit=False)
            self.mark_success(url, commit=False)

    def len(self):
        cur = self.conn.execute("SELECT COUNT(*) FROM pages")
        return cur.fetchone()[0]

    def close(self):
        self.conn.close()
        logger.info("connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == "__main__":
    state = CrawlerState()
    print(state.len())
    state.enqueue_url('myurl1',1)
    print(state.len())
    state.enqueue_url('myurl1',1)
    print(state.len())
    state.enqueue_url('myurl2',1)
    print(state.len())
    state.close()
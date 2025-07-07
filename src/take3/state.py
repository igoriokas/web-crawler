from datetime import datetime
import sqlite3


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class CrawlerState:
    def __init__(self, db_path="_state.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS to_visit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                depth INTEGER,
                retries INTEGER DEFAULT 0,
                inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_attempt TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS visited (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                depth INTEGER,
                inserted_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS failed (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                depth INTEGER,
                retries INTEGER,
                inserted_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def enqueue_url(self, url, depth):
        self.conn.execute("""
            INSERT OR IGNORE INTO to_visit (url, depth, retries, inserted_at)
            VALUES (?, ?, 0, ?)
        """, (url, depth, now()))
        self.conn.commit()

    def peek_url(self):
        cur = self.conn.execute("""
            SELECT id, url, depth, retries FROM to_visit
            ORDER BY depth, retries, inserted_at LIMIT 1
        """)
        return cur.fetchone()

    def mark_success(self, id, url, depth):
        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO visited (id, url, depth, inserted_at)
                VALUES (?, ?, ?, ?)
            """, (id, url, depth, now()))
            self.conn.execute("DELETE FROM to_visit WHERE url = ?", (url,))

    def mark_failure(self, id, url, depth, retries, max_retries=3):
        if retries + 1 >= max_retries:
            with self.conn:
                self.conn.execute("""
                    INSERT OR REPLACE INTO failed (id, url, depth, retries, inserted_at)
                    VALUES (?, ?, ?, ?)
                """, (id, url, depth, retries + 1, now()))
                self.conn.execute("DELETE FROM to_visit WHERE url = ?", (url,))
        else:
            self.conn.execute("""
                UPDATE to_visit SET retries = retries + 1, last_attempt = ?
                WHERE url = ?
            """, (now(), url))
        self.conn.commit()

    def update_last_attempt(self, url):
        self.conn.execute("""
            UPDATE to_visit SET last_attempt = ? WHERE url = ?
        """, (now(), url))
        self.conn.commit()

    def already_seen(self, url):
        cur = self.conn.execute("""
            SELECT 1 FROM visited WHERE url = ?
            UNION
            SELECT 1 FROM to_visit WHERE url = ?
            UNION
            SELECT 1 FROM failed WHERE url = ?
        """, (url, url, url))
        return cur.fetchone() is not None

    def has_work(self):
        cur = self.conn.execute("SELECT 1 FROM to_visit LIMIT 1")
        return cur.fetchone() is not None

    def close(self):
        self.conn.close()

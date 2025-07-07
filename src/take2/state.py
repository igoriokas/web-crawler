import sqlite3

class CrawlerState:
    def __init__(self, db_path="crawler.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")  # Safer concurrent access
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS visited (url TEXT PRIMARY KEY, depth INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS to_visit (url TEXT PRIMARY KEY, depth INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS failed (url TEXT PRIMARY KEY, error_count INTEGER)")
        self.conn.commit()

    def enqueue_url(self, url, depth):
        self.conn.execute("INSERT OR IGNORE INTO to_visit (url, depth) VALUES (?, ?)", (url, depth))
        self.conn.commit()

    def dequeue_url(self):
        cur = self.conn.execute("SELECT url, depth FROM to_visit ORDER BY depth LIMIT 1")
        row = cur.fetchone()
        if row:
            self.conn.execute("DELETE FROM to_visit WHERE url = ?", (row[0],))
            self.conn.commit()
            return row
        return None

    def mark_visited(self, url, depth):
        self.conn.execute("INSERT OR IGNORE INTO visited (url, depth) VALUES (?, ?)", (url, depth))
        self.conn.commit()

    def mark_failed(self, url):
        cur = self.conn.execute("SELECT error_count FROM failed WHERE url = ?", (url,))
        row = cur.fetchone()
        if row:
            self.conn.execute("UPDATE failed SET error_count = ? WHERE url = ?", (row[0] + 1, url))
        else:
            self.conn.execute("INSERT INTO failed (url, error_count) VALUES (?, ?)", (url, 1))
        self.conn.commit()

    def already_seen(self, url):
        cur = self.conn.execute("""
            SELECT 1 FROM visited WHERE url = ?
            UNION
            SELECT 1 FROM to_visit WHERE url = ?
        """, (url, url))
        return cur.fetchone() is not None

    def has_work(self):
        cur = self.conn.execute("SELECT 1 FROM to_visit LIMIT 1")
        return cur.fetchone() is not None

    def close(self):
        self.conn.close()
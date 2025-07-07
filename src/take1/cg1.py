import os
import time
import random
import pickle
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import deque

# Constants
DOMAIN = "books.toscrape.com"
START_URL = f"https://{DOMAIN}/"
STATE_FILE = "crawler_state.pkl"
MAX_DEPTH = 5
HEADERS = {
    "User-Agent": "MyResearchCrawler/1.0 (contact: crawler@homework.com)"
}

class CrawlerState:
    def __init__(self):
        self.visited = set()
        self.to_visit = deque([(START_URL, 0)])
        self.failed = {}

def save_state(state):
    with open(STATE_FILE, "wb") as f:
        pickle.dump(state, f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "rb") as f:
            return pickle.load(f)
    return CrawlerState()

def is_valid_link(href):
    if not href or ':' in href:
        return False
    parsed = urlparse(href)
    return parsed.netloc == '' or parsed.netloc.endswith(DOMAIN)

def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a['href']
        if is_valid_link(href):
            full_url = urljoin(base_url, href)
            if full_url.startswith(START_URL):
                yield full_url.split('#')[0]  # remove fragments

def fetch_url(url, max_retries=5):
    backoff = 1
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                return resp.text
            else:
                print(f"HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
        time.sleep(backoff)
        backoff *= 2
    return None

def main():
    state = load_state()
    print(f"Resuming crawler. {len(state.to_visit)} URLs in queue.")

    try:
        while state.to_visit:
            url, depth = state.to_visit.popleft()
            if url in state.visited or depth > MAX_DEPTH:
                continue

            print(f"Crawling (depth {depth}): {url}")
            html = fetch_url(url)

            if html:
                state.visited.add(url)

                # Save HTML page locally
                filename = url.replace(START_URL, "").replace('/', '_')
                path = f"pages/{filename}.html"
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(html)

                # Extract new links if depth allows
                if depth < MAX_DEPTH:
                    for link in extract_links(html, url):
                        if link not in state.visited:
                            state.to_visit.append((link, depth + 1))
            else:
                state.failed[url] = state.failed.get(url, 0) + 1

            # Periodic state saving
            if len(state.visited) % 10 == 0:
                print(f"Saving state after {len(state.visited)} pages...")
                save_state(state)

            time.sleep(random.uniform(0.2, 0.8))

    except KeyboardInterrupt:
        print("Stopping. Saving state...")
        save_state(state)

    print("Done.")

if __name__ == "__main__":
    main()
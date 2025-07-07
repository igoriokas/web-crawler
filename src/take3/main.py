import os
import time
import random
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from state import CrawlerState  # Save the class in crawler_state.py

DOMAIN = "books.toscrape.com"
START_URL = f"https://{DOMAIN}/"
MAX_DEPTH = 5
HEADERS = {
    "User-Agent": "MyResearchCrawler/1.0 (contact: crawler@homework.com)"
}

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
            full_url = urljoin(base_url, href).split('#')[0]
            if full_url.startswith(START_URL):
                yield full_url

def fetch_url(url, max_retries=2):
    backoff = 1
    for _ in range(max_retries):
        try:
            # Inject random failure (30% chance)
            if random.random() < 0.3:
                raise requests.RequestException("Simulated random failure for testing")

            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                return resp.text
        except requests.RequestException as e:
            print(f"Error: {e}")
        time.sleep(backoff)
        backoff *= 2
    return None

def save_html(url, html):
    filename = url.replace(START_URL, "").replace("/", "_") or "index"
    path = f"pages/{filename}.html"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

def main():
    state = CrawlerState()

    # Enqueue starting URL if queue is empty
    if not state.has_work():
        state.enqueue_url(START_URL, 0)

    try:
        while True:
            row = state.peek_url()
            if not row:
                print("Queue empty.")
                break

            id, url, depth, retries = row
            print(f"[id {id} depth {depth}, retry {retries}] Crawling: {url}")

            state.update_last_attempt(url)
            html = fetch_url(url)

            if html:
                save_html(url, html)
                state.mark_success(id, url, depth)
                if depth < MAX_DEPTH:
                    for link in extract_links(html, url):
                        if not state.already_seen(link):
                            state.enqueue_url(link, depth + 1)
            else:
                state.mark_failure(id, url, depth, retries)

            time.sleep(random.uniform(0.2, 0.8))
    except KeyboardInterrupt:
        print("Interrupted. Stopping...")

    finally:
        state.close()
        print("Finished.")

if __name__ == "__main__":
    main()
import os
import time
import random
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from state import CrawlerState
import logging
import logging.config
import yaml


# Load config from YAML
with open("logging_config.yaml", 'r') as f:
    logging_config = yaml.safe_load(f)

os.makedirs("run", exist_ok=True)
os.chdir("run")

# Apply logging configuration
logging.config.dictConfig(logging_config)
logger = logging.getLogger('crawler.main')

FIRST_PAGE = "https://books.toscrape.com/index.html"
MAX_DEPTH = 2
HEADERS = {
    "User-Agent": "MyResearchCrawler/1.0 (contact: crawler@homework.com)"
}

FIRST_PAGE_PARSED = urlparse(FIRST_PAGE)
DOMAIN = FIRST_PAGE_PARSED.netloc
PROTOCOL = FIRST_PAGE_PARSED.scheme
PRODOMAIN = PROTOCOL + "://" + DOMAIN + "/"


def is_valid_link(href):
    if not href or ':' in href:
        return False
    
    parsed = urlparse(href)
    if parsed.netloc == '' or parsed.netloc.endswith(DOMAIN): # limit to local links
        return True
    
    return False


def extract_links(url, body):
    if url.endswith('.html'):    
        soup = BeautifulSoup(body, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a['href']
            if is_valid_link(href):
                full_url = urljoin(url, href).split('#')[0]
                if full_url.startswith(PRODOMAIN):
                    yield full_url


RETRY_CODES = (429, 500, 502, 503, 504)
NON_RETRY_CODES = (400, 401, 403, 404, 405, 410, 422, 501)

def fetch_url(state, id, url, depth, attempts, max_attempts=2, base_delay=1):
    for attempt in range(attempts+1, max_attempts+1):
        next_wait =  base_delay * (2 ** attempt)
        try:
            logger.info(f"fetch: id {id} | depth {depth} | attempt {attempt} | {url}")
            state.mark_attempt(url)
            response = requests.get(url, timeout=5)

            # inject random failure
            if (id > 9) and (random.random() < 0.8):
                response.status_code = random.choice(RETRY_CODES+NON_RETRY_CODES)
                logger.warning(f'inject random failure, status_code {response.status_code}')

            # check if retriable status_code
            if response.status_code in RETRY_CODES:
                next_wait = int(response.headers.get("Retry-After",  next_wait))
                if attempt < max_attempts:
                    logger.warning(f"Retryable error {response.status_code}, retrying in {next_wait}s ...")                
                    time.sleep(next_wait)
                    continue
                else:
                    logger.warning(f"Retryable error {response.status_code}, no more attempts left")

            response.raise_for_status()
            return response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:  # retriable exception
            if attempt < max_attempts:
                logger.warning(f"Temporary error: {e}, retrying in {next_wait:.1f}s ...")
                time.sleep(next_wait)
            else:
                logger.warning(f"Temporary error: {e}, no more attempts left")
                raise e
        except requests.exceptions.RequestException as e:
            raise e


def save_file(url, body):
    filename = url.replace(PRODOMAIN, "") or "index"
    path = f"pages/{filename}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


def main():
    with CrawlerState() as state:
        # Enqueue starting URL if blank state
        if state.len() == 0:
            state.enqueue_url(FIRST_PAGE, 0)

        while True:
            try:
                row = state.peek_url()
                if not row:
                    print("ALL DONE")
                    break

                try:
                    id, url, depth, attempts = row
                    body = fetch_url(state, id, url, depth, attempts)
                    save_file(url, body)
                    state.mark_success(url)
                    if depth < MAX_DEPTH:
                        for link in extract_links(url, body):
                            state.enqueue_url(link, depth + 1)
                except Exception as e:
                    state.mark_failure(url, str(e).strip().split('\n')[0])
                    logger.error(f"[id {id}]: {e}")
        
                time.sleep(random.uniform(0.2, 0.8))
            except KeyboardInterrupt:
                logger.critical("Interrupted, stopping ...")
                break


if __name__ == "__main__":
    main()
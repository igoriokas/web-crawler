import os
import time
import random
import requests
from requests import RequestException
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from state import CrawlerState
import logging
import logging.config
import yaml
import utils
import json
import lockfile

import re
from collections import Counter

# Load config from YAML
with open("logging_config.yaml", 'r') as f:
    logging_config = yaml.safe_load(f)

os.makedirs("run", exist_ok=True)
os.chdir("run")

# Apply logging configuration
logging.config.dictConfig(logging_config)
logger = logging.getLogger('crawler.main')

FIRST_PAGE = "https://books.toscrape.com/index.html"
MAX_DEPTH = 3

HEADERS = {
    "User-Agent": "MyResearchCrawler/1.0 (contact: crawler@homework.com)"
}

FIRST_PAGE_PARSED = urlparse(FIRST_PAGE)
DOMAIN = FIRST_PAGE_PARSED.netloc
PROTOCOL = FIRST_PAGE_PARSED.scheme
PRODOMAIN = PROTOCOL + "://" + DOMAIN + "/"


class RetryableError(RequestException): pass
class PageException(RequestException): pass


def is_valid_link(href):
    if not href or ':' in href:
        return False
    
    parsed = urlparse(href)
    if parsed.netloc == '' or parsed.netloc.endswith(DOMAIN): # limit to local links
        return True
    
    return False


def extract_links(state, url, body, depth):
    try:
        if random.random() < 0.05:
            raise RuntimeError('simulated page parsing error')

        if body and (url.endswith('.html')) and (depth < MAX_DEPTH):
            soup = BeautifulSoup(body, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a['href']
                if is_valid_link(href):
                    full_url = urljoin(url, href).split('#')[0]
                    if full_url.startswith(PRODOMAIN):
                        state.enqueue_url(full_url, depth + 1)
    except Exception as e:
        raise PageException(e)


def fetch_url(state, id, url, depth, attempts, max_attempts=2, base_delay=1):
    for attempt in range(attempts+1, max_attempts+1):
        next_wait =  base_delay * (2 ** attempt)
        try:
            logger.info(f"fetch: id {id} | depth {depth} | attempt {attempt} | {url}")
            state.mark_attempt(url)
            response = utils.http_get(url, timeout=5)

            if response.status_code == 200: # currently only status_code 200 is handled
                if not response.text:
                    raise PageException("Reading error, response.text is None")
                return response.text
            elif response.status_code in utils.RETRY_CODES:
                next_wait = int(response.headers.get("Retry-After",  next_wait))
                raise RetryableError(f"Retryable HTTP error [{response.status_code}]")
            else:
                raise PageException(f"Non-Retryable HTTP error [{response.status_code}]")         
        except (RetryableError, requests.Timeout, requests.ConnectionError) as e:  # retriable errors
            if attempt < max_attempts:
                logger.warning(f"Temporary error: {e}: retrying in {next_wait:.1f} secs ...")
                time.sleep(next_wait)
            else:
                logger.warning(f"Temporary error: {e}: no more attempts left")
                raise e
        except Exception as e:
            raise PageException(e)
    raise PageException("Max attempts reached")


def save_file_raw(url, body):
    if body:
        filename = url.replace(PRODOMAIN, "") or "index.html"
        utils.file_write(f"pages/{filename}", body)


def save_file_text(url, body):
    if body and (url.endswith('.html')):
        soup = BeautifulSoup(body, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        filename = url.replace(PRODOMAIN, "") or "index.html"
        filename = filename.replace('.html', ".txt")
        utils.file_write(f"text/{filename}", text)
        return text


def save_word_counts_json(url:str, words:Counter):
    filename = url.replace(PRODOMAIN, "") or "index.html"
    filename = "words/" + filename + ".json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(words, f, ensure_ascii=False, indent='')


def load_word_counts_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return Counter(json.load(f))


def count_words(text):
    try:
        if text and len(text) > 0:
            words = re.findall(r'\b\w+\b', text.lower())
            return Counter(words)
    except Exception as e:
        raise PageException("Failed to count words") from e
    

stop = False
pause = False


def crawler_loop():
    global stop, pause

    with CrawlerState() as state:
        # Enqueue starting URL if blank state
        if state.len() == 0:
            state.enqueue_url(FIRST_PAGE, 0)

        while not stop:
            try:
                while pause:
                    time.sleep(1)

                row = state.peek_url()
                if not row:
                    logger.info("ALL DONE")
                    return

                try:
                    logger.info('-------------------------------------------')
                    id, url, depth, attempts = row
                    body = fetch_url(state, id, url, depth, attempts)
                    extract_links(state, url, body, depth)
                    save_file_raw(url, body)
                    text = save_file_text(url, body)
                    words = count_words(text)
                    save_word_counts_json(url, words)
                    state.update_word_counts_mark_success(words, url)
                    # state.mark_success(url)
                except RequestException as e:   # mark as falure and move to next page
                    state.mark_failure(url, utils.etos(e))
                    logger.error(f"{utils.etos(e)}")
                except OSError as e:    # stop programm 
                    state.decrease_attempt(url) # don't count the attempt
                    logger.critical(f"{utils.etos(e)}")
                    logger.critical(f"Fix environment and restart the crawler")
                    return
        
                time.sleep(random.uniform(0.2, 0.8))
            except KeyboardInterrupt as e:
                logger.critical("Interrupted, stopping ...")
                return


def main():   
    try:
        with lockfile.LockFile():
            crawler_loop()
    except BlockingIOError:
        print("Another crawler process is already running, EXIT")


if __name__ == "__main__":
    main()
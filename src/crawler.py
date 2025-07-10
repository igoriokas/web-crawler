import os
import time
import random
import requests
from requests import RequestException
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from state import CrawlerState
import logging
import json
import re
from collections import Counter

# my imports
import config as cfg
import lockfile
import utils
from exceptions import RetryableError, PageException
import word_counter

logger = logging.getLogger('crawler.main')


# flags to control crawler from UI
stop = False    # graceful termination
pause = False   # temporary pause/resume 


def is_valid_link(href):
    """
    Check if a given URL (href) is a valid internal link within the specified domain.

    Parameters:
        href (str): The URL to validate.

    Returns:
        bool: True if href is a valid internal link within cfg.DOMAIN, False otherwise.
    """

    if not href or ':' in href:
        return False

    parsed = urlparse(href)

    # Domain check: only same domain or relative URLs
    if parsed.netloc and not parsed.netloc.endswith(cfg.DOMAIN):
        return False

    # Whitelisted extensions (or clean URLs)
    allowed_suffixes = ('.html', '.htm', '.txt', '/')

    if not href.lower().endswith(allowed_suffixes):
        # allow "clean" paths like /about or /contact without extension
        path = parsed.path
        if '.' in path.split('/')[-1]:  # has file extension not in list
            return False

    return True


def extract_links(state, url, type, body, depth):
    """
    Parses the HTML content of a given URL to extract and enqueue valid internal links for further crawling.

    Args:
        state: crawler state handler.
        url (str): URL of the current page.
        body (str): The HTML content of the current page.
        depth (int): The current depth level in the crawl hierarchy.

    Raises:
        PageException: If any error occurs during parsing or processing of the page content.

    Behavior:
        - Simulates a parsing error randomly with a 5% chance to test error handling.
        - Only processes the page if:
            * The page is of HTML type and body is present,
            * The current depth is less than the configured maximum depth (cfg.MAX_DEPTH).
        - Filters links using the 'is_valid_link' function.
        - Normalizes links to absolute URLs.
    """

    try:
        if random.random() < 0.05:
            raise RuntimeError('simulated page parsing error')

        if body and (type == 'text/html') and (depth < cfg.MAX_DEPTH):
            soup = BeautifulSoup(body, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a['href']
                if is_valid_link(href):
                    full_url = urljoin(url, href).split('#')[0].rstrip('/')
                    if full_url.startswith(cfg.PRODOMAIN):
                        state.enqueue_url(full_url, depth + 1)
    except Exception as e:
        raise PageException(e)


# TODO: fetch only TEXT docs
def fetch_url(state, id, url, depth, attempts, max_attempts=2, base_delay=1):
    """
    Attempts to fetch the content of a URL with retry logic and exponential backoff.

    Parameters:
        state: crawler state handler.
        id (int or str): sequential ID of the URL, used for logging purposes.
        url (str): The URL to fetch.
        depth (int): The current depth level in the crawl hierarchy.
        attempts (int): Number of previous failed attempts for this URL.
        max_attempts (int, optional): Maximum number of allowed attempts.
        base_delay (int or float, optional): Initial delay in seconds for backoff calculation.

    Returns:
        str: The HTML content of the response if successful (HTTP 200 and non-empty body).

    Raises:
        PageException: On non-retryable HTTP errors, max attempts reached, or any other unexpected exception.
        Indicates that processing of the current URL has failed, but the overall crawl process should continue.

    Behavior:
        - Retries on temporary errors like timeouts or retryable status codes (defined in utils.RETRY_CODES).
        - Uses exponential backoff between retries (e.g., 2^attempt * base_delay).
        - Honors 'Retry-After' header if provided by the server.
        - Logs each attempt with the attempt number, depth, and URL.
    """

    for attempt in range(attempts+1, max_attempts+1):
        next_wait =  base_delay * (2 ** attempt)
        try:
            logger.info(f"fetch: id {id} | depth {depth} | attempt {attempt} | {url}")
            state.mark_attempt(url)
            response = utils.http_get(url, timeout=5)
            content_type = response.headers.get('Content-Type', None)
            logger.info(f'got: [{response.status_code}] {content_type}')

            if response.status_code == 200: # currently only status_code 200 is handled
                if not response.text:
                    raise PageException("Reading error, response.text is None")
                if not content_type:
                    raise PageException("Reading error, response.headers['Content-Type'] not set")
                content_type = content_type.lower().split(';')[0].strip()
                if content_type not in ('text/html','text/plain'):
                    raise PageException(f"Reading error, Content-Type {content_type} not supported")
                return content_type, response.text
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
                raise PageException("Max attempts reached") from e
        except Exception as e:
            raise PageException(e)
    raise PageException("Max attempts reached")


def add_extension_if_missing(url, type):
    url = url.rstrip('/') # Remove trailing slash
    
    if os.path.splitext(url)[1]: # Skip if already has extension
        return url

    # Decide extension
    if type == 'text/html':
        return url + '.html'
    elif type == 'text/plain':
        return url + '.txt'

    return url

def save_file_raw(url, type, body):
    if body:
        filename = url.replace(cfg.PRODOMAIN, "") or "index.html"
        print(filename)
        utils.file_write(f"{cfg.WORKDIR}/pages/{filename}", body)


def save_file_text(url, type, body):
    if type == 'text/html':
        soup = BeautifulSoup(body, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
    else: # 'text/plain"
        text = body

    filename = url.replace(cfg.PRODOMAIN, "") or "index.html"
    filename = filename.replace('.html', ".txt")
    utils.file_write(f"{cfg.WORKDIR}/text/{filename}", text)
    return text


def save_word_counts_json(url:str, words:Counter):
    filename = url.replace(cfg.PRODOMAIN, "") or "index.html"
    filename = f"{cfg.WORKDIR}/words/" + filename + ".json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(words, f, ensure_ascii=False, indent='')


def crawler_loop():
    global stop, pause

    with CrawlerState() as state:
        # Enqueue starting URL if blank state
        if state.len() == 0:
            state.enqueue_url(cfg.START_URL, 0)

        while not stop:
            try:
                while pause:
                    time.sleep(1)

                row = state.peek_url()
                if not row:
                    logger.info("ALL DONE")
                    return

                try:
                    logger.info('')
                    id, url, depth, attempts = row
                    type, body = fetch_url(state, id, url, depth, attempts)
                    extract_links(state, url, type, body, depth)
                    urlext = add_extension_if_missing(url, type)
                    save_file_raw(urlext, type, body)
                    text = save_file_text(urlext, type, body)
                    words = word_counter.count_words(text)
                    save_word_counts_json(urlext, words)
                    state.update_word_counts_mark_success(words, url)
                except RequestException as e:   # mark as falure and move to next page
                    state.mark_failure(url, utils.etos(e))
                    logger.error(f"{utils.etos(e)}")
                except Exception as e:    # stop programm 
                    state.decrease_attempt(url) # don't count the attempt
                    logger.critical(f"{utils.etos(e)}")
                    logger.critical(f"Fix environment and restart the crawler")
                    return
        
                time.sleep(cfg.GET_PAGE_DELAY)
            except KeyboardInterrupt as e:
                logger.critical("Interrupted, stopping ...")
                return


def main():   
    """
    Entry point of the crawler.

    Attempts to acquire an exclusive lock to ensure that only one instance of the
    crawler runs at a time. If the lock is successfully acquired, it starts the main
    crawling loop. 
    """

    try:
        with lockfile.LockFile():
            crawler_loop()
    except BlockingIOError:
        print("Another crawler process is already running, EXIT") # intentionally not logging


if __name__ == "__main__":
    main()
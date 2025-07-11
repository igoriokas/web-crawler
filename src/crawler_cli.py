"""
Web crawler module.

- Crawl Strategy
    The crawler uses a breadth-first search (BFS) approach to traverse the website hierarchy. 
    BFS was chosen over depth-first search (DFS) because it allows the crawler to process all pages at the current depth before moving deeper. 
    This approach improves page turnover speed, enabling faster discovery and processing of new pages. 
    Additionally, BFS helps limit memory usage by avoiding deep recursive paths, making the crawling process more efficient and scalable.

"""

import os
import time
import random
import requests
from requests import RequestException
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging
import json
from collections import Counter

# my imports
import config as cfg
import lockfile
import utils
from exceptions import RetryableError, PageException
import word_counter
from state import CrawlerState

logger = logging.getLogger('crawler.main')


# Flags to control crawler from UI
stop = False    # graceful termination
pause = False   # temporary pause/resume 

# Allowed content-types and mapping to file extention
content_type_to_ext = {
    'text/html': '.html',
    'text/plain': '.txt'
}


def is_valid_link(href):
    """
    Determine whether the given URL (href) is a valid internal link within the specified domain.

    A link is considered valid if:
    - It is not empty and does not contain a scheme (e.g., "mailto:", "javascript:").
    - It is either a relative URL or belongs to the configured domain (cfg.DOMAIN).
    - It ends with a whitelisted suffix (e.g., .html, .txt, /), or appears to be a "clean" path without a file extension.

    Parameters:
        href (str): The URL to validate.

    Returns:
        bool: True if the link is considered valid and internal, False otherwise.
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
    Extract and enqueue valid internal links from an HTML page.

    - Parses HTML content if the Content-Type is 'text/html' and depth < MAX_DEPTH.
    - Filters links using is_valid_link.
    - Normalizes to absolute URLs and removes fragments and trailing slashes.
    - Enqueues valid internal links for further crawling.
    - Randomly simulates a parsing error (5% chance) to test error handling.
    - Traverses website hierarchy in a breadth-first search (BFS) manner
      to improve page turnover speed and reduce memory usage compared to depth-first search (DFS).

    Parameters:
        state: crawler state handler.
        url (str): URL of the current page.
        type (str): Content-Type of the page (e.g., 'text/html').
        body (str): The HTML content of the current page.
        depth (int): The current depth level in the crawl hierarchy.

    Raises:
        PageException: On any parsing or processing error.
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
                if content_type not in content_type_to_ext.keys():
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
    """
    Add a file extension to the URL based on its Content-Type, if missing.

    - Raises PageException if the content type is not supported.

    Parameters:
        url (str): The URL to check and possibly modify.
        type (str): The Content-Type of the response (e.g., 'text/html').

    Returns:
        str: The URL with an appropriate extension if one was missing.
    """
    url = url.rstrip('/') # Remove trailing slash
    
    if os.path.splitext(url)[1]: # Skip if already has an extension
        return url

    # Pick extension
    if type in content_type_to_ext:
        return url + content_type_to_ext[type]
    else:
        raise PageException(f'Content-Type {type} not supported')


def url_to_filepath(url, type):
    """
    Convert a URL to a relative file path for saving content.

    Parameters:
        url (str): The full URL of the resource.
        type (str): The Content-Type of the resource (e.g., 'text/html').

    Returns:
        str: A relative file path suitable for saving the resource locally.
    """
    url = add_extension_if_missing(url, type)
    filename = url.replace(cfg.PRODOMAIN, "") or "index.html"
    return filename


def save_file_raw(filename:str, body:str):
    """
    Save raw content to a file if the body is not empty.

    Parameters:
        filename (str): Relative path to save the file as.
        body (str): The content to be saved.
    """
    if body:
        utils.file_write(f"{cfg.WORKDIR}/pages/{filename}", body)


def save_file_text(filename:str, type, body):
    """
    Extract and save readable text content from HTML or plain text.

    Parameters:
        filename (str): Original filename (typically ends with .html).
        type (str): The Content-Type of the response ('text/html' or 'text/plain').
        body (str): The content to be saved.

    Returns:
        str: Extracted or original plain text content. To be used later for word counting.
    """
    if type == 'text/html':
        soup = BeautifulSoup(body, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
    else: # 'text/plain"
        text = body

    filename = filename.replace('.html', ".txt")
    utils.file_write(f"{cfg.WORKDIR}/text/{filename}", text)
    return text


def save_word_counts_json(filename:str, word_counter:Counter):
    """
    Save word frequency counts as a JSON file.

    Parameters:
        filename (str): Original filename to base the JSON filename on.
        word_counter (Counter): A Counter object mapping words to their frequencies.
    """
    if not word_counter:
        return    

    filename = filename.replace('.html', ".json")
    filename = filename.replace('.txt', ".json")
    filename = f"{cfg.WORKDIR}/words/" + filename
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(word_counter, f, ensure_ascii=False, indent='')


def crawler_loop():
    """
    Main loop controlling the crawling process.

    - Initializes crawler state and enqueues the start URL if none exists.
    - Continuously processes URLs in a BFS manner until stopped.
    - Respects pause and stop flags to control execution flow.
    - For each URL:
        * Fetches the page content.
        * Extracts and enqueues new internal links.
        * Saves raw and text versions of the page.
        * Counts word frequencies and saves them as JSON.
        * Updates crawler state marking success or failure.
    - Handles request-related errors by marking failures and continuing.
    - Stops gracefully on critical errors or keyboard interrupt.
    - Applies delay between page fetches as configured.
    """
    global stop, pause

    with CrawlerState() as state:
        # Enqueue starting URL if blank state, resume previous run overwise
        if state.len() == 0:
            logger.info(f'START NEW CRAWL: {cfg.START_URL} -> {cfg.WORKDIR} (depth {cfg.MAX_DEPTH} hops)')
            state.enqueue_url(cfg.START_URL, 0)
        else:
            url, depth = state.start_url()
            logger.info(f'RESUME PREVIOUS CRAWL: {url} -> {cfg.WORKDIR} (depth {cfg.MAX_DEPTH} hops)')
        time.sleep(3)


        while not stop:
            try:
                while pause:
                    time.sleep(1)

                row = state.peek_url()
                if not row:
                    url, depth = state.start_url()
                    logger.info(f'CRAWL COMPLETED: {url} -> {cfg.WORKDIR} (depth {cfg.MAX_DEPTH} hops)')
                    return

                try:
                    logger.info('')
                    id, url, depth, attempts = row
                    type, body = fetch_url(state, id, url, depth, attempts)
                    extract_links(state, url, type, body, depth)
                    filepath = url_to_filepath(url, type)
                    save_file_raw(filepath, body)
                    text = save_file_text(filepath, type, body)
                    words = word_counter.count_words(text)
                    save_word_counts_json(filepath, words)
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
    crawler runs at a time in given WORKDIR. 
    If the lock is successfully acquired, it starts the main crawling loop. 
    """
    try:
        with lockfile.LockFile():
            crawler_loop()
    except BlockingIOError:
        print(f"Another crawler process is already running in {cfg.WORKDIR}, EXIT") # intentionally not logging
        exit()


if __name__ == "__main__":
    cfg.argparse_and_init('crawler-cli')
    logger.info('Start in CLI mode')
    main()
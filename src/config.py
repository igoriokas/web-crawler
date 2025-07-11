import argparse
import json
import yaml
import os
import time
import logging.config
from urllib.parse import urlparse
from pathlib import Path


logger = logging.getLogger('crawler.config')

# --------------------------------------------------------
# Simple configuration settings for the crawler.
# This plain Python-based config is easy to read, modify and flexible enough for the current project needs.
#
# For more complex applications, an external config file (YAML, JSON, ...) could be used.
# Here, simplicity is preferred.
# --------------------------------------------------------

# Logging config file
LOGGING_CONFIG_FILE = "logging.yaml"

# Working directory for the crawler.
# All crawler data (HTML files, logs, state, etc.) will be saved here.
# The directory will be created automatically if it doesn't exist.
WORKDIR = "./data_quotes"

# Starting point and scope for the crawler
# It anchors the allowed domain and URL prefix the crawler should stay within
START_URL = "https://quotes.toscrape.com"
# START_URL = "https://books.toscrape.com/index.html"
# START_URL = "https://en.wikipedia.org/wiki/NASA"

# Limit on link-following depth
MAX_DEPTH = 2

# Pause between page fetches (in seconds), to avoid overloading the server
GET_PAGE_DELAY = 0.1

# Default HTTP headers sent with each request
DEFAULT_HEADERS = {
    'User-Agent': 'MyResearchCrawler/1.0 (contact: mycrawler@homework.com)',
    'Accept': 'text/html, text/plain'
}


# --------------------------------------------------------
#   Derived config (DO NOT MODIFY) 
# --------------------------------------------------------

PROTOCOL = DOMAIN = PRODOMAIN = LOCK_FILE = DB_PATH = LOGFILE = NO_UI = None

def argparse_and_init(description):
    global START_URL, WORKDIR, MAX_DEPTH, PROTOCOL, DOMAIN, PRODOMAIN, LOCK_FILE, DB_PATH, LOGFILE, NO_UI

    # Parse program arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("url", type=str, help="Starting URL and scope (https://quotes.toscrape.com)")
    parser.add_argument("workdir", type=str, help="Working directory, for output and state (data-quotes)")
    parser.add_argument("-d", "--depth", type=int, default=1, help="Max crawl depth (default: %(default)s)")
    parser.add_argument("-no-ui", action="store_true", help="Run in non-UI mode (headless)")
    args = parser.parse_args()

    NO_UI = args.no_ui
    WORKDIR = args.workdir
    START_URL = args.url
    MAX_DEPTH = args.depth

    # Apply logging config
    os.makedirs(WORKDIR, exist_ok=True)
    with open(LOGGING_CONFIG_FILE, 'r') as f:
        logging_config = yaml.safe_load(f)
    logging_config['handlers']['file']['filename'] = f'{WORKDIR}/log.log' # override logfile path to be inside WORKDIR, need to improve this approach
    logging.config.dictConfig(logging_config) # Apply logging configuration, do it after workdir is created

    # Load previous config if exists
    cfg = Path(f'{WORKDIR}/config.json')
    if cfg.is_file(): # resume previous crawl
        with open(cfg, 'r') as f:
            oldcfg = json.load(f)
            START_URL = oldcfg['url']
            MAX_DEPTH = oldcfg['depth']
        logger.info(f"RESUME PREVIOUS CRAWL: {START_URL} -> {WORKDIR} (max depth: {MAX_DEPTH} hops)")
    else:   # start new crawl
        with open(cfg, 'w') as f:
            json.dump(vars(args), f, indent=2)
        logger.info(f"START NEW CRAWL: {START_URL} -> {WORKDIR} (max depth: {MAX_DEPTH} hops)")
    input('Press Enter to continue ...')


    _START_URL_PARSED = urlparse(START_URL)
    PROTOCOL = _START_URL_PARSED.scheme     # 'https'
    DOMAIN = _START_URL_PARSED.netloc       # 'example.com'             domain the crawler should stay within
    PRODOMAIN = PROTOCOL + "://" + DOMAIN   # 'https://example.com/'    prefix the crawler should stay within

    LOCK_FILE = f'{WORKDIR}/lock'
    DB_PATH = f'{WORKDIR}/state.db'
    LOGFILE = f'{WORKDIR}/log.log'





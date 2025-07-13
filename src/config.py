import argparse
import json
import yaml
import os
import shutil
import logging.config
from urllib.parse import urlparse
from pathlib import Path


logger = logging.getLogger('crawler.config')

# --------------------------------------------------------
# Simple configuration settings for the crawler.
# This plain Python-based config is easy to read, modify and flexible enough for the current project needs.
#
# MOST IMPORTANT SETTINGS (WORKDIR, START_URL, MAX_DEPTH) ARE LOADED FROM COMMAND LINE ARGUMENTS
#
# For more complex applications, an external config file (YAML, JSON, ...) could be used.
# Here, simplicity is preferred.
# --------------------------------------------------------

# Logging config file
LOGGING_CONFIG_FILE = "logging.yaml"

# Pause between page fetches (in seconds), to avoid overloading the server
GET_PAGE_DELAY = 0.1

# Default HTTP headers sent with each request
DEFAULT_HEADERS = {
    'User-Agent': 'MyResearchCrawler/1.0 (contact: mycrawler@homework.com)',
    'Accept': 'text/html, text/plain'
}

# Maximum number of allowed attempts
MAX_ATTEMPTS = 2

# Enable simulated errors injections, for testing
INJECT_ERRORS = False

# --------------------------------------------------------
#   Derived config (DO NOT MODIFY) 
#   Those settings are loaded or derived from command line arguments
# --------------------------------------------------------

# Working directory for the crawler.
# All crawler data (HTML files, logs, state, etc.) will be saved here.
# The directory will be created automatically if it doesn't exist.
WORKDIR = None

# Starting point and scope for the crawler
# It anchors the allowed domain and URL prefix the crawler should stay within
START_URL = None

# Limit on link-following depth
MAX_DEPTH = None

PROTOCOL = DOMAIN = PRODOMAIN = None
LOCK_FILE = DB_PATH = LOG_FILE = COUNTS_FILE = REPORT_FILE = None
NO_UI = None

def argparse_and_init(description):
    global START_URL, WORKDIR, MAX_DEPTH, MAX_ATTEMPTS, PROTOCOL, DOMAIN, PRODOMAIN, LOCK_FILE, DB_PATH, LOG_FILE, NO_UI, COUNTS_FILE, REPORT_FILE, INJECT_ERRORS

    # Parse program arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("url", type=str, help="Starting URL and scope (https://quotes.toscrape.com)")
    parser.add_argument("workdir", type=str, help="Working directory, for output and state (data-quotes)")
    parser.add_argument("-d", dest='max_depth', type=int, default=1, help="Max crawl depth (default: %(default)s)")
    parser.add_argument("-a", dest='max_attempts', type=int, default=2, help="Max allowed attempts (default: %(default)s)")
    parser.add_argument("-no-ui", action="store_true", help="Run in non-UI mode (headless)")
    parser.add_argument("-e", dest='inject_errors', action="store_true", help="Enable random error injection (testing)")
    parser.add_argument("-p", dest='purge', action="store_true", help="Purge workdir to start from scratch (advanced)")
    args = parser.parse_args()

    NO_UI = args.no_ui
    INJECT_ERRORS = args.inject_errors
    MAX_ATTEMPTS = args.max_attempts
    WORKDIR = args.workdir
    START_URL = args.url
    MAX_DEPTH = args.max_depth

    if args.purge and os.path.exists(WORKDIR):
        try:
            input('WARNING: purging workdir will delete all previously downloaded content, press Enter to continue ...')
        except KeyboardInterrupt:
            print(' EXIT')
            exit()
        shutil.rmtree(WORKDIR)        

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
            MAX_DEPTH = oldcfg['max_depth']
        logger.info(f"RESUME PREVIOUS CRAWL: {START_URL} -> {WORKDIR} (max_depth: {MAX_DEPTH}, max_attempts: {MAX_ATTEMPTS})")
    else:   # start new crawl
        with open(cfg, 'w') as f:
            json.dump(vars(args), f, indent=2)
        logger.info(f"START NEW CRAWL: {START_URL} -> {WORKDIR} (max_depth: {MAX_DEPTH}, max_attempts: {MAX_ATTEMPTS})")
    try:
        if INJECT_ERRORS:
            logger.info(f'RANDOM ERROR INJECTION ENABLED')
        input('Press Enter to continue ...')
    except KeyboardInterrupt:
        print(' EXIT')
        exit()


    _START_URL_PARSED = urlparse(START_URL)
    PROTOCOL = _START_URL_PARSED.scheme     # 'https'
    DOMAIN = _START_URL_PARSED.netloc       # 'example.com'             domain the crawler should stay within
    PRODOMAIN = PROTOCOL + "://" + DOMAIN   # 'https://example.com/'    prefix the crawler should stay within

    LOCK_FILE = f'{WORKDIR}/lock'
    DB_PATH = f'{WORKDIR}/state.db'
    LOG_FILE = f'{WORKDIR}/log.log'
    COUNTS_FILE = f'{WORKDIR}/word_counts.json'
    REPORT_FILE = f'{WORKDIR}/report.txt'




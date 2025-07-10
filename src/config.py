import yaml
import os
import logging.config
from urllib.parse import urlparse

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
# This will become the current working directory (CWD) of the process.
# All crawler data (HTML files, logs, state, etc.) will be saved here.
# The directory will be created automatically if it doesn't exist.
WORKDIR = "./data"

# Starting point and scope for the crawler
# It anchors the allowed domain and URL prefix the crawler should stay within
START_URL = "https://books.toscrape.com/index.html"

# Limit on link-following depth
MAX_DEPTH = 3

# Pause between page fetches (in seconds), to avoid overloading the server
GET_PAGE_DELAY = 0.1

# Default HTTP headers sent with each request
DEFAULT_HEADERS = {
    "User-Agent": "MyResearchCrawler/1.0 (contact: mycrawler@homework.com)"
}


# --------------------------------------------------------
#   Derived config (DO NOT MODIFY) 
# --------------------------------------------------------

# Extract parts of the start URL for use in crawling
_START_URL_PARSED = urlparse(START_URL)
PROTOCOL = _START_URL_PARSED.scheme             # 'https'
DOMAIN = _START_URL_PARSED.netloc               # 'example.com'             domain the crawler should stay within
PRODOMAIN = PROTOCOL + "://" + DOMAIN + "/"     # 'https://example.com/'    prefix the crawler should stay within

os.makedirs(WORKDIR, exist_ok=True)
LOCK_FILE = f'{WORKDIR}/lock'
DB_PATH = f'{WORKDIR}/state.db'
LOGFILE = f'{WORKDIR}/log.log'

# Apply logging config
with open(LOGGING_CONFIG_FILE, 'r') as f:
    logging_config = yaml.safe_load(f)
logging_config['handlers']['file']['filename'] = f'{WORKDIR}/log.log' # override logfile path to be inside WORKDIR, need to improve this approach
logging.config.dictConfig(logging_config) # Apply logging configuration, do it after workdir is created


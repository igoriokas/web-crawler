import yaml
from urllib.parse import urlparse

# --------------------------------------------------------
# Simple configuration settings for the crawler.
# This plain Python-based config is easy to read, modify and flexible enough for the current project needs.
#
# For more complex applications, an external config file (YAML, JSON, ...) could be used.
# Here, simplicity is preferred.
# --------------------------------------------------------

# Load logging config from YAML
with open("logging.yaml", 'r') as f:
    logging_config = yaml.safe_load(f)


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
GET_PAGE_DELAY = 0.5 # (secs) pause between page fetches, to avoid overloading the server

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


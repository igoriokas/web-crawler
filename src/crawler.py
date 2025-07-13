"""
Entry point for the web crawler document module.

Initializes configuration by parsing command-line arguments, then decides
whether to launch the crawler in CLI (headless) or UI (interactive) mode.

- Configuration is initialized via `cfg.argparse_and_init('crawler')`.
- Mode selection is controlled by the `cfg.NO_UI` flag:

usage: python crawler.py [-h] [-d DEPTH] [-no-ui] url workdir

positional arguments:
  url                Starting URL and scope (https://quotes.toscrape.com)
  workdir            Working directory, for output and state (data-quotes)

options:
  -h, --help       show this help message and exit
  -d MAX_DEPTH     Max crawl depth (default: 1)
  -a MAX_ATTEMPTS  Max allowed attempts (default: 2)
  -no-ui           Run in non-UI mode (headless)
  -e               Enable random error injection (testing)
  -p               Purge workdir to start from scratch (advanced)

example: python crawler.py https://quotes.toscrape.com data-quotes -d 2 -a 3 -e

"""

import config as cfg

cfg.argparse_and_init('crawler')

if cfg.NO_UI:
    import crawler_cli
    crawler_cli.main()
else:
    import crawler_ui

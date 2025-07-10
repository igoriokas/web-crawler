# Web Crawler project

## current limitations:
* starting web page URL and max_depth is hardcoded in crawler.py
* working directory is switched to *run* dir under current folder


## run Web Crawler:
*  UI: `python crawler_ui.py`
* CLI: `python crawler.py`

## UI screenshot:
![](doc/Screenshot.png)

## Limitations
* Currently, only processing docs with response.headers['Content-Type'] in ('text/html', 'text/plain')
* Currently, if the crawler is restarted midway with a different starting URL while retaining the existing workdir content, the state and content may become inconsistent or corrupted. This behavior should be disabled to prevent such cases.
* Consider naming the workdir based on the domain or starting URL (optionally including a timestamp and max_depth) to improve organization and traceability. While this helps distinguish between different crawl sessions, the design should still allow for safe restarts using the same workdir to continue interrupted work without data loss or inconsistency.
* Need to handle empty word Counter before sending to DB

## Improvements
* Multithreading - Although multithreading is not strictly necessary for the crawler’s purpose—especially since aggressive crawling can trigger rate limits or blacklisting—it may still be worthwhile to implement safe parallel processing as an exercise. This would provide a useful learning opportunity and lay the groundwork for future scalability, while still respecting polite crawling behavior.
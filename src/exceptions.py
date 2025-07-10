"""
Exceptions derived from `RequestException` are used to signal
failures related to the processing of a specific URL. 
Raising these exceptions shall not terminate the entire crawl process.
"""

from requests import RequestException


class RetryableError(RequestException): 
    """
    Indicates a temporary failure that allows retrying the request.
    Raised for transient issues (e.g., timeouts, retryable HTTP status codes), where the operation may succeed if attempted again.
    """
    pass


class PageException(RequestException): 
    """
    Indicates that processing of the current URL has failed, but the overall crawl process should continue.
    """
    pass

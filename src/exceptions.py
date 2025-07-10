
from requests import RequestException


class RetryableError(RequestException): 
    pass


class PageException(RequestException): 
    pass

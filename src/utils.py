"""
Simulated error injection utilities for testing crawler robustness.

This module provides functions that simulate intermittent network, I/O, and parsing
errors based on a random probability, controlled by the `cfg.INJECT_ERRORS` flag.

Functions:
- http_get(url, timeout): Simulates network errors or returns a real HTTP response.
- inject_page_parsing_error(): Randomly raises a parsing error.
- file_write(path, content): Writes content to a file, with occasional simulated I/O errors.

These utilities are useful for testing error handling and recovery logic.
"""

import os
import time
import random
import requests
import logging

import config as cfg

logger = logging.getLogger('crawler.utils')

# HTTP status codes considered transient and eligible for retries
RETRY_CODES = (429, 500, 502, 503, 504)

# HTTP status codes considered permanent failures (not retried)
# FOR SIMULATED ERROR INJECTION USE ONLY !!!
NON_RETRY_CODES = (403, 404, 501)


def etos(e:BaseException):
    """
    Convert an exception to a short string representation:
    <ExceptionType>(<first line of message, trimmed to 100 chars>)
    """
    return f"{e.__class__.__name__}({str(e).strip().split('\n')[0][:100]})"


def simulated_probability():
    """
    Return a random float in [0,1) if error injection is enabled;
    otherwise, always return 1 to avoid triggering simulated errors.
    """
    return random.random() if cfg.INJECT_ERRORS else 1

def http_get(url, timeout):
    """
    Perform an HTTP GET request with optional error injection for testing.

    - With X% probability, raises a simulated ConnectionError or Timeout.
    - With X% probability, returns a fake response with a retriable or non-retriable status code.
    - Otherwise, performs a normal requests.get call.

    Args:
        url: Target URL to fetch.
        timeout: Timeout in seconds for the HTTP request.

    Returns:
        A requests.Response object.
    """
    if simulated_probability() < 0.05:
        raise random.choice([
            requests.ConnectionError('simulated ConnectionError'),
            requests.Timeout('simulated Timeout'),
        ])
        
    if simulated_probability() < 0.1:
        response = requests.models.Response()
        response.status_code = random.choice(RETRY_CODES + NON_RETRY_CODES)
        response._content = 'simulated error'.encode('utf-8')
        response.headers = {}
        response.url = url
        return response

    response = requests.get(url, timeout=timeout)
    return response


def inject_page_parsing_error():
    """
    Simulate a page parsing error with.

    Raises:
        RuntimeError: Simulated parsing failure.
    """
    if simulated_probability() < 0.05:
        raise RuntimeError('simulated page parsing error')


def file_write(path:str, content:str):
    """
    Write content to a file, creating parent directories as needed.
    Optionally simulates disk I/O errors (when error injection is enabled).

    Args:
        path: Full file path to write to.
        content: UTF-8 string content to write.

    Raises:
        FileNotFoundError or OSError if simulated error is triggered.
    """
    if simulated_probability() < 0.00001:
        raise random.choice([
            FileNotFoundError('simulated FileNotFoundError'),
            OSError('simulated DiskIsFull'),
        ])
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


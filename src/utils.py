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


RETRY_CODES = (429, 500, 502, 503, 504)
NON_RETRY_CODES = (403, 404, 501) # 400, 401

def etos(e:BaseException):
    return f"{e.__class__.__name__}({str(e).strip().split('\n')[0][:100]})"


def _random_or_one():
    return random.random() if cfg.INJECT_ERRORS else 1

def http_get(url, timeout):
    if _random_or_one() < 0.05:
        raise random.choice([
            requests.ConnectionError('simulated ConnectionError'),
            requests.Timeout('simulated Timeout'),
        ])
        
    if _random_or_one() < 0.1:
        response = requests.models.Response()
        response.status_code = random.choice(RETRY_CODES+NON_RETRY_CODES)
        response._content = 'simulated error'.encode('utf-8')
        response.headers = {}
        response.url = url
        return response

    response = requests.get(url, timeout=timeout)
    return response


def inject_page_parsing_error():
    if _random_or_one() < 0.05:
        raise RuntimeError('simulated page parsing error')


def file_write(path:str, content:str):
    if _random_or_one() < 0.005:
        raise random.choice([
            FileNotFoundError('simulated FileNotFoundError'),
            OSError('simulated DiskIsFull'),
        ])
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


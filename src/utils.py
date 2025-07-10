import os
import time
import random
import requests
import logging

logger = logging.getLogger('crawler.utils')


RETRY_CODES = (429, 500, 502, 503, 504)
NON_RETRY_CODES = (403, 404, 501) # 400, 401

def etos(e:BaseException):
    return f"{e.__class__.__name__}({str(e).strip().split('\n')[0][:100]})"

def http_get(url, timeout):
    if random.random() < 0.05:
        raise random.choice([
            requests.ConnectionError('simulated ConnectionError'),
            requests.Timeout('simulated Timeout'),
        ])
        
    if random.random() < 0.2:
        response = requests.models.Response()
        response.status_code = random.choice(RETRY_CODES+NON_RETRY_CODES)
        response._content = 'simulated error'.encode('utf-8')
        response.headers = {}
        response.url = url
        return response

    response = requests.get(url, timeout=timeout)
    return response


def file_write(path:str, content:str):
    if random.random() < 0.0:
        raise random.choice([
            FileNotFoundError('simulated FileNotFoundError'),
            OSError('simulated DiskIsFull'),
        ])
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


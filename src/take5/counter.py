import logging
import glob
import json
from collections import Counter

def summ_counters_folder(folder:str):
    totals = Counter()
    filenames = glob.glob(f'{folder}/**/*.json', recursive=True)
    for filename in filenames:
        with open(filename, 'r', encoding='utf-8') as f:
            totals.update(Counter(json.load(f)))
    return totals, len(filenames)
import glob
import json
import re
import pandas as pd
from collections import Counter

import exceptions

def count_words(text):
    try:
        if text and len(text) > 0:
            words = re.findall(r'\b\w+\b', text.lower())
            return Counter(words)
    except Exception as e:
        raise exceptions.PageException("Failed to count words") from e


def summ_counters_folder(folder:str):
    totals = Counter()
    filenames = glob.glob(f'{folder}/**/*.json', recursive=True)
    for filename in filenames:
        with open(filename, 'r', encoding='utf-8') as f:
            totals.update(Counter(json.load(f)))
    return totals, len(filenames)

def summ_counters_folder_df(folder:str):
    totals, _ = summ_counters_folder(folder)
    return pd.DataFrame(totals.items(), columns=['word','count']).sort_values(['count', 'word'], ascending=[False, True]).reset_index(drop=True)


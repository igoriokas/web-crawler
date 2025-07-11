"""
This helper module provides utilities for counting word frequencies in plain text and aggregating JSON word count files.

- Words are extracted using basic word boundary regex.
- JSON word counts are assumed to be saved as dictionaries: { "word": count, ... }.
- Aggregation functions allow computing total word stats across crawled pages.

"""

import glob
import json
import re
import pandas as pd
from collections import Counter

import exceptions

def count_words(text):
    """
    Count word frequencies in a given text string.

    - Converts text to lowercase.
    - Extracts words using word boundaries ('\b\w+\b'), which match
      sequences of alphanumeric characters (letters, digits, and underscores)
      that are separated by non-word characters like spaces or punctuation.
    - Returns a Counter of word -> frequency.

    Parameters:
        text (str): The text to process.

    Returns:
        Counter: Mapping of word to frequency.

    Raises:
        PageException: If text is invalid or processing fails.
    """
    try:
        if text and len(text) > 0:
            words = re.findall(r'\b\w+\b', text.lower())
            return Counter(words)
    except Exception as e:
        raise exceptions.PageException("Failed to count words") from e


def sum_counters_folder(folder:str):
    """
    Aggregate word counts from all JSON files in a folder (recursively).

    - Reads each JSON file and treats it as a Counter.
    - Sums all counters into a single total.

    Parameters:
        folder (str): Path to the directory containing JSON word count files.

    Returns:
        tuple:
            - Counter: Aggregated word frequencies.
            - int: Number of files processed.
    """
    totals = Counter()
    filenames = glob.glob(f'{folder}/**/*.json', recursive=True)
    for filename in filenames:
        with open(filename, 'r', encoding='utf-8') as f:
            totals.update(Counter(json.load(f)))
    return totals, len(filenames)

def sum_counters_folder_df(folder:str):
    """
    Aggregate word counts from a folder and return as a sorted DataFrame.

    - Converts the result of `sum_counters_folder` into a pandas DataFrame.
    - Sorts by descending count and then alphabetically.

    Parameters:
        folder (str): Path to directory containing JSON word count files.

    Returns:
        DataFrame: Table of words and their total counts.
    """
    totals, _ = sum_counters_folder(folder)
    return pd.DataFrame(totals.items(), columns=['word','count']).sort_values(['count', 'word'], ascending=[False, True]).reset_index(drop=True)


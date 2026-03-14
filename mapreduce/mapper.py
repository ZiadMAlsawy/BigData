#!/usr/bin/env python3
"""
mapper.py — Reverse Index Mapper
---------------------------------
Hadoop Streaming calls this script once per input split.
It reads lines from stdin, one line at a time.

Each line of input looks like:
    "Some line of text from the book"

The document name (filename) is available through an environment variable
that Hadoop sets automatically:
    mapreduce_map_input_file  →  e.g. "hdfs://.../library/book1.txt"

Output format (one emission per valid word):
    word\tdocument_name
    e.g.  "hadoop\tbook1.txt"
"""

import sys
import os
import re


# The stopwords file is distributed to every node by Hadoop's
# Distributed Cache. It lands in the current working directory
# of the task, so we can open it by filename alone.

def load_stopwords(filepath="stopwords.txt"):
    """
    Read stopwords.txt and return a Python set of stopwords.
    A set is used (not a list) because checking membership in a set
    is O(1) — very fast when filtering millions of words.
    """
    stopwords = set()


    try:
        with open(filepath, "r") as f:
            for line in f:
                word = line.strip().lower()
                if word:
                    stopwords.add(word)
    except FileNotFoundError:
        pass

    return stopwords


def get_document_name():
    """
    Hadoop sets the env variable `mapreduce_map_input_file` to the
    full HDFS path of the file being processed, e.g.:
        hdfs://namenode:9000/user/student/library/book3.txt

    We only want the basename: "book3.txt"
    """
    full_path = os.getenv("mapreduce_map_input_file", "")
    doc_name = os.path.basename(full_path)
    return doc_name


def normalize(line):
    """
    Why?  We want "Hello", "hello", and "hello!" to all count as the
    same word. Without normalization, they'd be treated as 3 different tokens.
    """

    line = line.lower()
    line = re.sub(r"[^a-z0-9\s]", "", line) # ^a for negation, -z0-9 for letters and digits, \s for whitespace

    return line


def main():
    stopwords   = load_stopwords("stopwords.txt")
    doc_name    = get_document_name()


    for line in sys.stdin:

        line = line.strip()
        if not line:
            continue

        line = normalize(line)


        words = line.split() # tokens

        for word in words:
            if word and word not in stopwords:
                print("{}\t{}".format(word, doc_name))


if __name__ == "__main__":
    main()

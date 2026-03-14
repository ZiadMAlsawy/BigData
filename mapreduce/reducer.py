#!/usr/bin/env python3
"""
reducer.py — Reverse Index Reducer
------------------------------------
Hadoop Streaming calls this script after the Shuffle & Sort phase.

The Shuffle & Sort phase:
  - Takes ALL mapper outputs from ALL nodes
  - Sorts them alphabetically by key (the word)
  - Groups all lines that share the same key together
  - Feeds them to this reducer in sorted order

So the reducer receives stdin that looks like this (already sorted & grouped):
    hadoop\tbook1.txt
    hadoop\tbook1.txt
    hadoop\tbook3.txt
    python\tbook2.txt
    python\tbook2.txt
    python\tbook2.txt
    ...

for each unique word, count how many times it appears in each
document and emit ONE line in this format:
    word --> doc_a.txt:count, doc_b.txt:count, ...

Example:
    hadoop --> book1.txt:2, book3.txt:1
"""

import sys
from collections import defaultdict


def emit_index_entry(word, doc_counts):
    """
    Given a word and a dict of {document_name: count},
    print one line in the required format:
        word --> doc_a.txt:3, doc_b.txt:1

    `doc_counts` is a dict, e.g.: {"book1.txt": 2, "book3.txt": 1}
    """
    # this formatting is for python 3.6 compatible with the one installed in the namenode
    parts = ["{}:{}".format(doc, count) for doc, count in doc_counts.items()]
    rhs   = ", ".join(parts)
    print("{} --> {}".format(word, rhs))



"""
Key insight: because Hadoop has already SORTED the input by word,
all lines for the same word arrive consecutively.
We can detect a "new word" the moment the word changes, and that
is our signal to emit the accumulated result for the previous word.
"""


def main():
    current_word = None          # tracks which word we are currently counting
    doc_counts   = defaultdict(int)  # {"book1.txt": 3, "book2.txt": 1, ...}
    #
    # defaultdict(int) is like a regular dict but automatically initializes
    # any missing key to 0, so `doc_counts["book1.txt"] += 1` works even
    # on the first occurrence without a KeyError.

    for line in sys.stdin:

        line = line.strip()

        parts = line.split("\t")
        if len(parts) != 2:
            continue
        word, doc_name = parts

        if word != current_word:
            if current_word is not None:
                emit_index_entry(current_word, doc_counts)
            current_word = word
            doc_counts   = defaultdict(int)

        # TODO (d): Increment the count for this document.
        #           `doc_counts[doc_name] += 1`
        doc_counts[doc_name] += 1

    """
    The loop above emits a word's entry only when it sees a DIFFERENT word.
    But the very last word in the sorted input never triggers that condition,
    so we must emit it manually here, after the loop ends.

    """
    
    if current_word is not None:
        emit_index_entry(current_word, doc_counts)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, OSError):
        pass

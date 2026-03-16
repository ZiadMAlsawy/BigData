import sys
from collections import defaultdict


def emit_index_entry(word, doc_counts):
    parts = ["{}:{}".format(doc, count) for doc, count in doc_counts.items()]
    rhs   = ", ".join(parts)
    print("{} --> {}".format(word, rhs))


# def main():
current_word = None          # tracks which word we are currently counting
doc_counts   = defaultdict(int)  # {"1.txt": 3, "2.txt": 1, ...}

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

    doc_counts[doc_name] += 1


if current_word is not None:
    emit_index_entry(current_word, doc_counts)


# if __name__ == "__main__":
#     try:
#         main()
#     except (BrokenPipeError, OSError):
#         pass

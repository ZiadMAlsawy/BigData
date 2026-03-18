import sys
import os
import re

def load_stopwords(filepath):
    stopwords = set()
    try:
        with open(filepath, "r") as file:
            for line in file:
                word = line.strip().lower()
                if word:
                    stopwords.add(word)
    except FileNotFoundError:
        pass

    return stopwords


def get_document_name():
    full_path = os.getenv("mapreduce_map_input_file", "")
    doc_name = os.path.basename(full_path)
    return doc_name


def normalize(line):
    line = line.lower()
    line = re.sub(r"[^a-z0-9\s]", "", line) # ^a for negation, -z0-9 for letters and digits, \s for whitespace
    return line


# def main():
stopwords = load_stopwords("../stopwords.txt")
doc_name = get_document_name()
print(doc_name)
count = 0
total_tokens = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    line = normalize(line)


    tokens = line.split() # tokens
    total_tokens += len(tokens)

    for token in tokens:
        if token not in stopwords:
            print("{}\t{}".format(token, doc_name))
        else:
            count += 1
i = count/total_tokens
print(i)
# if __name__ == "__main__":
#     main()

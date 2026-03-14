#!/bin/bash


BOOK="../Assignment 1/Books/1.txt"
STOPWORDS="../Assignment 1/stopwords.txt"

# Copy stopwords to current dir so mapper.py can find it by filename
cp "$STOPWORDS" stopwords.txt

echo "=== Running local test on: $BOOK ==="
echo ""

# Simulate Hadoop environment variable (normally set by Hadoop automatically)
export mapreduce_map_input_file="$BOOK"

# Pipeline: mapper → sort (shuffle) → reducer
cat "$BOOK" \
  | python mapper.py \
  | sort \
  | python reducer.py \
  # | head -30     # show only the first 30 output lines

echo ""
echo "=== Done. Check output above. ==="

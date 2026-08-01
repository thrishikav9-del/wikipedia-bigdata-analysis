#!/usr/bin/env python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    try:
        word, count = line.strip().split('\t', 1)
        count = int(count)
        
        if current_word == word:
            current_count += count
        else:
            if current_word:
                print("{}\t{}".format(current_word, current_count))
            current_word = word
            current_count = count
    except:
        pass

if current_word:
    print("{}\t{}".format(current_word, current_count))

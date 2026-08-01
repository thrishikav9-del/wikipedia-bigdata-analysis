#!/usr/bin/env python
import sys
import json
import re

for line in sys.stdin:
    try:
        article = json.loads(line)
        title = article.get('title', '')
        # Use format() instead of f-strings
        words = re.findall(r'\b[a-z]{3,}\b', title.lower())
        for word in words:
            print("{}\t{}".format(word, 1))
    except:
        pass

import json
import pandas as pd
from collections import Counter

print("=== WIKIPEDIA ANALYSIS ===")

# Load data
data = []
with open('wikipedia_output.json', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 100000:  # Load 100K articles
            try:
                data.append(json.loads(line))
            except:
                continue
        else:
            break

print(f"✓ Loaded {len(data):,} articles")

df = pd.DataFrame(data)

# 1. Year analysis
print("\n1. YEAR-WISE GROWTH:")
year_counts = df['year'].value_counts().sort_index()
for year in list(year_counts.index)[:5]:
    print(f"   {year}: {year_counts[year]:,} articles")

# 2. Category analysis
print("\n2. TOP CATEGORIES:")
all_cats = []
for cats in df['categories']:
    all_cats.extend(cats)

if all_cats:
    top_cats = Counter(all_cats).most_common(10)
    for cat, count in top_cats:
        print(f"   {cat}: {count:,}")
else:
    print("   No categories found")

# 3. Text length
print("\n3. TEXT LENGTH:")
if 'text_length' in df.columns:
    lengths = pd.to_numeric(df['text_length'], errors='coerce')
    print(f"   Average: {lengths.mean():.0f} chars")
    print(f"   Max: {lengths.max():.0f} chars")
    print(f"   Min: {lengths.min():.0f} chars")

print("\n✓ ANALYSIS COMPLETE")
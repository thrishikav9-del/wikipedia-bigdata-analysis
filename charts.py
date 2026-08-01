import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

print("=== CREATING CHARTS ===")

# Load data
data = []
with open('wikipedia_output.json', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 50000:  # Load 50K for charts
            try:
                data.append(json.loads(line))
            except:
                continue
        else:
            break

print(f"✓ Loaded {len(data):,} articles")

df = pd.DataFrame(data)

# Chart 1: Year growth
plt.figure(figsize=(10, 5))
year_counts = df['year'].value_counts().sort_index()
plt.bar(year_counts.index.astype(str), year_counts.values)
plt.title('Articles per Year')
plt.xlabel('Year')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('year_chart.png')
print("✓ Saved year_chart.png")

# Chart 2: Top categories
plt.figure(figsize=(10, 6))
all_cats = []
for cats in df['categories']:
    all_cats.extend(cats)

if all_cats:
    top_cats = Counter(all_cats).most_common(15)
    categories = [c[0] for c in top_cats]
    counts = [c[1] for c in top_cats]
    
    plt.barh(categories, counts)
    plt.title('Top 15 Categories')
    plt.xlabel('Number of Articles')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('categories_chart.png')
    print("✓ Saved categories_chart.png")

print("✓ ALL CHARTS CREATED")
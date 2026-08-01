import matplotlib.pyplot as plt
import numpy as np

print("="*70)
print("PHASE 5: VISUALIZATION - WIKIPEDIA ANALYSIS CHARTS")
print("="*70)

# ============================================
# CHART 1: ML Clustering Results (from Phase 4)
# ============================================
print("\n Creating Chart 1: ML Cluster Distribution...")

# Data from your cluster_summary.txt
clusters = ['Cluster 0', 'Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4', 'Cluster 5']
articles = [825052, 721412, 58556, 122547, 8865, 509689]
colors = ['#4CAF50', '#FF9800', '#2196F3', '#9C27B0', '#F44336', '#FFC107']
explode = (0.05, 0.05, 0, 0, 0.2, 0.05)  # Highlight clusters 0,1,4

plt.figure(figsize=(12, 8))
plt.pie(articles, labels=clusters, autopct='%1.1f%%', 
        colors=colors, explode=explode, startangle=90,
        shadow=True, textprops={'fontsize': 12})
plt.title('Wikipedia Article Clusters (2.2M Articles)', fontsize=16, fontweight='bold')
plt.axis('equal')
plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart1_clusters.png', dpi=300)
print(" Saved: chart1_clusters.png")

# ============================================
# CHART 2: Average Length by Cluster
# ============================================
print("\n Creating Chart 2: Article Length by Cluster...")

avg_length = [6388, 171, 63388, 17158, 199317, 1181]

plt.figure(figsize=(12, 6))
bars = plt.bar(clusters, avg_length, color=colors, edgecolor='black')
plt.title('Average Article Length by Cluster', fontsize=16, fontweight='bold')
plt.xlabel('Cluster', fontsize=12)
plt.ylabel('Average Length (characters)', fontsize=12)
plt.yscale('log')  # Log scale because of huge variation
plt.grid(True, alpha=0.3, axis='y')

# Add value labels on bars
for bar, length in zip(bars, avg_length):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500, 
             f'{length:,}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart2_length.png', dpi=300)
print(" Saved: chart2_length.png")

# ============================================
# CHART 3: Average Categories by Cluster
# ============================================
print("\n Creating Chart 3: Categories by Cluster...")

avg_categories = [3.92, 0.01, 6.42, 18.51, 14.13, 0.0]

plt.figure(figsize=(12, 6))
bars = plt.bar(clusters, avg_categories, color=colors, edgecolor='black')
plt.title('Average Number of Categories by Cluster', fontsize=16, fontweight='bold')
plt.xlabel('Cluster', fontsize=12)
plt.ylabel('Average Categories', fontsize=12)
plt.grid(True, alpha=0.3, axis='y')

# Add value labels
for bar, cat in zip(bars, avg_categories):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2, 
             f'{cat:.2f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart3_categories.png', dpi=300)
print(" Saved: chart3_categories.png")

# ============================================
# CHART 4: Average Year by Cluster
# ============================================
print("\n Creating Chart 4: Timeline by Cluster...")

avg_year = [2024, 2008, 2025, 2025, 2024, 2021]

plt.figure(figsize=(12, 6))
bars = plt.bar(clusters, avg_year, color=colors, edgecolor='black')
plt.title('Average Year of Articles by Cluster', fontsize=16, fontweight='bold')
plt.xlabel('Cluster', fontsize=12)
plt.ylabel('Average Year', fontsize=12)
plt.ylim(2005, 2026)
plt.grid(True, alpha=0.3, axis='y')

# Add value labels
for bar, year in zip(bars, avg_year):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() - 1, 
             str(int(year)), ha='center', va='top', fontsize=10, color='white', fontweight='bold')

plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart4_timeline.png', dpi=300)
print(" Saved: chart4_timeline.png")

# ============================================
# CHART 5: Cluster Composition Summary
# ============================================
print("\n Creating Chart 5: Cluster Summary Table...")

fig, ax = plt.subplots(figsize=(14, 4))
ax.axis('tight')
ax.axis('off')

# Create table data
table_data = [
    ['Cluster', 'Articles', '% of Total', 'Avg Length', 'Avg Categories', 'Avg Year', 'Type'],
    ['0', '825,052', '36.7%', '6,388', '3.92', '2024', 'Standard Modern'],
    ['1', '721,412', '32.1%', '171', '0.01', '2008', 'Old Stubs'],
    ['2', '58,556', '2.6%', '63,388', '6.42', '2025', 'Comprehensive New'],
    ['3', '122,547', '5.5%', '17,158', '18.51', '2025', 'Category-Rich'],
    ['4', '8,865', '0.4%', '199,317', '14.13', '2024', 'Monumental'],
    ['5', '509,689', '22.7%', '1,181', '0.0', '2021', 'Recent Stubs']
]

table = ax.table(cellText=table_data, loc='center', cellLoc='center', colWidths=[0.1, 0.12, 0.1, 0.12, 0.12, 0.1, 0.2])
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.2, 1.5)

# Style the header row
for i in range(7):
    table[(0, i)].set_facecolor('#4472C4')
    table[(0, i)].set_text_props(weight='bold', color='white')

plt.title('Wikipedia Article Clusters - Summary Table', fontsize=16, fontweight='bold', pad=20)
plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart5_table.png', dpi=300)
print(" Saved: chart5_table.png")

# ============================================
# CHART 6: Category Coverage (from Phase 3)
# ============================================
print("\n Creating Chart 6: Category Coverage...")

plt.figure(figsize=(8, 8))
sizes = [1017134, 1228987]  # Categorized vs Uncategorized
labels = ['Categorized', 'Uncategorized']
colors_cat = ['#66b3ff', '#ff9999']
explode_cat = (0.05, 0.05)

plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors_cat,
        explode=explode_cat, startangle=90, shadow=True)
plt.title('Wikipedia Category Coverage (2.2M Articles)', fontsize=16, fontweight='bold')
plt.axis('equal')
plt.tight_layout()
plt.savefig('D:/hadoop-docker/project_results/chart6_coverage.png', dpi=300)
print(" Saved: chart6_coverage.png")

print("\n" + "="*70)
print(" PHASE 5 COMPLETE - 6 CHARTS CREATED!")
print(" All charts saved to: D:\\hadoop-docker\\project_results\\")
print("="*70)
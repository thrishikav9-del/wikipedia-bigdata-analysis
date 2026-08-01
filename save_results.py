from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, size, when

print(" SAVING WIKIPEDIA ANALYSIS RESULTS")

spark = SparkSession.builder \
    .appName("SaveResults") \
    .master("local[*]") \
    .getOrCreate()

# Load data
df = spark.read.json("file:///app/wikipedia_output.json")
df = df.withColumn("text_length", col("text_length").cast("int"))
df = df.withColumn("year", col("year").cast("int"))

total = df.count()
print(f"Total articles: {total:,}")

# Create output directory in container
import os
os.makedirs("/app/project_results", exist_ok=True)

# 1. Year statistics
print("\n Saving year distribution...")
year_stats = df.groupBy("year").count().orderBy("year")
year_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/year_distribution")
print(" Saved: year_distribution/")

# 2. Top categories
print("\n Saving top categories...")
cat_stats = df.select(explode("categories").alias("category")) \
    .groupBy("category").count() \
    .orderBy(desc("count")) \
    .limit(100)
cat_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/top_categories")
print(" Saved: top_categories/")

# 3. Text length distribution
print("\n Saving length distribution...")
df.createOrReplaceTempView("wiki")
length_dist = spark.sql("""
    SELECT 
        CASE 
            WHEN text_length < 100 THEN 'Tiny (<100)'
            WHEN text_length < 1000 THEN 'Small (100-1k)'
            WHEN text_length < 10000 THEN 'Medium (1k-10k)'
            WHEN text_length < 50000 THEN 'Large (10k-50k)'
            ELSE 'Huge (>50k)'
        END as size_category,
        COUNT(*) as articles,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM wiki), 2) as percentage
    FROM wiki
    GROUP BY size_category
    ORDER BY articles DESC
""")
length_dist.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/length_distribution")
print(" Saved: length_distribution/")

# 4. Category coverage
print("\n Saving category coverage...")
coverage = df.withColumn("has_categories", size("categories") > 0) \
    .groupBy("has_categories") \
    .count()
coverage.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/category_coverage")
print(" Saved: category_coverage/")

# 5. Save summary as text
print("\n Creating summary report...")
with open("/app/project_results/summary.txt", "w") as f:
    f.write("="*50 + "\n")
    f.write("WIKIPEDIA ANALYSIS SUMMARY\n")
    f.write("="*50 + "\n\n")
    f.write(f"Total Articles: {total:,}\n\n")
    
    # Get coverage counts
    cov_data = coverage.collect()
    for row in cov_data:
        status = "Categorized" if row[0] else "Uncategorized"
        f.write(f"{status}: {row[1]:,} ({row[1]/total*100:.2f}%)\n")
    
    # Get top category
    top_cat = cat_stats.first()
    f.write(f"\nTop Category: {top_cat['category']} ({top_cat['count']:,})\n")
    
    # Get length distribution
    len_data = length_dist.collect()
    f.write("\nLength Distribution:\n")
    for row in len_data:
        f.write(f"  {row['size_category']}: {row['articles']:,} ({row['percentage']}%)\n")

print(" Saved: summary.txt")
print("\n ALL FILES SAVED to /app/project_results/")
print("   Check D:\\hadoop-docker\\project_results folder")

spark.stop()
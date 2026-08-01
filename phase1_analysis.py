from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, avg, max, min, size, when, round, desc, stddev
from pyspark.sql.types import IntegerType

print("="*70)
print("PHASE 1: COMPLETE WIKIPEDIA ANALYSIS - 2.2M ARTICLES")
print("="*70)

# Initialize Spark
spark = SparkSession.builder \
    .appName("WikipediaCompleteAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

print("\n Loading Wikipedia JSON file...")
# Load data
df = spark.read.json("file:///app/wikipedia_output.json")
df = df.withColumn("text_length", col("text_length").cast(IntegerType()))
df = df.withColumn("year", col("year").cast(IntegerType()))

total = df.count()
print(f" TOTAL ARTICLES LOADED: {total:,}")

# ============================================
# ANALYSIS 1: TEMPORAL ANALYSIS
# ============================================
print("\n" + "="*50)
print("ANALYSIS 1: TEMPORAL PATTERNS (2002-2026)")
print("="*50)

yearly_stats = df.groupBy("year") \
    .agg(
        count("*").alias("article_count"),
        avg("text_length").alias("avg_length"),
        max("text_length").alias("max_length"),
        min("text_length").alias("min_length")
    ) \
    .orderBy("year")

print("\n YEAR-WISE DISTRIBUTION:")
yearly_stats.show(25, truncate=False)

# Peak year
peak_year = yearly_stats.orderBy(desc("article_count")).first()
print(f"\n PEAK YEAR: {peak_year['year']} with {peak_year['article_count']:,} articles")

# ============================================
# ANALYSIS 2: CATEGORY ANALYSIS
# ============================================
print("\n" + "="*50)
print("ANALYSIS 2: CATEGORY DISTRIBUTION")
print("="*50)

# Category coverage
cat_coverage = df.withColumn("has_categories", size("categories") > 0) \
    .groupBy("has_categories") \
    .agg(
        count("*").alias("count"),
        round(count("*") * 100.0 / total, 2).alias("percentage")
    )

print("\n CATEGORY COVERAGE:")
cat_coverage.show()

# Uncategorized count
uncategorized = df.filter(size("categories") == 0).count()
print(f"\n Uncategorized Articles: {uncategorized:,} ({uncategorized/total*100:.2f}%)")

# Top categories
top_cats = df.select(explode("categories").alias("category")) \
    .groupBy("category") \
    .agg(count("*").alias("frequency")) \
    .orderBy(desc("frequency")) \
    .limit(20)

print("\n TOP 20 CATEGORIES:")
top_cats.show(20, truncate=False)

# ============================================
# ANALYSIS 3: CONTENT ANALYSIS
# ============================================
print("\n" + "="*50)
print("ANALYSIS 3: CONTENT CHARACTERISTICS")
print("="*50)

content_stats = df.agg(
    avg("text_length").alias("avg_length"),
    stddev("text_length").alias("std_dev_length"),
    max("text_length").alias("max_length"),
    min("text_length").alias("min_length")
)

print("\n CONTENT STATISTICS:")
content_stats.show(truncate=False)

# Length distribution
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
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM wiki), 2) as percentage,
        ROUND(AVG(text_length), 0) as avg_length
    FROM wiki
    GROUP BY size_category
    ORDER BY 
        CASE size_category
            WHEN 'Tiny (<100)' THEN 1
            WHEN 'Small (100-1k)' THEN 2
            WHEN 'Medium (1k-10k)' THEN 3
            WHEN 'Large (10k-50k)' THEN 4
            ELSE 5
        END
""")

print("\n TEXT LENGTH DISTRIBUTION:")
length_dist.show(truncate=False)

# ============================================
# SAVE RESULTS
# ============================================
print("\n" + "="*50)
print("SAVING RESULTS TO CSV")
print("="*50)

# Create output directory in Docker container
import subprocess
subprocess.run(["docker", "exec", "namenode", "mkdir", "-p", "/tmp/output"])

# Save to CSV files
yearly_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/output/yearly_stats")
top_cats.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/output/top_categories")
length_dist.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/output/length_dist")
cat_coverage.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/output/category_coverage")

print(" Results saved to /app/output/ folder")

# ============================================
# FINAL SUMMARY
# ============================================
print("\n" + "="*70)
print(" PHASE 1 COMPLETE - SUMMARY STATISTICS")
print("="*70)
print(f"""
 DATASET OVERVIEW:
   • Total Articles: {total:,}
   • Time Range: 2002-2026
   • Peak Year: {peak_year['year']} ({peak_year['article_count']:,} articles)

 CATEGORY INSIGHTS:
   • Categorized: {total - uncategorized:,} ({(total-uncategorized)/total*100:.2f}%)
   • Uncategorized: {uncategorized:,} ({uncategorized/total*100:.2f}%)
   • Top Category: '{top_cats.first()['category']}' ({top_cats.first()['frequency']:,} articles)

 CONTENT INSIGHTS:
   • Average Length: {content_stats.first()['avg_length']:.0f} characters
   • Max Length: {content_stats.first()['max_length']:,} characters
   • Min Length: {content_stats.first()['min_length']} characters

 KNOWLEDGE GAPS:
   • Uncategorized Articles: {uncategorized:,}
   • Tiny Articles (<100 chars): {length_dist.filter("size_category = 'Tiny (<100)'").first()['articles']:,}
""")
print("="*70)

spark.stop()
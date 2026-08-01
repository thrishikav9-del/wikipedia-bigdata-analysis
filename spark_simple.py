from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, avg, max, min, size, when
from pyspark.sql.types import IntegerType

print("="*60)
print(" SPARK BIG DATA ANALYSIS - WIKIPEDIA")
print("="*60)

# Initialize Spark
spark = SparkSession.builder \
    .appName("WikipediaBDA") \
    .master("local[*]") \
    .getOrCreate()

print(" Loading 2.2M articles...")
df = spark.read.json("file:///app/wikipedia_output.json")
total = df.count()
print(f" LOADED: {total:,} ARTICLES")

# Convert types
df = df.withColumn("text_length", col("text_length").cast(IntegerType()))
df = df.withColumn("year", col("year").cast(IntegerType()))

print("\n" + "="*60)
print(" BIG DATA ANALYSIS RESULTS")
print("="*60)

# 1. Year distribution
print("\n1.  YEAR-WISE DISTRIBUTION:")
year_stats = df.groupBy("year") \
    .agg(count("*").alias("articles")) \
    .orderBy(col("articles").desc())
year_stats.show(15, truncate=False)

# 2. Content analysis
print("\n2.  CONTENT STATISTICS:")
content_stats = df.agg(
    count("*").alias("total_articles"),
    avg("text_length").alias("avg_length"),
    max("text_length").alias("max_length"),
    min("text_length").alias("min_length")
)
content_stats.show(truncate=False)

# 3. Category analysis
print("\n3.  CATEGORY ANALYSIS:")
# Check category coverage
cat_coverage = df.select(size("categories").alias("cat_count"))
cat_coverage.agg(
    avg("cat_count").alias("avg_categories_per_article"),
    count(when(col("cat_count") > 0, True)).alias("categorized_articles"),
    count(when(col("cat_count") == 0, True)).alias("uncategorized_articles")
).show(truncate=False)

# 4. Top categories
try:
    print("\n4.  TOP 10 CATEGORIES:")
    cat_stats = df.select(explode("categories").alias("category")) \
        .groupBy("category") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(10)
    cat_stats.show(truncate=False)
except:
    print("Categories not available in sample")

# 5. Text length distribution
print("\n5.  TEXT LENGTH DISTRIBUTION:")
df.createOrReplaceTempView("wiki")
spark.sql("""
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
""").show(truncate=False)

# 6. Save results
print("\n SAVING RESULTS TO CSV...")
year_stats.toPandas().to_csv("/app/spark_year_results.csv", index=False)
print(" Saved: spark_year_results.csv")

print("\n" + "="*60)
print(" BIG DATA ACHIEVEMENTS")
print("="*60)
print(f"• Articles processed: {total:,}")
print(f"• Data size: ~424 MB")
print(f"• Processing: Spark Local Mode (Parallel)")
print(f"• Analytics: Temporal + Categorical + Statistical")
print("="*60)

spark.stop()
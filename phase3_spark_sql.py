from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, count, avg, desc

print("="*70)
print("PHASE 3: SPARK SQL - WIKIPEDIA ANALYSIS")
print("="*70)

# Initialize Spark
spark = SparkSession.builder \
    .appName("WikipediaSparkSQL") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Load JSON data as temporary view
print("\n Loading data...")
df = spark.read.json("file:///app/wikipedia_output.json")
df.createOrReplaceTempView("wikipedia")
print(f" Loaded {df.count():,} articles")

# ============================================
# SQL QUERY 1: Yearly article count
# ============================================
print("\n" + "="*50)
print("SQL QUERY 1: Articles per year")
print("="*50)

query1 = """
SELECT 
    year,
    COUNT(*) as article_count,
    ROUND(AVG(text_length), 0) as avg_length
FROM wikipedia
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year
"""
result1 = spark.sql(query1)
result1.show(25, truncate=False)

# ============================================
# SQL QUERY 2: Top categories
# ============================================
print("\n" + "="*50)
print("SQL QUERY 2: Top 20 categories")
print("="*50)

query2 = """
SELECT 
    category,
    COUNT(*) as frequency
FROM wikipedia
LATERAL VIEW EXPLODE(categories) catTable AS category
GROUP BY category
ORDER BY frequency DESC
LIMIT 20
"""
result2 = spark.sql(query2)
result2.show(20, truncate=False)

# ============================================
# SQL QUERY 3: Category coverage
# ============================================
print("\n" + "="*50)
print("SQL QUERY 3: Category coverage analysis")
print("="*50)

query3 = """
SELECT 
    CASE 
        WHEN SIZE(categories) = 0 THEN 'Uncategorized'
        ELSE 'Categorized'
    END as status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM wikipedia), 2) as percentage
FROM wikipedia
GROUP BY CASE WHEN SIZE(categories) = 0 THEN 'Uncategorized' ELSE 'Categorized' END
"""
result3 = spark.sql(query3)
result3.show()

# ============================================
# SQL QUERY 4: Text length distribution
# ============================================
print("\n" + "="*50)
print("SQL QUERY 4: Text length categories")
print("="*50)

query4 = """
SELECT 
    CASE 
        WHEN text_length < 100 THEN 'Tiny (<100)'
        WHEN text_length < 1000 THEN 'Small (100-1k)'
        WHEN text_length < 10000 THEN 'Medium (1k-10k)'
        WHEN text_length < 50000 THEN 'Large (10k-50k)'
        ELSE 'Huge (>50k)'
    END as size_category,
    COUNT(*) as articles,
    ROUND(AVG(text_length), 0) as avg_length,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM wikipedia), 2) as percentage
FROM wikipedia
GROUP BY 
    CASE 
        WHEN text_length < 100 THEN 'Tiny (<100)'
        WHEN text_length < 1000 THEN 'Small (100-1k)'
        WHEN text_length < 10000 THEN 'Medium (1k-10k)'
        WHEN text_length < 50000 THEN 'Large (10k-50k)'
        ELSE 'Huge (>50k)'
    END
ORDER BY 
    CASE size_category
        WHEN 'Tiny (<100)' THEN 1
        WHEN 'Small (100-1k)' THEN 2
        WHEN 'Medium (1k-10k)' THEN 3
        WHEN 'Large (10k-50k)' THEN 4
        ELSE 5
    END
"""
result4 = spark.sql(query4)
result4.show(truncate=False)

# ============================================
# SQL QUERY 5: Yearly category trends
# ============================================
print("\n" + "="*50)
print("SQL QUERY 5: Top categories by year (2025)")
print("="*50)

query5 = """
SELECT 
    year,
    category,
    COUNT(*) as count
FROM wikipedia
LATERAL VIEW EXPLODE(categories) catTable AS category
WHERE year = 2025
GROUP BY year, category
ORDER BY count DESC
LIMIT 10
"""
result5 = spark.sql(query5)
result5.show(truncate=False)

# ============================================
# SQL QUERY 6: Complex analytics - articles with most categories
# ============================================
print("\n" + "="*50)
print("SQL QUERY 6: Top 10 articles by category count")
print("="*50)

query6 = """
SELECT 
    title,
    year,
    SIZE(categories) as num_categories,
    text_length
FROM wikipedia
WHERE SIZE(categories) > 0
ORDER BY num_categories DESC, text_length DESC
LIMIT 10
"""
result6 = spark.sql(query6)
result6.show(truncate=False)

# ============================================
# SQL QUERY 7: Knowledge gap analysis by year
# ============================================
print("\n" + "="*50)
print("SQL QUERY 7: Uncategorized articles by year")
print("="*50)

query7 = """
SELECT 
    year,
    COUNT(*) as total_articles,
    SUM(CASE WHEN SIZE(categories) = 0 THEN 1 ELSE 0 END) as uncategorized,
    ROUND(SUM(CASE WHEN SIZE(categories) = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as uncategorized_pct
FROM wikipedia
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year
"""
result7 = spark.sql(query7)
result7.show(25, truncate=False)

# ============================================
# Save results
# ============================================
print("\n Saving results...")
result1.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/sql_yearly")
result2.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/sql_categories")
result4.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/sql_length")
result7.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/project_results/sql_gaps")

print(" Results saved to /app/project_results/")

print("\n" + "="*70)
print(" PHASE 3 COMPLETE - Spark SQL Analysis Done!")
print("="*70)

spark.stop()
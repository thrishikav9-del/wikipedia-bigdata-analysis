from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import json

spark = SparkSession.builder \
    .appName("Wikipedia_Clean_JSON") \
    .getOrCreate()

print("=" * 80)
print("PROCESSING WIKIPEDIA XML TO CLEAN JSON")
print("=" * 80)

# Read data
raw_df = spark.read.option("lineSep", "</page>").text("hdfs://namenode:9000/datasets/wikipedia/raw/*.bz2")
print(f"Processing {raw_df.count():,} page chunks")

def extract_clean(xml_text):
    """Extract clean data from Wikipedia XML with proper category handling."""
    result = {}
    
    # Title
    title_match = re.search(r'<title>([^<]+)</title>', xml_text)
    result['title'] = title_match.group(1).strip() if title_match else ""
    
    # Page ID
    page_id_match = re.search(r'<id>(\d+)</id>', xml_text)
    result['page_id'] = page_id_match.group(1) if page_id_match else ""
    
    # Year
    year_match = re.search(r'<timestamp>(\d{4})', xml_text)
    result['year'] = year_match.group(1) if year_match else ""
    
    # Text and categories
    text_match = re.search(r'<text[^>]*>([\s\S]*?)</text>', xml_text)
    if text_match:
        text = text_match.group(1)
        result['text_length'] = len(text)
        
        # PROPER CATEGORY EXTRACTION - FIXED!
        categories = []
        
        # Method 1: Find all [[Category:xxx]] patterns
        category_pattern = r'\[\[Category:([^\]|]+)'
        found_categories = re.findall(category_pattern, text)
        
        # Clean and add categories
        for cat in found_categories:
            cat = cat.strip()
            if cat and not cat.startswith(':'):  # Skip hidden categories
                categories.append(cat)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_categories = []
        for cat in categories:
            if cat not in seen:
                seen.add(cat)
                unique_categories.append(cat)
        
        result['categories'] = json.dumps(unique_categories)
    else:
        result['text_length'] = 0
        result['categories'] = json.dumps([])
    
    return result

# Register UDF
extract_udf = udf(extract_clean, MapType(StringType(), StringType()))

# Process data
df = raw_df.withColumn("data", extract_udf(col("value"))) \
           .select(
               col("data.page_id").alias("page_id"),
               col("data.title").alias("title"),
               col("data.year").alias("year"),
               col("data.categories").alias("categories_json"),
               col("data.text_length").alias("text_length")
           ) \
           .filter((col("title") != "") & col("title").isNotNull())

# Convert JSON string to array
df = df.withColumn("categories", from_json(col("categories_json"), ArrayType(StringType()))) \
       .drop("categories_json")

print(f"✅ Extracted {df.count():,} valid articles")

# Show statistics
print("\n" + "=" * 80)
print("DATA STATISTICS")
print("=" * 80)

# 1. Year distribution
print("1. Year distribution of articles:")
df.select("year").groupBy("year").count().orderBy(col("count").desc()).show(10)

# 2. Articles with most categories
print("\n2. Top 10 articles with most categories:")
df.withColumn("num_categories", size(col("categories"))) \
  .orderBy(col("num_categories").desc()) \
  .select("title", "num_categories", "categories") \
  .show(10, truncate=False)

# 3. Most common categories overall
print("\n3. Top 20 most common categories across all articles:")
# Explode categories to get one row per category
category_counts = df.select(explode(col("categories")).alias("category")) \
                   .groupBy("category") \
                   .count() \
                   .orderBy(col("count").desc())

category_counts.show(20, truncate=False)

# 4. Articles by length
print("\n4. Top 10 longest articles:")
df.orderBy(col("text_length").desc()) \
  .select("title", "text_length", "year") \
  .show(10, truncate=False)

# 5. Sample articles with their categories
print("\n5. Sample articles with categories:")
df.filter(size(col("categories")) > 0) \
  .select("title", "categories") \
  .limit(10) \
  .show(10, truncate=False)

# 6. Category growth by year (for knowledge growth analysis)
print("\n6. Category diversity by year (knowledge growth):")
category_growth = df.groupBy("year") \
                   .agg(
                       count("*").alias("num_articles"),
                       expr("sum(size(categories))").alias("total_categories"),
                       expr("avg(size(categories))").alias("avg_categories_per_article")
                   ) \
                   .orderBy("year")

category_growth.show(20)

# 7. Write final JSON for further analysis
output_path = "hdfs://namenode:9000/datasets/wikipedia/processed/articles_clean_final"
df.write.mode("overwrite").json(output_path)

# 8. Also write category statistics
category_stats_path = "hdfs://namenode:9000/datasets/wikipedia/processed/category_stats"
category_counts.write.mode("overwrite").json(category_stats_path)

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE - OUTPUTS SAVED")
print("=" * 80)
print(f"1. Clean articles: {output_path}")
print(f"2. Category statistics: {category_stats_path}")
print("\nKey metrics for knowledge growth analysis:")
print("- Category frequency distribution")
print("- Category diversity by year")
print("- Articles per category")
print("- Category growth trends")

spark.stop()
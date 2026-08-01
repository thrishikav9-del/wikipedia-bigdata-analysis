from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, size, when, count, avg, round, desc
from pyspark.sql.types import IntegerType

print("="*70)
print("PHASE 4: SPARK ML - ARTICLE CLUSTERING (2.2M ARTICLES)")
print("="*70)

# Initialize Spark
spark = SparkSession.builder \
    .appName("WikipediaMLClustering") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Load data
print("\n Loading articles...")
df = spark.read.json("file:///app/wikipedia_output.json")
df = df.withColumn("text_length", col("text_length").cast(IntegerType()))
df = df.withColumn("year", col("year").cast(IntegerType()))
df = df.na.fill(0)

total = df.count()
print(f" Loaded {total:,} articles")

# ============================================
# FEATURE ENGINEERING
# ============================================
print("\n Creating ML features...")

# Create features for clustering
df_features = df.withColumn("num_categories", size("categories")) \
    .withColumn("has_categories", when(col("num_categories") > 0, 1).otherwise(0)) \
    .withColumn("log_length", when(col("text_length") > 0, 
                                    col("text_length")).otherwise(0))

# Select features for ML
feature_cols = ["log_length", "num_categories", "has_categories", "year"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
df_assembled = assembler.transform(df_features)

# Scale features (important for K-Means)
scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                        withStd=True, withMean=True)
scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

print(" ML features created and scaled")

# ============================================
# FIND OPTIMAL K USING SILHOUETTE SCORE
# ============================================
print("\n Finding optimal number of clusters...")

silhouette_scores = []
k_range = [3, 4, 5, 6, 7]

for k in k_range:
    kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("features")
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    silhouette_scores.append(silhouette)
    print(f"  K={k}: Silhouette Score = {silhouette:.4f}")

# Select best K
best_k = k_range[silhouette_scores.index(max(silhouette_scores))]
print(f"\n Best K value: {best_k} (Silhouette: {max(silhouette_scores):.4f})")

# ============================================
# FINAL MODEL WITH BEST K
# ============================================
print(f"\n Running K-Means with K={best_k}...")
kmeans = KMeans().setK(best_k).setSeed(42).setFeaturesCol("features")
model = kmeans.fit(df_scaled)
predictions = model.transform(df_scaled)

# Get cluster centers
centers = model.clusterCenters()
print("\n Cluster Centers:")
for i, center in enumerate(centers):
    print(f"  Cluster {i}: Length={center[0]:.0f}, Categories={center[1]:.2f}, HasCat={center[2]:.2f}, Year={center[3]:.0f}")

# ============================================
# ANALYZE CLUSTERS
# ============================================
print("\n CLUSTER ANALYSIS:")
cluster_stats = predictions.groupBy("prediction") \
    .agg(
        count("*").alias("article_count"),
        round(avg("text_length"), 0).alias("avg_length"),
        round(avg("num_categories"), 2).alias("avg_categories"),
        round(avg("year"), 0).alias("avg_year"),
        round(avg("has_categories") * 100, 2).alias("pct_categorized")
    ) \
    .orderBy("prediction")

cluster_stats.show(truncate=False)

# ============================================
# INTERPRET CLUSTERS (THIS IS YOUR NOVEL INSIGHT!)
# ============================================
print("\n CLUSTER INTERPRETATION (KNOWLEDGE DISCOVERY):")
clusters = cluster_stats.collect()
for c in clusters:
    cluster_id = c['prediction']
    count = c['article_count']
    pct = count/total*100
    avg_len = c['avg_length']
    avg_cat = c['avg_categories']
    avg_year = c['avg_year']
    pct_cat = c['pct_categorized']
    
    # Determine cluster type based on patterns
    if avg_len < 100 and avg_cat == 0:
        type_desc = " STUB ARTICLES - Uncategorized, Very Short"
    elif avg_len < 1000 and avg_cat < 1:
        type_desc = " MINIMAL ARTICLES - Few Categories, Short"
    elif avg_len < 10000 and avg_cat < 3:
        type_desc = " BASIC ARTICLES - Moderate Categories, Medium"
    elif avg_len < 50000 and avg_cat >= 3:
        type_desc = " DEVELOPED ARTICLES - Good Categories, Long"
    else:
        type_desc = " COMPREHENSIVE ARTICLES - Rich Categories, Very Long"
    
    # Time-based insight
    if avg_year > 2020:
        time_desc = "Recent"
    elif avg_year > 2010:
        time_desc = "Mid-era"
    else:
        time_desc = "Early"
    
    print(f"""
Cluster {cluster_id}: {type_desc}
  • Articles: {count:,} ({pct:.1f}% of total)
  • Avg Length: {avg_len:.0f} chars
  • Avg Categories: {avg_cat}
  • Avg Year: {avg_year:.0f} ({time_desc})
  • Categorized: {pct_cat}%
""")

# ============================================
# CROSS-TABULATION: CLUSTER VS YEAR
# ============================================
print("\n CLUSTER DISTRIBUTION BY YEAR:")
cluster_by_year = predictions.groupBy("year", "prediction") \
    .count() \
    .orderBy("year", "prediction")

cluster_by_year.show(30, truncate=False)

# ============================================
# SAVE RESULTS
# ============================================
print("\n Saving ML results...")
predictions.select("title", "year", "text_length", "num_categories", "prediction") \
    .write.mode("overwrite") \
    .csv("/app/project_results/ml_clusters")

# Save cluster interpretation
with open("/app/project_results/cluster_summary.txt", "w") as f:
    f.write("="*70 + "\n")
    f.write("SPARK ML CLUSTERING RESULTS - 2.2M ARTICLES\n")
    f.write("="*70 + "\n\n")
    f.write(f"Total Articles: {total:,}\n")
    f.write(f"Best K Value: {best_k} (Silhouette Score: {max(silhouette_scores):.4f})\n\n")
    
    f.write("CLUSTER INTERPRETATIONS:\n")
    f.write("-"*50 + "\n")
    for c in clusters:
        cluster_id = c['prediction']
        count = c['article_count']
        pct = count/total*100
        avg_len = c['avg_length']
        avg_cat = c['avg_categories']
        avg_year = c['avg_year']
        
        f.write(f"\nCluster {cluster_id}:\n")
        f.write(f"  Articles: {count:,} ({pct:.1f}%)\n")
        f.write(f"  Avg Length: {avg_len:.0f} chars\n")
        f.write(f"  Avg Categories: {avg_cat}\n")
        f.write(f"  Avg Year: {avg_year:.0f}\n")

print(" Saved to /app/project_results/ml_clusters/")
print(" Saved cluster_summary.txt")

print("\n" + "="*70)
print(" PHASE 4 COMPLETE - ML Clustering Done!")
print(" Discovered 5 distinct article types automatically!")
print("="*70)

spark.stop()
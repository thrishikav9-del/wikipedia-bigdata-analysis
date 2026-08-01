from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, size, when
import numpy as np

print(" ADVANCED ANALYTICS WITH SPARK ML")

# Load data
df = spark.read.json("hdfs://namenode:9000/datasets/wikipedia/final/wikipedia_output.json")

# Feature Engineering
df = df.withColumn("text_length_norm", col("text_length") / 1000)
df = df.withColumn("num_categories", size("categories"))
df = df.withColumn("has_categories", when(col("num_categories") > 0, 1).otherwise(0))

# Prepare features for clustering
assembler = VectorAssembler(
    inputCols=["text_length_norm", "num_categories", "has_categories"],
    outputCol="features"
)
feature_df = assembler.transform(df)

# K-Means Clustering
kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(feature_df)

# Make predictions
predictions = model.transform(feature_df)

# Evaluate clustering
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette score: {silhouette}")

# Show cluster centers
centers = model.clusterCenters()
print("Cluster centers:")
for i, center in enumerate(centers):
    print(f"Cluster {i}: {center}")

# Analyze cluster distribution
cluster_stats = predictions.groupBy("prediction").agg(
    count("*").alias("count"),
    avg("text_length").alias("avg_length"),
    avg("num_categories").alias("avg_categories")
).orderBy("prediction")

print("\n CLUSTER ANALYSIS:")
cluster_stats.show()

# Save results
predictions.select("title", "year", "prediction", "text_length", "num_categories") \
    .write.mode("overwrite") \
    .csv("hdfs://namenode:9000/output/wikipedia_clusters")
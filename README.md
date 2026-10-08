# Wikipedia Big Data Analytics

### Knowledge Evolution, Temporal Analysis & Article Clustering Using Hadoop & Spark

A scalable Big Data analytics project for studying the evolution, organization, and content characteristics of Wikipedia articles using distributed data processing and machine learning.

The project analyzes **2.2+ million Wikipedia articles** from **3.8 GB of raw XML data** through a five-phase pipeline involving data ingestion, Hadoop MapReduce, Spark SQL, K-Means clustering, and visualization.

---

## Project Overview

Wikipedia has evolved continuously over more than two decades, producing a large and complex collection of articles.

This project builds an end-to-end Big Data analytics pipeline to investigate:

- How Wikipedia article volume changes over time
- Which categories occur most frequently
- How many articles are categorized or uncategorized
- How article length varies across the dataset
- How categorization changes over time
- Whether machine learning can identify different types of articles automatically

The project combines distributed storage, distributed processing, SQL-based analytics, machine learning, and visualization into a single workflow.

---

## Key Highlights

- Processed **3.8 GB of Wikipedia XML data**
- Analyzed **2,246,121 structured articles**
- Used **HDFS** for distributed storage
- Implemented **Hadoop MapReduce** using Python
- Performed **7 Spark SQL analytical queries**
- Applied **K-Means clustering using Spark MLlib**
- Identified **6 article clusters**
- Achieved a **0.7905 Silhouette Score**
- Performed temporal analysis across **2002–2026**
- Generated analytical visualizations using Python and Matplotlib
- Used Docker-based project configuration for the development environment

---

## System Architecture

~~~text
Wikipedia XML Data
        |
        v
+---------------------------+
|     Data Ingestion        |
|     XML -> Structured     |
|     JSON + HDFS           |
+-------------+-------------+
              |
              v
+---------------------------+
|     Hadoop MapReduce      |
|     Python Word Count     |
|     Title Analysis        |
+-------------+-------------+
              |
              v
+---------------------------+
|        Spark SQL          |
|  Temporal & Category      |
|        Analysis           |
+-------------+-------------+
              |
              v
+---------------------------+
|       Spark MLlib         |
|    K-Means Clustering     |
+-------------+-------------+
              |
              v
+---------------------------+
|      Visualization        |
|     Charts & Insights     |
+---------------------------+
~~~

---

## Dataset

The project uses Wikipedia XML dump subsets for large-scale analysis.

### Dataset Statistics

| Property | Value |
|---|---:|
| Raw data size | 3.8 GB |
| Articles analyzed | 2,246,121 |
| Time span | 2002–2026 |
| Input format | XML |
| Processed format | JSON |
| Distributed storage | HDFS |

Each processed article contains structured information including:

- `page_id`
- `title`
- `year`
- `text_length`
- `categories`

The raw Wikipedia XML data is transformed into structured records before distributed analysis.

---

## Processing Pipeline

### Phase 1 — Data Ingestion and Storage

The Wikipedia XML data is loaded into HDFS and processed to extract structured article information.

The ingestion stage extracts fields such as:

- Article ID
- Article title
- Year
- Text length
- Categories

The resulting structured dataset contains more than **2.2 million article records**.

~~~text
Wikipedia XML
      |
      v
    HDFS
      |
      v
Structured Article Records
~~~

---

### Phase 2 — Hadoop MapReduce

A Hadoop Streaming MapReduce workflow implemented in Python is used for article-title word frequency analysis.

#### Mapper

The mapper:

1. Reads article records
2. Extracts article titles
3. Tokenizes title text
4. Emits word-count pairs

~~~text
Article
   |
   v
Extract Title
   |
   v
Tokenization
   |
   v
(word, 1)
~~~

#### Reducer

The reducer groups identical words and aggregates their counts.

~~~text
(word, 1)
    |
    v
Group by Word
    |
    v
Aggregate Counts
    |
    v
Word Frequency
~~~

This demonstrates distributed processing using the MapReduce programming model.

---

### Phase 3 — Spark SQL Analytics

Apache Spark and Spark SQL are used for structured analysis of the processed Wikipedia dataset.

The project performs seven analytical queries covering:

1. **Year-wise Article Counts**  
   Analyzes how the number of Wikipedia articles changes over time.

2. **Most Frequent Categories**  
   Identifies categories occurring most frequently across the dataset.

3. **Category Coverage**  
   Measures the proportion of categorized and uncategorized articles.

4. **Article Text-Length Distribution**  
   Examines the distribution of article lengths.

5. **Top Categories in 2025**  
   Analyzes the most common categories for articles associated with 2025.

6. **Highly Categorized Articles**  
   Identifies articles associated with the largest number of categories.

7. **Uncategorized Articles by Year**  
   Examines how the number of uncategorized articles changes over time.

These analyses provide both temporal and structural insights into Wikipedia's organization.

---

### Phase 4 — K-Means Clustering

Spark MLlib is used to perform unsupervised K-Means clustering.

The clustering model uses the following features:

- `text_length`
- `num_categories`
- `has_categories`
- `year`

### Machine Learning Pipeline

~~~text
Article Features
       |
       v
VectorAssembler
       |
       v
StandardScaler
       |
       v
K-Means
       |
       v
Cluster Evaluation
~~~

The clustering process evaluates different values of K and selects the configuration based on Silhouette Score.

### Best Configuration

| Parameter | Value |
|---|---:|
| Number of Clusters | 6 |
| Silhouette Score | 0.7905 |

---

## Discovered Article Clusters

The K-Means model identified six distinct article groups based on the selected features.

| Cluster | Interpreted Type | Percentage |
|---|---|---:|
| C0 | Standard Modern | 36.7% |
| C1 | Old Stubs | 32.1% |
| C2 | Comprehensive New | 2.6% |
| C3 | Category-Rich Hubs | 5.5% |
| C4 | Monumental Articles | 0.4% |
| C5 | Recent Stubs | 22.7% |

These groups are interpretations of the resulting clusters based on their feature characteristics.

---

## Key Findings

### Categorization

A substantial proportion of the analyzed articles were found to be uncategorized.

**54.7%** of analyzed articles were uncategorized.

### Article Size

**42.8%** of articles were identified as tiny stubs containing fewer than 100 characters.

### Temporal Change

The proportion of uncategorized articles decreased considerably over the analyzed period.

~~~text
2002 -> 100% uncategorized
2025 -> 9.21% uncategorized
~~~

This indicates a substantial improvement in categorization coverage over time.

### Knowledge Hubs

Some highly connected articles were associated with more than 200 categories, highlighting areas with extensive relationships within Wikipedia's category structure.

### Article Clustering

The K-Means analysis identified six article groups with a **0.7905 Silhouette Score**, providing a data-driven view of different article characteristics.

---

## Visualization

The project generates visualizations for:

- Year-wise article counts
- Category distribution
- Article-length distribution
- Cluster distribution
- Category coverage
- Temporal trends
- Uncategorized article trends

The repository includes generated analytical visualizations such as:

- `categories_chart.png`

---

## Technologies Used

### Big Data

- Apache Hadoop
- HDFS
- Hadoop MapReduce
- Apache Spark
- Spark SQL
- Spark MLlib

### Programming and Analytics

- Python
- PySpark
- SQL
- K-Means Clustering
- Matplotlib

### Data Formats

- XML
- JSON
- CSV

### Development Tools

- Docker
- Git
- GitHub

---

## Repository Structure

~~~text
wikipedia-bigdata-analysis/
|
+-- advanced_analytics.py
+-- analysis.py
+-- charts.py
|
+-- categories_chart.png
|
+-- docker/
+-- docker-compose.yml
|
+-- .gitignore
+-- LICENSE
+-- README.md
+-- hello.txt
~~~

### Main Files

| File / Directory | Description |
|---|---|
| `analysis.py` | Core data analysis functionality |
| `advanced_analytics.py` | Extended analytical processing |
| `charts.py` | Visualization generation |
| `categories_chart.png` | Generated category analysis visualization |
| `docker/` | Docker-related configuration |
| `docker-compose.yml` | Docker Compose configuration |
| `.gitignore` | Git ignored files configuration |
| `LICENSE` | MIT License |
| `README.md` | Project documentation |

---

## Research Questions

The project investigates the following questions:

1. How has Wikipedia article volume changed over time?
2. Which categories occur most frequently?
3. What proportion of articles are categorized?
4. How has category coverage changed over time?
5. How does article length vary across the dataset?
6. Can unsupervised learning identify meaningful article types?
7. How are article length, categories, and year associated with different article groups?

---

## Project Workflow

~~~text
1. Collect Wikipedia XML data
            |
            v
2. Store data using HDFS
            |
            v
3. Transform XML into structured records
            |
            v
4. Perform Hadoop MapReduce processing
            |
            v
5. Load structured data into Spark
            |
            v
6. Execute Spark SQL analytics
            |
            v
7. Engineer clustering features
            |
            v
8. Apply K-Means using Spark MLlib
            |
            v
9. Evaluate clusters using Silhouette Score
            |
            v
10. Generate visualizations
            |
            v
11. Interpret knowledge evolution patterns
~~~

---

## Results

The project demonstrates how distributed computing and machine learning can be combined to analyze large-scale knowledge repositories.

The final pipeline provides:

- Large-scale distributed data processing
- Temporal analysis of knowledge growth
- Category and content analysis
- Unsupervised article clustering
- Quantitative cluster evaluation
- Visualization-driven interpretation

---

## Future Work

Potential extensions include:

- NLP-based topic modeling
- Automated category recommendation
- Multilingual Wikipedia comparison
- Category co-occurrence network analysis
- Automated article-quality classification
- Real-time Wikipedia edit processing
- Article recommendation based on cluster similarity

These are proposed extensions and are not part of the current implementation.

---

## Contributors

**Vullasa Thrishika**  
Amrita Vishwa Vidyapeetham, Coimbatore

**R.K. Sri Raaghavi**  
Amrita Vishwa Vidyapeetham, Coimbatore

**Nikhilesh Vaibhav Krupakar**  
Amrita Vishwa Vidyapeetham, Coimbatore

**Project Guide:** Dr. Ardra P S

---

## License

This project is licensed under the **MIT License**.

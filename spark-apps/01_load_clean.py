from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils import write_to_postgres

# 1️⃣ Spark session
spark = (
    SparkSession.builder
    .appName("01_Load_Clean_MovieLens")
    .getOrCreate()
)

# 2️⃣ Chargement ratings (u.data)
ratings_path = "/data/ml-100k/u.data"

ratings = (
    spark.read
    .option("sep", "\t")
    .csv(ratings_path)
    .toDF("user_id", "item_id", "rating", "timestamp")
)

# 3️⃣ Nettoyage ratings
ratings_clean = (
    ratings
    .select(
        col("user_id").cast("int"),
        col("item_id").cast("int"),
        col("rating").cast("double"),
        col("timestamp").cast("long")
    )
    .dropna()
    .filter((col("rating") >= 1) & (col("rating") <= 5))
)

# 4️⃣ Chargement films (u.item)
movies_path = "/data/ml-100k/u.item"

movies_raw = (
    spark.read
    .option("sep", "|")
    .csv(movies_path)
)

movies = movies_raw.select(
    col("_c0").cast("int").alias("item_id"),
    col("_c1").alias("title"),
    col("_c2").alias("release_date")
)

# 5️⃣ Écriture PostgreSQL
write_to_postgres(ratings_clean, "ratings_clean", "overwrite")
write_to_postgres(movies, "movies", "overwrite")

spark.stop()

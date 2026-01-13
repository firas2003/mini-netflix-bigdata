from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col
from utils import write_to_postgres

spark = (
    SparkSession.builder
    .appName("02_SQL_Analysis_Top_Movies")
    .getOrCreate()
)

# 🔹 Lire ratings_clean
ratings = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/netflix_dw")
    .option("dbtable", "ratings_clean")
    .option("user", "netflix")
    .option("password", "netflix")
    .option("driver", "org.postgresql.Driver")
    .load()
)

# 🔹 Lire movies
movies = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/netflix_dw")
    .option("dbtable", "movies")
    .option("user", "netflix")
    .option("password", "netflix")
    .option("driver", "org.postgresql.Driver")
    .load()
)

# 🔹 Calcul Top Films
top_movies = (
    ratings
    .groupBy("item_id")
    .agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("rating_count")
    )
    .filter(col("rating_count") >= 50)
    .join(movies, "item_id")
    .select("item_id", "title", "avg_rating", "rating_count")
    .orderBy(col("avg_rating").desc(), col("rating_count").desc())
)

# 🔹 Écriture PostgreSQL
write_to_postgres(top_movies, "top_movies", "overwrite")

spark.stop()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import current_timestamp
from utils import write_to_postgres

spark = (
    SparkSession.builder
    .appName("03_ALS_Train_Eval")
    .getOrCreate()
)

# 🔹 Charger les ratings
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

# 🔹 Split train / test
train, test = ratings.randomSplit([0.8, 0.2], seed=42)

# 🔹 Modèle ALS
als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop"
)

model = als.fit(train)

# 🔹 Prédictions
predictions = model.transform(test)

# 🔹 Évaluation RMSE
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse_value = evaluator.evaluate(predictions)

print(f"RMSE = {rmse_value}")

# 🔹 Sauvegarder RMSE (historique)
rmse_df = spark.createDataFrame(
    [(rmse_value,)],
    ["rmse"]
).withColumn("created_at", current_timestamp())

write_to_postgres(rmse_df, "rmse_history", "append")

# 🔹 Sauvegarder les prédictions
write_to_postgres(
    predictions.select("user_id", "item_id", "prediction"),
    "als_predictions",
    "overwrite"
)

spark.stop()

# spark-apps/utils.py

PG_URL = "jdbc:postgresql://postgres:5432/netflix_dw"
PG_USER = "netflix"
PG_PASSWORD = "netflix"
PG_DRIVER = "org.postgresql.Driver"

def write_to_postgres(df, table_name, mode="overwrite"):
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table_name)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", PG_DRIVER)
        .mode(mode)
        .save()
    )

import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

load_dotenv()
PG_URL = "jdbc:postgresql://localhost:5432/" + os.environ["POSTGRES_DB"]
PG_PROPS = {"user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver"}

spark = SparkSession.builder.appName("events-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("product_id", IntegerType()),
    StructField("price", DoubleType()),
    StructField("event_time", StringType()),
])

# 1. Read from Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "user_events")
       .option("startingOffsets", "latest")
       .load()
       .select(F.col("value").cast("string").alias("raw_value")))

# 2. Parse JSON (bad JSON becomes null)
parsed = raw.withColumn("data", F.from_json("raw_value", schema))

# 3. Split good vs bad
bad = parsed.filter(F.col("data").isNull()).select("raw_value")

good = (parsed.filter(F.col("data").isNotNull())
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .dropna(subset=["event_id", "event_time", "event_type"])
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"]))

# 4. Windowed aggregation: per minute, per event type
agg = (good.groupBy(F.window("event_time", "1 minute"), "event_type")
       .agg(F.count("*").alias("event_count"),
            F.round(F.sum("price"), 2).alias("revenue"))
       .select(F.col("window.start").alias("window_start"),
               "event_type", "event_count", "revenue"))

# 5. Writers (called once per micro-batch)
def pg_execute(sql):
    conn = psycopg2.connect(host="localhost", dbname=os.environ["POSTGRES_DB"],
                            user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])
    with conn, conn.cursor() as cur:
        cur.execute(sql)
    conn.close()

def write_raw(df, batch_id):
    df.write.jdbc(PG_URL, "stg_raw", "overwrite", PG_PROPS)
    pg_execute("""INSERT INTO raw_events (event_id,user_id,event_type,product_id,price,event_time)
                  SELECT event_id,user_id,event_type,product_id,price,event_time FROM stg_raw
                  ON CONFLICT (event_id) DO NOTHING;""")

def write_agg(df, batch_id):
    df.write.jdbc(PG_URL, "stg_agg", "overwrite", PG_PROPS)
    pg_execute("""INSERT INTO agg_per_minute (window_start,event_type,event_count,revenue)
                  SELECT window_start,event_type,event_count,revenue FROM stg_agg
                  ON CONFLICT (window_start,event_type)
                  DO UPDATE SET event_count=EXCLUDED.event_count, revenue=EXCLUDED.revenue;""")

def write_bad(df, batch_id):
    df.withColumnRenamed("raw_value", "raw_value") \
      .write.jdbc(PG_URL, "dead_letter", "append", PG_PROPS)

# 6. Start three streams, each with its own checkpoint folder
q1 = good.writeStream.foreachBatch(write_raw).outputMode("append") \
     .option("checkpointLocation", "checkpoints/raw").start()
q2 = agg.writeStream.foreachBatch(write_agg).outputMode("update") \
     .option("checkpointLocation", "checkpoints/agg").start()
q3 = bad.writeStream.foreachBatch(write_bad).outputMode("append") \
     .option("checkpointLocation", "checkpoints/bad").start()

spark.streams.awaitAnyTermination()
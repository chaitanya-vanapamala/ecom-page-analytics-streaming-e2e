from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType


class Constants:
    DB_HOST='database'
    DB_PORT=5432
    DB_NAME='page_analytics'
    DB_USER='analytics_owner'
    DB_PASS='&3!Yk&84'


class Connector:
    __sess = None

    def __init__(self):
        self.create_spark_session()
        self.db_config = {
            "url": self.get_db_url,
            "user": Constants.DB_USER,
            "password": Constants.DB_PASS,
            "driver": "org.postgresql.Driver"
        }
    
    def get(self):
        return Connector.__sess

    def create_spark_session(self):
        try:
            if Connector.__sess is None:
                Connector.__sess = SparkSession.builder.appName('Analytics-Processor').getOrCreate()
                Connector.__sess.sparkContext.setLogLevel("ERROR")
                print("Spark Session created successfully!")
            else:
                print("Spark Session alread created!")
        except Exception as e:
            print(f"Couldn't create the spark session due to exception {e}")


    @property
    def get_db_url(self):
        return f"jdbc:postgresql://{Constants.DB_HOST}:{Constants.DB_PORT}/{Constants.DB_NAME}"
    

    def db_write(self, table_name, batch_df, batch_id, mode="append"):
        print(f"Writing to {table_name} : {batch_id}...")
        batch_df.write.format("jdbc") \
            .options(**con.db_config) \
            .option('dbtable', table_name) \
            .mode(mode) \
            .save()
        print(f"Completed Writing to {table_name} : {batch_id}.")


class StreamProcess:

    def __init__(self, con = None):
        if con:
            self.con = con
        else:
            self.con = Connector()

    @property
    def event_schema(self):
        return StructType([
            # Core Fields
            StructField("timestamp", StringType(), False), 
            StructField("user_name", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("page_url", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("product_category", StringType(), False),
            StructField("source", StringType(), False),
            StructField("geolocation", StringType(), False),
            StructField("page_load_time_ms", IntegerType(), False)
        ])
    
    def get_stream_data(self):
        df = None
        try:
            df = self.con.get().readStream.format('kafka') \
                .option('kafka.bootstrap.servers', 'broker:29092') \
                .option('subscribe', 'raw_page_analytics') \
                .option('startingOffsets', 'earliest') \
                .load() \
                .selectExpr("CAST(value AS STRING)")
            print("kafka dataframe created successfully")
        except Exception as e:
            print(f"kafka dataframe could not be created because: {e}")

        return df
    
    def db_write_events(self, batch_df, batch_id):
        print("*"*20)
        print(f"Batch : {batch_id}")
        print("*"*20)
        batch_df.show()
        self.con.db_write('fact_events', batch_df, batch_id)

    def db_write_event_agg(self, batch_df, batch_id):
        print("*"*20)
        print(f"Batch : {batch_id}")
        print("*"*20)
        batch_df.show()
        self.con.db_write('fact_events_realtime_agg', batch_df, batch_id)


if __name__ == '__main__':

    con = Connector()
    print("Loading Dimension tables...")
    dim_time = con.get().read.format('jdbc').options(**con.db_config).option('dbtable', 'dim_time').load()
    dim_date = con.get().read.format('jdbc').options(**con.db_config).option('dbtable', 'dim_date').load()
    dim_locations = con.get().read.format('jdbc').options(**con.db_config).option('dbtable', 'dim_locations').load()
    dim_users = con.get().read.format('jdbc').options(**con.db_config).option('dbtable', 'dim_users').load()
    print("Finished Loading dimension tables.")

    print("Init Stream Processing...")
    sp = StreamProcess(con)

    kafka_df = sp.get_stream_data()
    
    raw_events_df_clean = kafka_df.withColumn("value", F.regexp_replace(F.col("value"), "\\\\", "")) \
        .withColumn("value", F.regexp_replace(F.col("value"), "^\"|\"$", ""))

    raw_events = raw_events_df_clean.select(
        F.from_json(F.col("value"), sp.event_schema).alias("event_data")
    )

    raw_events = raw_events.select("event_data.*") \
        .withColumn('timestamp', F.col('timestamp').cast("timestamp")) \
        .withColumn('source', F.expr("COALESCE(source, 'N/A')")) \
        .withColumn('event_type', F.expr("""
            CASE
                WHEN page_url = '/order-success' THEN 'order'
                WHEN page_url = '/cart' THEN 'cart_view'
                WHEN page_url = '/products' THEN 'catalogue_view'
                ELSE event_type
            END
        """))

    raw_events = raw_events.withColumn('e_year', F.year(F.col('timestamp'))) \
                        .withColumn('e_month', F.month(F.col('timestamp'))) \
                        .withColumn('e_day', F.dayofmonth(F.col('timestamp'))) \
                        .withColumn('e_hour', F.hour(F.col('timestamp'))) \
                        .withColumn('e_minute', F.minute(F.col('timestamp'))) \
                        .withColumn('e_second', F.second(F.col('timestamp')))

    events = raw_events.alias("e").join(
        dim_time.alias("t"),
        on = [
            F.col("e.e_hour") == F.col("t.hour"),
            F.col("e.e_minute") == F.col("t.minute"),
            F.col("e.e_second") == F.col("t.seconds")
        ]
    ).join(
        dim_date.alias("d"),
        on = [
            F.col("e.e_year") == F.col("d.year"),
            F.col("e.e_month") == F.col("d.month"),
            F.col("e.e_day") == F.col("d.day")
        ]
    ).join(
        dim_locations.alias('loc'),
        on = [
            F.col("e.geolocation") == F.col("loc.country_name")
        ]
    ).join(
        dim_users.alias('users'),
        on = [
            F.col("e.user_name") == F.col("users.user_name")
        ]
    ).selectExpr(
        "d.date_key",
        "t.time_key",
        "loc.location_key",
        "users.user_key",
        "e.timestamp",
        "e.source",
        "e.event_type",
        "e.page_url",
        "e.product_category",
        "e.page_load_time_ms",
    )

    events_last_xmin = events.withWatermark("timestamp", "15 minutes") \
                    .groupBy(
                        F.window(F.col("timestamp"), "15 minutes", "1 minute"),
                        F.col("event_type")
                        ) \
                    .agg(F.count("event_type").alias("event_count")) \
                    .select(
                        F.col("window.start").alias("start_time"),
                        F.col("window.end").alias("end_time"),
                        F.col("event_type"),
                        F.col("event_count")
                    )

    write_event = events.writeStream \
        .foreachBatch(sp.db_write_events) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/event_stream") \
        .start()
    
    write_last_xmin = events_last_xmin.writeStream \
                    .foreachBatch(sp.db_write_event_agg) \
                    .outputMode("append") \
                    .option("checkpointLocation", "/tmp/checkpoints/event_stream_agg") \
                    .trigger(processingTime="1 minutes") \
                    .start()

    print("--- Starting Structured Streaming Console Output. ---")

    write_event.awaitTermination()
    write_last_xmin.awaitTermination
    

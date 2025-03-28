import json
import time
import logging
from datetime import datetime
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

# Configure robust logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load Configuration
config = ConfigParser()
config.read('config.ini')

# PostgreSQL Connection
def connect_postgres():
    for attempt in range(3):
        try:
            conn = psycopg2.connect(
                dbname=config.get('POSTGRES', 'DB_NAME'),
                user=config.get('POSTGRES', 'USER'),
                password=config.get('POSTGRES', 'PASSWORD'),
                host=config.get('POSTGRES', 'HOST'),
                port=config.get('POSTGRES', 'PORT'),
                connect_timeout=5
            )
            return conn
        except Exception as e:
            logger.warning(f"PostgreSQL connection attempt {attempt+1} failed: {str(e)}")
            time.sleep(2)
    raise ConnectionError("Failed to connect to PostgreSQL after 3 attempts")

# Initialize Database (Create Tables & Indexes)
def initialize_database():
    with connect_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                product_id VARCHAR(50),
                rating FLOAT,
                timestamp TIMESTAMP
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id);")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                log_id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                log_level VARCHAR(10),
                message TEXT
            )""")
            conn.commit()

# Create Spark Session
def create_spark_session():
    return SparkSession.builder \
        .appName("ReviewProcessor") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
        .getOrCreate()

# Process Reviews
def process_reviews(spark):
    logger.info("Loading dataset into Spark...")

    df = spark.read.csv(
        config.get('DATA', 'INPUT_FILE'),
        header=True,
        inferSchema=True
    ).filter(col("rating").isNotNull())  # Drop empty ratings

    df = df.withColumn("timestamp", unix_timestamp(col("timestamp")).cast("timestamp"))

    # Save as Parquet for fast future loads
    df.write.parquet("data/reviews.parquet", mode="overwrite")
    
    logger.info(f"Dataset loaded: {df.count()} rows after filtering.")

    return df.filter(col("rating") >= 4)  # Filter only high-rated reviews

# Save to PostgreSQL Efficiently
def save_to_postgres(df):
    logger.info(f"Saving {df.count()} records to PostgreSQL...")

    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{config.get('POSTGRES', 'HOST')}:{config.get('POSTGRES', 'PORT')}/{config.get('POSTGRES', 'DB_NAME')}") \
        .option("dbtable", "reviews") \
        .option("user", config.get('POSTGRES', 'USER')) \
        .option("password", config.get('POSTGRES', 'PASSWORD')) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()

    logger.info("Data successfully saved to PostgreSQL.")

# Create Kafka Producer
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=config.get('KAFKA', 'BOOTSTRAP_SERVERS').split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        acks='all'
    )

# Process Kafka Messages (Logs)
def process_kafka_messages():
    consumer = KafkaConsumer(
        config.get('KAFKA', 'TOPIC'),
        bootstrap_servers=config.get('KAFKA', 'BOOTSTRAP_SERVERS').split(','),
        auto_offset_reset='earliest',
        group_id='review_pipeline',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    with connect_postgres() as conn:
        with conn.cursor() as cur:
            for message in consumer:
                log_entry = message.value
                cur.execute(
                    "INSERT INTO logs VALUES (default,%s,%s,%s)",
                    (datetime.now(), log_entry['log_level'], log_entry['message'])
                )
            conn.commit()
    
    consumer.close()

# Run Pipeline
if __name__ == "__main__":
    try:
        logger.info("Starting pipeline execution")
        initialize_database()

        producer = create_kafka_producer()
        producer.send(config.get('KAFKA', 'TOPIC'), {
            "timestamp": datetime.now().isoformat(),
            "log_level": "INFO",
            "message": "Pipeline started processing"
        }).get()  # Wait for acknowledgement

        spark = create_spark_session()
        df = process_reviews(spark)
        save_to_postgres(df)

        producer.send(config.get('KAFKA', 'TOPIC'), {
            "timestamp": datetime.now().isoformat(),
            "log_level": "INFO",
            "message": f"Processed {df.count()} records"
        }).get()
        
        process_kafka_messages()
        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        if 'producer' in locals():
            producer.send(config.get('KAFKA', 'TOPIC'), {
                "timestamp": datetime.now().isoformat(),
                "log_level": "ERROR",
                "message": str(e)
            })
        raise
    finally:
        if 'producer' in locals():
            producer.close()
        if 'spark' in locals():
            spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from app.config.config import LIMIT, DATA_PATH

def ingest_data(file_path, limit):
    spark = SparkSession.builder.appName("MovieRevenueAnalysis").getOrCreate()
    revenue_df = spark.read.csv(file_path, header=True, inferSchema=True).limit(limit)
    return revenue_df

if __name__ == "__main__":
    df, year_list = ingest_data(DATA_PATH, LIMIT)
    df.show()
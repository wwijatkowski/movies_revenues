from pyspark.sql.functions import dayofmonth, month, year, col
from app.config.config import LIMIT, DIM_DATE_LOCATION, DIM_MOVE_LOCATION, FACT_DAILY_REVENUE_LOCATION, DATA_PATH
from pyspark.sql import SparkSession

def create_dimension_tables(enriched_df, revenue_df):
    dim_movie_df = enriched_df.select("id", "title", "date", "genre", "director", "actors", "imdb_rating").dropDuplicates(["id"])
    dim_date_df = revenue_df.select("date").distinct().withColumn("day", dayofmonth(col("date"))).withColumn("month", month(col("date"))).withColumn("year", year(col("date")))
    return dim_movie_df, dim_date_df

def create_fact_table(enriched_df):
    fact_daily_revenue_df = enriched_df.select("date", "id", "revenue")
    return fact_daily_revenue_df

def save_tables(dim_movie_df, dim_date_df, fact_daily_revenue_df):
    dim_movie_df.write.mode("overwrite").parquet(DIM_MOVE_LOCATION)
    dim_date_df.write.mode("overwrite").parquet(DIM_DATE_LOCATION)
    fact_daily_revenue_df.write.mode("overwrite").parquet(FACT_DAILY_REVENUE_LOCATION)

if __name__ == "__main__":
    from ingest_data import ingest_data
    from enrich_data import enrich_data

    spark = SparkSession.builder.appName("MovieRevenueAnalysis").getOrCreate()
    revenue_df = ingest_data(DATA_PATH, LIMIT)
    enriched_df = enrich_data(revenue_df)
    dim_movie_df, dim_date_df = create_dimension_tables(enriched_df, revenue_df)
    fact_daily_revenue_df = create_fact_table(enriched_df)
    save_tables(dim_movie_df, dim_date_df, fact_daily_revenue_df)
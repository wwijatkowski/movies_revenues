import unittest
from pyspark.sql import SparkSession
from app.scripts.ingest_data import ingest_data
from app.scripts.enrich_data import enrich_data
from app.scripts.data_pipeline import create_dimension_tables, create_fact_table, save_tables
from app.config.config import DATA_PATH, LIMIT

class TestDataPipeline(unittest.TestCase):
    def test_data_pipeline(self):
        revenue_df = ingest_data(DATA_PATH, LIMIT)
        enriched_df = enrich_data(revenue_df)
        dim_movie_df, dim_date_df = create_dimension_tables(enriched_df, revenue_df)
        fact_daily_revenue_df = create_fact_table(enriched_df)
        save_tables(dim_movie_df, dim_date_df, fact_daily_revenue_df)
        self.assertGreater(dim_movie_df.count(), 0)
        self.assertGreater(dim_date_df.count(), 0)
        self.assertGreater(fact_daily_revenue_df.count(), 0)

if __name__ == "__main__":
        unittest.main()
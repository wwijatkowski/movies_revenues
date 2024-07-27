import unittest
from pyspark.sql import SparkSession
from app.scripts.ingest_data import ingest_data
from app.scripts.enrich_data import enrich_data
from app.config.config import LIMIT, DATA_PATH

class TestEnrichData(unittest.TestCase):
    def test_enrich_data(self):
        revenue_df = ingest_data(DATA_PATH, LIMIT)
        enriched_df = enrich_data(revenue_df)
        self.assertIsNotNone(enriched_df)
        self.assertGreater(enriched_df.count(), 0)
        self.assertTrue('revenue' in enriched_df.columns)
        self.assertTrue('id' in enriched_df.columns)
        self.assertTrue('date' in enriched_df.columns)
        self.assertTrue('title' in enriched_df.columns)

if __name__ == "__main__":
    unittest.main()
import unittest
from pyspark.sql import SparkSession
from app.scripts.ingest_data import ingest_data
from app.config.config import DATA_PATH, LIMIT

class TestIngestData(unittest.TestCase):
    def test_ingest_data(self):
        df = ingest_data(DATA_PATH, LIMIT)
        self.assertIsNotNone(df)
        self.assertGreater(df.count(), 0)

if __name__ == "__main__":
    unittest.main()
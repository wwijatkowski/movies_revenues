import unittest

# Import test cases
from tests.test_ingest_data import TestIngestData
from tests.test_enrich_data import TestEnrichData
from tests.test_data_pipeline import TestDataPipeline
from tests.test_create_er_diagram import TestCreateERDiagram

# Create a Test Suite
def suite():
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    # Add tests in the desired order
    test_suite.addTest(loader.loadTestsFromTestCase(TestIngestData))
    test_suite.addTest(loader.loadTestsFromTestCase(TestEnrichData))
    test_suite.addTest(loader.loadTestsFromTestCase(TestDataPipeline))
    test_suite.addTest(loader.loadTestsFromTestCase(TestCreateERDiagram))
    
    return test_suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
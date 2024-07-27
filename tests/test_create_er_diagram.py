import unittest
import os
from app.scripts.create_er_diagram import main

class TestCreateERDiagram(unittest.TestCase):
    def test_create_er_diagram(self):
        main()
        self.assertTrue(os.path.exists('er_diagram.png'))

if __name__ == "__main__":
    unittest.main()
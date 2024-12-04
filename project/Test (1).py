#!/usr/bin/env python
# coding: utf-8

# In[13]:


import os
import unittest
import pandas as pd
import sqlite3
pipeline_path = r"C:\Users\sajja\Downloads"
sys.path.insert(0, pipeline_path)

# Import pipeline functions
from pipeline import run_pipeline, clean_data, save_to_sqlite

class TestDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Run the pipeline to generate outputs for testing."""
        run_pipeline()

    def test_csv_files_exist(self):
        """Test that the downloaded CSV files exist."""
        self.assertTrue(os.path.isfile('./data/nla_condition.csv'), "NLA CSV file does not exist")
        self.assertTrue(os.path.isfile('./data/ecos_data.csv'), "ECOS CSV file does not exist")

    def test_sqlite_file_exists(self):
        """Test that the SQLite database file exists."""
        self.assertTrue(os.path.isfile('./data/water_quality_analysis.db'), "SQLite database file does not exist")

    def test_cleaned_data_content(self):
        """Test that the cleaned data has the expected structure and content."""
        # Cleaned data
        nla_data, ecos_data = clean_data()

        # Assert that both datasets are non-empty
        self.assertFalse(nla_data.empty, "Cleaned NLA data is empty")
        self.assertFalse(ecos_data.empty, "Cleaned ECOS data is empty")

        # Assert specific columns exist
        self.assertIn('Study_Population', nla_data.columns, "Missing 'Study_Population' column in NLA data")
        self.assertIn('Site_Id', ecos_data.columns, "Missing 'Site_Id' column in ECOS data")

    def test_sqlite_content(self):
        """Test that the SQLite database contains expected data."""
        conn = sqlite3.connect('./data/water_quality_analysis.db')
        cursor = conn.cursor()

        # Check data exists in the `nla_condition` table
        cursor.execute("SELECT COUNT(*) FROM nla_condition")
        nla_count = cursor.fetchone()[0]
        self.assertGreater(nla_count, 0, "No data in 'nla_condition' table")

        # Check data exists in the `ecos_data` table
        cursor.execute("SELECT COUNT(*) FROM ecos_data")
        ecos_count = cursor.fetchone()[0]
        self.assertGreater(ecos_count, 0, "No data in 'ecos_data' table")

        conn.close()

    def test_no_missing_values(self):
        """Test that there are no missing values in the cleaned data."""
        nla_data, ecos_data = clean_data()
        self.assertEqual(nla_data.isnull().sum().sum(), 0, "NLA cleaned data contains missing values")
        self.assertEqual(ecos_data.isnull().sum().sum(), 0, "ECOS cleaned data contains missing values")

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)


# In[ ]:





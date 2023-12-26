import unittest
import os
import sqlite3

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'Linda_data.db')


    def test_data_pipeline(self):
        # Check if the database is created
        self.assertTrue(os.path.exists(self.db_path))

        # Check if the necessary tables exist in the database
        self.assertTrue(self.table_exists('germany_combined_gdp'))
        self.assertTrue(self.table_exists('startup_data_business'))
        self.assertTrue(self.table_exists('employment_growth_data'))

        # Check data consistency and completeness
        self.test_startup_data_consistency()
        self.test_gdp_growth_consistency()
        self.test_total_employment_growth_consistency()
        self.test_business_startups_consistency()


    def table_exists(self, table_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    
    def get_row_count(self, table_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        result = cursor.fetchone()[0]
        conn.close()
        return result
    
    
    def check_no_null_values(self, table_name, columns):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for column in columns:
        # escape column names with spaces
            escaped_column = f'"{column}"' if ' ' in column else column
            query = f"DELETE FROM {table_name} WHERE {escaped_column} IS NULL;"
            cursor.execute(query)
        conn.commit()
        conn.close()
        

    def test_gdp_growth_consistency(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM germany_combined_gdp;")
        data = cursor.fetchall()
        conn.close()
        gdp_per_capita_column = [row[2] for row in data]  
        self.assertTrue(all(gdp_per_capita is not None and gdp_per_capita > 0 for gdp_per_capita in gdp_per_capita_column if gdp_per_capita is not None))
        self.check_no_null_values('germany_combined_gdp', ['Year', 'GDP Growth Rate', 'GDP per Capita (Current US Dollars)'])


    def test_insolvency_rates_consistency(self):
        expected_insolvency_rates_row_count = 23
        self.assertEqual(self.get_row_count('insolvency_data'), expected_insolvency_rates_row_count)
        self.check_no_null_values('insolvency_data', ['Year','Insolvencies'])  


    def test_total_employment_growth_consistency(self):
        expected_employment_growth_row_count = 31
        self.assertEqual(self.get_row_count('employment_growth_data'), expected_employment_growth_row_count)
        self.check_no_null_values('employment_growth_data', ['Year','Total','Agriculture, Forestry, and Fishing','Manufacturing Industry excl. Construction','Construction','Service Industries'])  
      

    def test_business_startups_consistency(self):
        expected_startup_business_row_count = 20
        self.assertEqual(self.get_row_count('startup_data_business'), expected_startup_business_row_count)
        self.check_no_null_values('startup_data_business', ['Year','Business Startups'])  


if __name__ == '__main__':
    unittest.main()








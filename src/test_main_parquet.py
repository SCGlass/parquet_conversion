import unittest
from main_parquet import import_csv, CsvCleaner, upload_file, process_lambda
import pandas as pd 

class TestMainParquet(unittest.TestCase):
    def setUp(self):
        # set up mock data and objects for training
        self.test_bucket_name = "test-bucket"
        self.test_file_key = "test-file.csv"
        self.test_df = pd.DataFrame({"Timestamp": [1707752941,float("nan"), 123456789,1707752942, "ERROR",1707752943,1707752944],
                                     "speed_over_ground": [0.54, 19.96, -10.65, 21.12, 15.04, float("nan"), "ERROR"],
                                     "Longitude": [-179.567485,115.456454, 179.364547, -190.454637, 190.464735, float("nan"), "ERROR"],
                                     "Latitude": [-89.354635, 89.1342524,50.142534, -100.142534, 100.857463, float("nan"), "ERROR" ],
                                     "engine_fuel_rate": [0.54, 99.45, -10.34, 150.78, 45.78, float("nan"), "ERROR"]})


    def test_timestamp_clean(self):
        # call the function
        cleaned_df, rows_removed = CsvCleaner.timestamp_clean(self.test_df.copy(), 'Timestamp')

        cleaned_df.reset_index(drop=True, inplace=True)
        self.test_df.reset_index(drop=True, inplace=True)

        # Check if the returns work
        self.assertIsInstance((cleaned_df, rows_removed), tuple)
        self.assertEqual(cleaned_df.shape, (4,5)) # check DataFrame shape
        self.assertEqual(rows_removed, 1) # check number of rows removed
        self.assertTrue(cleaned_df.isnull().any().any()) # check for NaN values
        self.assertTrue((cleaned_df["Timestamp"].diff().dropna() >= pd.Timedelta(seconds=0)).all()) # Check timestamp sorting
        self.assertNotIn("ERROR", cleaned_df["Timestamp"]) # check for invalid timestamps
        self.assertTrue((cleaned_df.columns == self.test_df.columns).all()) #check data integrity (columns)
                               
                               
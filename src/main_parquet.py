import boto3
import pandas as pd




class CsvCleaner:
    @staticmethod
    def timestamp_clean(df, col_name):
        # convert the column to numeric with any errors(for example strings or letter) to NaN
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        
        # Interpolate NaN values in the timestamp column
        df[col_name] = df[col_name].interpolate()

        # Calculate the initial number of rows
        initial_rows = df.shape[0]
        
        #Filter out rows with "Timestamp" values not containing 10 digits
        df = df[df[col_name].apply(lambda x: len(str(int(x))) == 10)]

        #calculate how many rows removed
        rows_removed = initial_rows - df.shape[0]

        # Convert the Unix timestamp to datetime with seconds
        df[col_name] = pd.to_datetime(df[col_name], unit="s")

        # Sort the DataFrame by the timestamp column
        df = df.sort_values(by=col_name)

        return df, rows_removed
    
    @staticmethod
    def clean_columns(df, col_name, low, high):
    
        # Convert column to numeric, making errors to Nan instead
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        # Calculate the initial number of rows
        initial_rows = df.shape[0]

        df = df[(df[col_name] >= low) & (df[col_name] <= high)]

        # Calculate the number of rows removed
        rows_removed = initial_rows - df.shape[0]

        df[col_name] = df[col_name].interpolate()

        return df, rows_removed
    
    @staticmethod
    def clean_file(csv_file):
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        total_rows_removed = 0

        # Clean the DataFrame
        for col in df.columns:
            if "Timestamp" in col:
                df, rows_removed = CsvCleaner.timestamp_clean(df, col)
                total_rows_removed += rows_removed
            
            if "speed_over_ground" in col:
                low = 0
                high = 100
                df, rows_removed = CsvCleaner.clean_columns(df, col, low, high)
                total_rows_removed += rows_removed
            
            if "Longitude" in col:
                low = -180
                high = 180
                df, rows_removed = CsvCleaner.clean_columns(df, col, low, high)
                total_rows_removed += rows_removed

            if "Latitude" in col:
                low = -90
                high = 90
                df, rows_removed = CsvCleaner.clean_columns(df, col, low, high)
                total_rows_removed += rows_removed

            if "engine_fuel_rate" in col:
                low = 0
                high = 100
                df,rows_removed = CsvCleaner.clean_columns(df, col, low, high)
                total_rows_removed += rows_removed

        # Save the cleaned DataFrame as a Parquet file
        cleaned_parquet_file = csv_file.replace(".csv", ".parquet")
        df.to_parquet(cleaned_parquet_file, index=False)

        print(f"Total rows removed: {total_rows_removed}")

        return cleaned_parquet_file



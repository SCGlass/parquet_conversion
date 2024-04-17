import boto3
import pandas as pd
import traceback
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path


def import_csv(bucket_name:str, file_key:str) -> pd.DataFrame:
    """
    Imports a CSV file from an S3 bucket into a Pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key of the file in the S3 bucket.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the data from the CSV file,
        or None if an error occurred during the import process.
    """
    
    s3 = boto3.client('s3')

    # Read CSV file from S3 into a Pandas DataFrame
    try:
        # Use 's3.get_object' to get the object and 'pd.read_csv' to read it into a DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        return df
        
    except Exception as e:
        print(f"Error importing CSV file from S3: {e}")
    
        traceback.print_exc()
        return None



class CsvCleaner:
    @staticmethod
    def timestamp_clean(df:pd.DataFrame, col_name:str) -> tuple[pd.DataFrame,int]:
        """
        Cleans the DataFrame by converting the specified column to numeric, 
        filtering out rows with invalid timestamps, converting timestamps to datetime, 
        sorting by timestamp, and returning the cleaned DataFrame along with 
        the number of rows removed.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            col_name (str): The name of the column containing timestamps.

        Returns:
            tuple[pd.DataFrame, int]: A tuple containing the cleaned DataFrame 
            and the number of rows removed.
        """
        # convert the column to numeric with any errors(for example strings or letter) to NaN
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        
        df.dropna(subset=[col_name], inplace=True)

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
    def clean_columns(df: pd.DataFrame, col_name: str, low: float, high: float) -> tuple[pd.DataFrame, int]:
        """
        Cleans the specified column in the DataFrame by converting it to numeric, 
        filtering out values that are not within the specified range, 
        and returning the cleaned DataFrame along with the number of rows removed.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            col_name (str): The name of the column to be cleaned.
            low (float): The lower bound of the acceptable range.
            high (float): The upper bound of the acceptable range.

        Returns:
            tuple[pd.DataFrame, int]: A tuple containing the cleaned DataFrame 
            and the number of rows removed.
        """

        # Convert column to numeric, making errors to Nan instead
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        # Calculate the initial number of rows
        initial_rows = df.shape[0]

        df.loc[~df[col_name].between(low, high), col_name] = float('nan')

        # Calculate the number of rows removed
        rows_removed = initial_rows - df.shape[0]

        return df, rows_removed
    
    @staticmethod
    def clean_file(df: pd.DataFrame, file_key: str, bucket_name:str) ->str:
        """
        Cleans the DataFrame by applying specific cleaning operations based on column names,
        saves the cleaned DataFrame as a Parquet file, and returns the path to the cleaned Parquet file.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            file_key (str): The key of the file.

        Returns:
            str: The path to the cleaned Parquet file.
        """
        
        total_rows_removed = 0

        # Clean the DataFrame
        for col in df.columns:
            if "Timestamp" in col:
                df, rows_removed = CsvCleaner.timestamp_clean(df.copy(), col)
                total_rows_removed += rows_removed
            
            if "speed_over_ground" in col:
                low = 0
                high = 100
                df, rows_removed = CsvCleaner.clean_columns(df.copy(), col, low, high)
                df[col] = df[col].round(2)
                total_rows_removed += rows_removed
            
            if "Longitude" in col:
                low = -180
                high = 180
                df, rows_removed = CsvCleaner.clean_columns(df.copy(), col, low, high)
                total_rows_removed += rows_removed

            if "Latitude" in col:
                low = -90
                high = 90
                df, rows_removed = CsvCleaner.clean_columns(df.copy(), col, low, high)
                total_rows_removed += rows_removed

            if "engine_fuel_rate" in col:
                low = 0
                high = 100
                df,rows_removed = CsvCleaner.clean_columns(df.copy(), col, low, high)
                df[col] = df[col].round(2)
                total_rows_removed += rows_removed

        # Resample the DataFrame
        df.set_index('Timestamp', inplace=True)
        df = df.resample('10s').mean()  # No fillna(0) here
        df = df.reset_index()
        
        # Save as partitioned Parquet file
        parquet_file = CsvCleaner._partition_and_save(df, file_key, bucket_name)

        print(f"Total rows removed: {total_rows_removed}")

        return parquet_file
    
    @staticmethod
    def _partition_and_save(df: pd.DataFrame, file_key: str, bucket_name: str) -> str:
        """
        Partitions and saves the cleaned DataFrame as a Parquet file.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            file_key (str): The key of the file.

        Returns:    
            str: The path to the saved Parquet file.
        """

        # use path lib with file key to get the vessel name
        # Extract vessel name from the file key
        vessel_name = file_key.split('_')[0] # use path lib here for realdata
        file_key_without_extension = file_key.replace('.csv', '')

        df_copy = df.copy()

        # Partition by timestamp
        df_copy["year"] = df["Timestamp"].dt.year.astype(str)
        df_copy["month"] = df["Timestamp"].dt.month.astype(str).str.zfill(2)
        df_copy["day"] = df["Timestamp"].dt.day.astype(str).str.zfill(2)
        

        # Define the partition keys
        partition_cols = ["year", "month", "day"]

        partition_df = df_copy.drop(columns=partition_cols)
        

        # Iterate over each partition and save corresponding Parquet file
        for _, partition in df_copy.groupby(partition_cols):
            
            partition_path = "/".join(f"{col}={partition[col].iloc[0]}" for col in partition_cols)
            parquet_file_name = f"{vessel_name}/{partition_path}/{file_key_without_extension}.parquet" # do this locally
            
            # Write Parquet file to S3
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(pa.Table.from_pandas(partition_df), parquet_buffer)
            parquet_bytes = parquet_buffer.getvalue().to_pybytes()
            
            s3_resource = boto3.resource("s3")
            s3_object = s3_resource.Object(bucket_name, parquet_file_name)
            s3_object.put(Body=parquet_bytes)

            parquet_file_path = f"s3://{bucket_name}/{parquet_file_name}"
            print(f"Parquet file saved at: {parquet_file_path}")

        return parquet_file_path
        


def process_lambda(event, context):
    """
    Processes an event triggered Lambda function.

    Args:
        event (dict): The event that triggered the function.
        context (LambdaContext): The Lambda execution context.

    Returns:
        None
    """
    # Extracting necessary info from the event
    #bucket_name = event['Records'][0]['s3']['bucket']['name'] 
    #file_key = event['Records'][0]['s3']['object']['key']

    file_key = event
    

    print(bucket_name)
    print(file_key)

    print(f"Function Name: {context.function_name}")
    print(f"Function Version: {context.function_version}")
    print(f"AWS Request ID: {context.aws_request_id}")

    destination_bucket_name = "new-parquet-files" # Change to the actual bucket

    #Import CSV file from S3
    df= import_csv(bucket_name, file_key)

    if df is None:
        print("CSV import failed. Exiting function")
        return
    
    # Clean the Dataframe
    cleaned_parquet_file = CsvCleaner.clean_file(df, file_key, destination_bucket_name)

    remaining_time_ms = context.get_remaining_time_in_millis()
    print(f"Remaining Time (ms): {remaining_time_ms}")

    return cleaned_parquet_file

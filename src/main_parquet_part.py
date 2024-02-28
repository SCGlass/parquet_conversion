import boto3
import pandas as pd
import os
import traceback
import pyarrow.parquet as pq


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
        
    except Exception as e:
        print(f"Error: {e}")
    
        print(f"{traceback.format_exc()=}")
    return df



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

        #df[col_name] = df[col_name].interpolate()

        return df, rows_removed
    
    @staticmethod
    def clean_file(df: pd.DataFrame, file_key: str) ->str:
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
                total_rows_removed += rows_removed

        # Save the cleaned DataFrame as a partitinoed Parquet file
        vessel_name = file_key.split('_')[0] # This is just a test so takes first part of the file, can change this to take in regex

        # Partition by timestamp and vessel name
        df["date"] = df["Timestamp"].dt.date
        df["vessel_name"] = vessel_name

        # Define the partition keys
        partition_cols = ["date", "veseel_name"]

        # Save as partitioned Parquet file
        cleaned_parquet_file = f"tmp/{file_key}.parquet"
        table = pq.Table.from_pandas(df)
        pq.write_to_dataset(table, root_path=cleaned_parquet_file, partition_cols=partition_cols)

        print(f"Total rows removed: {total_rows_removed}")

        return cleaned_parquet_file
        

def upload_file(parquet_file: str, bucket_name: str) -> None:
    """
    Uploads a Parquet file to an S3 bucket.

    Args:
        parquet_file (str): The path to the Parquet file.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        None
    """
    s3_resource = boto3.resource("s3")

    try:
        filename = os.path.basename(parquet_file)
        
        s3_resource.Bucket(bucket_name).upload_file(
            Filename = parquet_file,
            Key = filename )
        
        print(f"Uploaded {filename} to {bucket_name}")
    
    except Exception as e:
        print(f"Error: {e}")


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
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(bucket_name)
    print(file_key)

    print(f"Function Name: {context.function_name}")
    print(f"Function Version: {context.function_version}")
    print(f"AWS Request ID: {context.aws_request_id}")

    destination_bucket_name = "new-parquet-files"

    #Import CSV file from S3
    df= import_csv(bucket_name, file_key)
    
    # Clean the Dataframe
    cleaned_parquet_file = CsvCleaner.clean_file(df, file_key)
    
    # Add AWS glue function here

    # Upload cleaned file to S3
    upload_file(cleaned_parquet_file, destination_bucket_name)

    remaining_time_ms = context.get_remaining_time_in_millis()
    print(f"Remaining Time (ms): {remaining_time_ms}")

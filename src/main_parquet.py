import boto3
import pandas as pd
import os


def import_csv(bucket_name, file_key):
    s3 = boto3.client('s3', 
                    aws_access_key_id= os.getenv("AWS_ACCESS_KEY_ID"), 
                    aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"), 
                    region_name= os.getenv("AWS_REGION_NAME"))

    # Read CSV file from S3 into a Pandas DataFrame
    try:
        # Use 's3.get_object' to get the object and 'pd.read_csv' to read it into a DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        
    except Exception as e:
        print(f"Error: {e}")

    return df



class CsvCleaner:
    @staticmethod
    def timestamp_clean(df, col_name):
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
    def clean_columns(df, col_name, low, high):
    
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
    

def upload_file(parquet_file, bucket_name):

    s3_resource = boto3.resource(
        "s3",
        region_name = os.getenv("AWS_REGION_NAME"),
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"))

    try:
        s3_resource.Bucket(bucket_name).upload_file(
            Filename = parquet_file,
            Key = os.path.basename(parquet_file))
    
    except Exception as e:
        print(f"Error: {e}")


def process_lambda(event, context):
    
    # Extracting necessary info from the event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    print(f"Function Name: {context.function_name}")
    print(f"Function Version: {context.function_version}")
    print(f"AWS Request ID: {context.aws_request_id}")

    destination_bucket_name = "new-parquet-files"

    #Import CSV file from S3
    df = import_csv(bucket_name, file_key)
    
    # Clean the Dataframe
    cleaned_parquet_file = CsvCleaner.clean_file(df)
    
    # Upload cleaned file to S3
    upload_file(cleaned_parquet_file, destination_bucket_name)

    remaining_time_ms = context.get_remaining_time_in_millis()
    print(f"Remaining Time (ms): {remaining_time_ms}")

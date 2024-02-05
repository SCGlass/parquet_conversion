import boto3
import pandas as pd
import numpy as np



def load_df_from_s3(bucket_name, key):
    """
    Read a CSV from S3 bucket & load into pandas dataframe
    """
    s3 = GlobalVariables.s3.client
    logger.info("Satrting S3 object retrieval process...")
    try:
        get_response = s3.get_object(Bucket=bucket_name, Key=key)
        logger.info("Object retrieved from S3 bucket successfully")
    except ClientErrot as e:
        logger.error(f"S3 object cannot be retrieved: {e}")

    file_content = get_response["Body"].read()
    df = pd.read_csv(io.BytesIO(file_content)) # This is necessary transformation from S3 to pandas

    return df
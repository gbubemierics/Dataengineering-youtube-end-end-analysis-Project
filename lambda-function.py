import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Read required environment variables.
# These are injected into the Lambda configuration.
# They control where data is written and how Glue is updated.
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    """
    Lambda entry point.
    Triggered by an S3 event when a new object is created.
    Reads a JSON file from S3, normalizes nested data,
    and writes the result as Parquet to a cleansed S3 layer
    while updating the Glue Data Catalog.
    """

    # Extract bucket name from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']

    # Extract and decode the object key
    # This handles spaces and special characters in S3 paths
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8'
    )

    try:
        # Read the raw JSON file directly from S3 into a DataFrame
        # awswrangler handles the S3 connection and deserialization
        df_raw = wr.s3.read_json(f"s3://{bucket}/{key}")

        # Flatten the nested "items" field in the JSON structure
        # json_normalize converts nested objects into tabular columns
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write the transformed data back to S3 in Parquet format
        # dataset=True enables partitioned, table-style storage
        # database and table update the Glue Data Catalog
        # mode controls overwrite or append behavior
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        # Return the awswrangler response metadata
        return wr_response

    except Exception as e:
        # Log the exception for CloudWatch troubleshooting
        print(e)
        print(
            f"Error getting object {key} from bucket {bucket}. "
            "Make sure the object exists and the bucket is in the same region as this Lambda."
        )
        # Re-raise the exception so Lambda reports a failure
        raise e

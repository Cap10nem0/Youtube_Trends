import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Retrieve environment variables
s3_cleansed_layer_path = os.environ['s3_cleansed_layer']
glue_catalog_db_name = os.environ['glue_catalog_db_name']
glue_catalog_table_name = os.environ['glue_catalog_table_name']
write_data_operation_mode = os.environ['write_data_operation']

def lambda_handler(event, context):
    
	# Extract information from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Read JSON content from S3 and create a DataFrame
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extract required columns using json_normalize
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write DataFrame to Parquet format in S3 and update Glue Catalog
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=s3_cleansed_layer_path,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_table_name,
            mode=write_data_operation_mode
        )

        return wr_response
    except Exception as e:
        # Print error details and raise the exception
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

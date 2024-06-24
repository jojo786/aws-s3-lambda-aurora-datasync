from boto3.session import Session 
import os
import json

session = Session()  
rds_data = session.client(
    service_name='rds-data'
)

DBClusterArn = os.environ['DBClusterArn']
DBName = os.environ['DBName']
SecretArn = os.environ['SecretArn']

def run_command(sql_statement, sql_values=None):
    print(f"SQL statement: {sql_statement}")
    result = ''
    
    if not sql_values:
        #Use the Data API ExecuteStatement operation to run the SQL command
        result = rds_data.execute_statement(
            resourceArn=DBClusterArn,
            secretArn=SecretArn,
            database=DBName,
            sql=sql_statement
        )
    else:    
        result = rds_data.execute_statement(
            resourceArn=DBClusterArn,
            secretArn=SecretArn,
            database=DBName,
            sql=sql_statement,
            includeResultMetadata=True,
            parameters=[
                {
                    'name':'artist', 
                    'value':{'stringValue':sql_values['artist']}
                },
                {
                    'name':'album',
                    'value':{'stringValue':sql_values['album']}
                }
            ] 
        )

    #print(f"SQL Result: {result}")
    return result

def lambda_handler(event, context):
   
    #Log event object and database name to CloudWatch Logs
    print(f"Event: {event}")
    print(f"Database Name: {DBName}")

    # Extract the bucket name and object key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
            


    

  

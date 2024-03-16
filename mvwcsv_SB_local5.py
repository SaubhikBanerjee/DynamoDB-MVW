# import required libraries..
# Some libraries are NOT used in the example code, but in real life you may need those.
import boto3
import csv
import sys
import pandas as pd
import ast
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

# Saubhik: This will load csv file from local to AWS DynamoDB.

# Access Credentials
###################

# Change the access key as per your need.
a_key = "AKIA6E4JE5XXXXXX"

# Change the sccret key as per your need.
a_S_key = "mmDFZ6mNLWa+eviXXXXXX+oYYYYYYdHAM"

# Change the region as required
region = 'ap-south-1'

"""
The above section is just an example to show you that, you need AWS access credentials.
You can not do that is real world.
In real project a profile is configured using AWS CLI: aws configure --profile your_profile_name
Download and install AWS CLI from https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
Then the profile name is read from a configuration file called config.ini using ConfigParser 
"""

# Dataset contains lots of duplicate, So overwrite_keys provided. You cannot perform multiple
# operations on the same item in the same BatchWriteItem request. 
overwrite_keys = ["xpmID"]

# CSV file name, change this as per your need
filename = "C:\Saubhik\Project\DynamoDB\CSV\mvw_at04162023.csv"
# filename="C:\Saubhik\Project MVW\DynamoDB\CSV\mvw_at07062023.csv"

# Error file name.
errorfile = "C:\Saubhik\Project\DynamoDB\CSV\mycsv_error.csv"

"""
The above two and the below DynamoDB table mane is command line argument for the Python 
script in real project. This is done by importing sys and using sys.arv

"""

# Connecting to DynamoDB
try:
    dynamodb = boto3.resource("dynamodb",
                              aws_access_key_id=a_key,
                              aws_secret_access_key=a_S_key,
                              region_name=region,
                              config=Config(
                                  retries={"max_attempts": 5, "mode": "adaptive"},
                                  max_pool_connections=1024,
                              )
                              )

except Exception as error:
    print(error)
    raise
# DynamoDB table name, change it as per need.

try:
    table = dynamodb.Table("mwcsv8jun")
except Exception as error:
    print("Error loading DynamoDB table. Check if table was created correctly and environment variable")
    print(error)
    raise

# CSV Reading using Panda data frame - SB.
# Note the data types.
Customers = pd.read_csv(filename, dtype={'createDate': 'Int32',
                                         'cognitoSub': str,
                                         'customerIdHash': str,
                                         'username': str,
                                         'mpmID': str}
                        )


# Here you may get "Float types are not supported. Use Decimal types instead." This is
# a very common error in DynamoDB.

# Trying to write in batch to get faster inserts. overwrite_by_pkeys=overwrite_keys
try:
    with table.batch_writer(overwrite_by_pkeys=overwrite_keys) as batch:
        for i, record in enumerate(Customers.to_dict("records")):
            # add to dynamodb
            try:

                batch.put_item(
                    Item=record
                )

            except Exception as error:
                print(error)
                print(record)
                print("End of Data Load.. starting new batch automatically..")
                # Writing an error file. This is written more dynamically in real project.
                try:
                    with open(errorfile, 'a') as f:
                        f.writelines(
                            str(record["xpmID"]) + "," + str(record["cognitoSub"]) + "," + str(record["customerIdHash"])
                            + "," + str(record["username"]) + "," + str(record["createDate"]) + "," + str(error) + '\n')
                except Exception as error:
                    print(error)
except Exception as error:
    print(error)

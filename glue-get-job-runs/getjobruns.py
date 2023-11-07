from datetime import datetime

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with AWS Glue to
create and manage crawlers, databases, and jobs.
"""

import logging
from botocore.exceptions import ClientError
import boto3
import pandas as pd

logger = logging.getLogger(__name__)

glue_client = (
    boto3.client("glue")
    # boto3.resource("iam").Role(args.role_name),
    # boto3.resource("s3").Bucket(args.bucket_name),
)    

def get_job_runs(glueclient, job_name):
    """
    Gets information about runs that have been performed for a specific job
    definition.

    :param job_name: The name of the job definition to look up.
    :return: The list of job runs.
    """
    try:
        response = glueclient.get_job_runs(JobName=job_name)
    except ClientError as err:
        logger.error(
            "Couldn't get job runs for %s. Here's why: %s: %s",
            job_name,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        return response["JobRuns"]
        
# aws glue get-job-runs --job-name "almis-save-automated-rds-report-refresh"
# runs_results = get_job_runs(glueclient=glue_client, job_name = "almis-save-automated-rds-report-refresh")
runs_results = get_job_runs(glueclient=glue_client, job_name = "cfp-accounts-stage-prod")
df = pd.DataFrame(runs_results)
# print(df)



# dffailed = df.loc[df['JobRunState'] == 'FAILED']
# print(dffailed)
dfsuccess = df.loc[df['JobRunState'] == 'SUCCEEDED']
# dfsuccess = df.loc[df['StartedOn'] >= '2023-07-20']

# mindate = min(dfsuccess['StartedOn'])
# maxdate = max(dfsuccess['StartedOn'])

# print(f'mindate is {mindate}, maxdate is {maxdate}')


# num_of_rows = len(dfsuccess)
# print(f"The number of rows is {num_of_rows}")

# filtereddf = dffailed[dffailed['ErrorMessage'].str.lower().str.contains("validation")] # Search for "text", after converting  to lowercase   
# print('\nResult dataframe :\n', filtereddf)



#Get failed runs 
next_token = ""
client = boto3.client('glue',region_name='eu-west-1')
failedruns = 0

while True:
  response = glue_client.get_job_runs(JobName="cfp-accounts-stage-prod", NextToken = next_token)
  for jobrun in response["JobRuns"]:
    if jobrun['JobRunState'] == 'FAILED' and "check failed" in jobrun['ErrorMessage'].lower():
       failedruns += 1
  next_token = response.get('NextToken')
  if next_token is None:
    break
print(f'failedruns is {failedruns}')

#Get successful runs 
next_token = ""
client = boto3.client('glue',region_name='eu-west-1')
successfulruns = 0

while True:
  response = glue_client.get_job_runs(JobName="cfp-accounts-stage-prod", NextToken = next_token)
  for jobrun in response["JobRuns"]:
    if jobrun['JobRunState'] == 'SUCCEEDED':
       successfulruns += 1
  next_token = response.get('NextToken')
  if next_token is None:
    break
print(f'successfulruns is {failesuccessfulrunsdruns}')

#all runs 
next_token = ""
client = boto3.client('glue',region_name='eu-west-1')
totalruns = 0

while True:
  response = glue_client.get_job_runs(JobName="cfp-accounts-stage-prod", NextToken = next_token)
  for jobrun in response["JobRuns"]:
    totalruns += 1
  next_token = response.get('NextToken')
  if next_token is None:
    break
print(f'totalruns is {totalruns}')
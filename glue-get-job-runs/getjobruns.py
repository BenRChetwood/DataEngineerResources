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
# dfsuccess = df.loc[df['StartedOn'] >= '2023-11-01']

mindate = min(dfsuccess['StartedOn'])
maxdate = max(dfsuccess['StartedOn'])

print(f'mindate is {mindate}, maxdate is {maxdate}')

# print(dfsuccess)

# filtereddf = dffailed[dffailed['ErrorMessage'].str.lower().str.contains("validation")] # Search for "text", after converting  to lowercase   
# print('\nResult dataframe :\n', filtereddf)


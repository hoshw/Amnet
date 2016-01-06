
# coding: utf-8

# In[5]:

import argparse
import json
import time
import uuid


# In[6]:

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


# In[69]:

def load_table(bigquery, project_id, dataset_id, table_name, source_schema,
               source_path, num_retries=5):
    """
    Starts a job to load a bigquery table from CSV

    Args:
        bigquery: an initialized and authorized bigquery client
        google-api-client object
        source_schema: a valid bigquery schema,
        see https://cloud.google.com/bigquery/docs/reference/v2/tables
        source_csv: the fully qualified Google Cloud Storage location of
        the data to load into your table

    Returns: a bigquery load job, see
    https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
    """

    # Generate a unique job_id so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'load': {
                'sourceUris': [source_path],
                "sourceFormat": "CSV",
                "fieldDelimiter": "þ",
                "skipLeadingRows": 1,
                'schema': {
                    'fields': source_schema
                },
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_name
                }
            }
        }
    }

    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)


# In[72]:

# [START run]
def main(project_id, dataset_id, table_name, schema_file, data_path, num_retries):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    with open(schema_file, 'r') as f:
       schema = json.load(f)

    job = load_table(
        bigquery,
        project_id,
        dataset_id,
        table_name,
        schema,
        data_path,
        num_retries)

# [END run]


# In[73]:

main(project_id="discover-1110", dataset_id="DiscoverMediaData", table_name="imps", schema_file=schemalocal, data_path="gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-15-2015.log.gz",poll_interval=1000, num_retries=5)


# In[50]:

schemalocal="impSchema.json"


# In[84]:

schemalocalaction="actionSchema.json"


# In[76]:

files = [
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-16-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-17-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-18-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-19-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-20-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-21-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-22-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-23-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-24-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-25-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-26-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-27-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-28-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-29-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_06-30-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-01-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-02-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-03-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-04-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-05-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-06-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-07-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-08-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-09-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-10-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-11-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-12-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-13-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-14-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-15-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-16-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-17-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-18-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-19-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-20-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-21-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-22-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-23-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-24-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-25-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-26-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-27-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-28-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-29-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-30-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_07-31-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-01-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-02-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-03-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-04-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-05-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-06-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-07-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-08-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkImpression_7285_08-09-2015.log.gz"
]


# In[80]:

for x in files:
    main(project_id="discover-1110", dataset_id="DiscoverMediaData", table_name="imps", schema_file=schemalocal, data_path=x,poll_interval=1000, num_retries=5)


# In[82]:

actions = [
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-01-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-02-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-03-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-04-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-05-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-06-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-07-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-08-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-09-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-10-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-11-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-12-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-13-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-14-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-15-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-16-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-17-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-18-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-19-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-20-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-21-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-22-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-23-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-24-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-25-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-26-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-27-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-28-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-29-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_06-30-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-01-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-02-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-03-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-04-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-05-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-06-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-07-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-08-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-09-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-10-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-11-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-12-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-13-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-14-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-15-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-16-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-17-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-18-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-19-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-20-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-21-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-22-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-23-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-24-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-25-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-26-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-27-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-28-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-29-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-30-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_07-31-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-01-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-02-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-03-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-04-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-05-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-06-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-07-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-08-2015.log.gz",
"gs://dfa_-54fe4bd7f3ec8d6b8e31f6f2a18f9fa261ccae56/NetworkActivity_7285_3853263_08-09-2015.log.gz"
]


# In[ ]:

for x in actions:
    main(project_id="discover-1110", dataset_id="DiscoverMediaData", table_name="actions", schema_file=schemalocalaction, data_path=x,poll_interval=1000, num_retries=5)


# In[ ]:




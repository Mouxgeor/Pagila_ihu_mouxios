#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[ ]:


# Run the following commands once, in order to install libraries - DO NOT Uncomment this line.

# Uncomment below lines
#
# !pip3 install --upgrade pip
# !pip install google-cloud-bigquery
# !pip install pandas-gbq -U
# !pip install db-dtypes
# !pip install packaging --upgrade


# # Import libraries

# In[1]:


# Import libraries
from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')


# In[2]:


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/Mouxios Giorgos/Downloads/Data Analytics/warm-influence-480112-t2-3027da7e0b9c.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[3]:


# Set your Google Cloud project ID and BigQuery dataset details

# -- YOUR CODE GOES BELOW THIS

project_id = 'warm-influence-480112-t2' # Edit with your project id
dataset_id = 'staging_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'stg_payment' # Modify the necessary table name: stg_customer, stg_city etc.

# -- YOUR CODE GOES ABOVE THIS LINE


# # SQL Query

# In[5]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query ='''
with base as (
  select *
  from `warm-influence-480112-t2.pagila_production_mouxpublic.payment`
),
final as (
  select
    payment_id,
    customer_id AS payment_customer_id,
    staff_id AS payment_staff_id,
    rental_id AS payment_rental_id,
    amount AS payment_amount,
    payment_date AS payment_payment_date
  FROM base
)
select * from final

'''

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# # Write to BigQuery

# In[8]:


# Define the full table ID
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('payment_id', 'INTEGER'),
    bigquery.SchemaField('payment_customer_id', 'INTEGER'),
    bigquery.SchemaField('payment_staff_id', 'INTEGER'),
    bigquery.SchemaField('payment_rental_id', 'INTEGER'),
    bigquery.SchemaField('payment_amount', 'NUMERIC'),
    bigquery.SchemaField('payment_payment_date', 'DATETIME')
]


# -- YOUR CODE GOES ABOVE THIS LINE


# In[9]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)
if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")


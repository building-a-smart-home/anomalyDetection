from google.cloud import bigquery
from google.oauth2 import service_account

BQ_CREDENTIALS_FILE = '../credentials/bq_credentials.json'
# Create BQ credentials object
credentials = service_account.Credentials.from_service_account_file(BQ_CREDENTIALS_FILE)

# Construct a BigQuery client object
bq_client = bigquery.Client(credentials=credentials)

import datetime
import sys

from google.cloud import bigquery
from google.oauth2 import service_account


key = '/home/timur/swiftstack/kakeibo/gcp-key.json'
creds = service_account.Credentials.from_service_account_file(
    key, scopes=["https://www.googleapis.com/auth/cloud-platform"])
client = bigquery.Client(project=creds.project_id, credentials=creds)
filename = sys.argv[1]
dataset_id = 'kakeibo'
table_id = 'Kakeibo'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

rows = []
with open(filename, "r") as source_file:
    for line in source_file:
        if line.startswith('Person'):
            continue
        try:
            person, date, merchant, amount, kind = line.strip().lower.split(
                ',')
        except Exception:
            print(line)
            raise
        try:
            date = datetime.datetime.strptime(
                date, '%m/%d/%y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.datetime.strptime(
                date, '%m/%d/%Y').strftime('%Y-%m-%d')

        rows.append({"Person": person,
                     "Date": datetime.datetime.strptime(
                        date, '%m/%d/%y').strftime('%Y-%m-%d'),
                     "Merchant": merchant,
                     "Kind": kind,
                     "Amount": float(amount),
                     "Comments": None})

job = client.load_table_from_json(rows, table_ref, job_config=job_config)

try:
    job.result()  # Waits for table load to complete.
except Exception:
    print(job.errors)

print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id,
                                          table_id))

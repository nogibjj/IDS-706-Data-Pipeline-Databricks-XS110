import requests
from pyspark.dbutils import DBUtils

# Initialize DBUtils
dbutils = DBUtils(spark)

# The raw GitHub URL of the CSV file
github_csv_url = 'https://raw.githubusercontent.com/username/repo/branch/filename.csv'

# DBFS destination path
dbfs_path = '/FileStore/IDS706_Data_Pipeline/filename.csv'

# Download the file content from GitHub
response = requests.get(github_csv_url)
response.raise_for_status()  # will raise an exception if there is a download error

# Write the file content to DBFS
dbutils.fs.put(dbfs_path, response.text, overwrite=True)

# Command to confirm the file is in DBFS (optional)
display(dbutils.fs.ls(dbfs_path))

# import requests
# import os
# import base64
# import json
# from dotenv import load_dotenv

# # Load Databricks environment variables

# load_dotenv()
# DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
# DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

# # Base URL for Databricks REST API
# url = f"https://{DATABRICKS_HOST}/api/2.0"

# # Common headers for Databricks REST API requests
# headers = {
#     "Authorization": f"Bearer {DATABRICKS_TOKEN}",
#     "Content-Type": "application/json"
# }

# # Functions for DBFS operations
# def perform_query(path, headers, data={}):
#     session = requests.Session()
#     resp = session.request('POST', url + path, 
#                            data=json.dumps(data), 
#                            verify=True, 
#                            headers=headers)
#     return resp.json()

# def mkdirs(path, headers):
#     _data = {'path': path}
#     return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

# def create(path, overwrite, headers):
#     _data = {'path': path, 'overwrite': overwrite}
#     return perform_query('/dbfs/create', headers=headers, data=_data)

# def add_block(handle, data, headers):
#     _data = {'handle': handle, 'data': data}
#     return perform_query('/dbfs/add-block', headers=headers, data=_data)

# def close(handle, headers):
#     _data = {'handle': handle}
#     return perform_query('/dbfs/close', headers=headers, data=_data)

# def put_file_from_url(url, dbfs_path, overwrite, headers):
#     response = requests.get(url)
#     if response.status_code == 200:
#         content = response.content
#         handle = create(dbfs_path, overwrite, headers=headers)['handle']
#         print("Putting file: " + dbfs_path)
#         for i in range(0, len(content), 2**20):
#             add_block(handle, 
#                       base64.standard_b64encode(content[i:i+2**20]).decode(), 
#                       headers=headers)
#         close(handle, headers=headers)
#         print(f"File {dbfs_path} uploaded successfully.")
#     else:
#         print(f"Error downloading file from {url}. Status code: {response.status_code}")

# # Function to upload multiple files
# def ingest(file_mappings):
#     for github_url, dbfs_path in file_mappings.items():
#         put_file_from_url(github_url, dbfs_path, True, headers)

# # Map of GitHub URLs to DBFS paths
# file_mappings = {
#     "https://github.com/nogibjj/IDS-706-Python-MySQL-XS110/blob/main/orders.csv": "/FileStore/IDS706_Data_Pipeline/orders.csv",
#     "https://github.com/nogibjj/IDS-706-Python-MySQL-XS110/blob/main/customers.csv": "/FileStore/IDS706_Data_Pipeline/customers.csv"
# }



# if __name__ == "__main__":
#     ingest(file_mappings)
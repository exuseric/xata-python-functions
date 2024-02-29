from xata.client import XataClient
import pandas as pd
import csv

xata = XataClient()

country:str = input('Which Country, (kenya, uganda, rwanda): ')
record:str = ''
# Replace "data.csv" with the actual path to your CSV file
df = pd.read_csv(f"./records/{country}_branches.csv")
data = []

# Access data by column name
for index, row in df.iterrows():
  # Access each column using its name (e.g., row["column_name"])
  sku:str = row["sku"]
  branch:str = row["branch"]
  county:str = row["county"]
  url:str = row["url"]
  data.append({
        "sku": sku,
        "branch": branch,
        "county": county,
        "url": url
    })
  
# resp = xata.records().insert("kenya_branches", {
#     "sku": sku,
#     "branch": branch,
#     "county": county,
#     "url": url
# })

def process_data_chunks(data, chunk_size, api_call_function):
  """
  This function iterates through a large dataset in chunks and makes API calls for each chunk.

  Args:
      data: The large dataset to process.
      chunk_size: The number of items per chunk.
      api_call_function: A function that takes a list of data items and performs an API call.

  Returns:
      None
  """

  for i in range(0, len(data), chunk_size):
    try:
      chunk = data[i:i + chunk_size]
      api_call_function(chunk)
      print(f"Chunk {i // chunk_size + 1} processed successfully.")
    except Exception as e:
      print(f"Error processing chunk {i // chunk_size + 1}: {e}")

def xata_api(data_chunk):
  # Replace with your actual API call logic
  try:
    resp = xata.records().bulk_insert("kenya_branches", {"records": data_chunk})
    assert resp.status_code == 201, f"Insert Successful. New resource created: {resp.status_code}"
    assert resp.status_code == 200, f"Request Successful. New resource created: {resp.status_code}"
    assert resp.is_success(), f"Bulk Insert Successful: {resp.status_code}"
  except Exception as e:
    print(f"API call failed: {e}")

process_data_chunks(data=data, chunk_size=500, api_call_function=xata_api)

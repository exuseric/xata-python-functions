from time import sleep
from xata.client import XataClient
import pandas as pd
import sys

xata = XataClient()
df = ''
inputError = False

# Large dataset
data:list = []

# Database Schema
table_schema:dict = {
  "columns": [
    {
      "name": "sku",
      "type": "string",
    },
    {
      "name": "branch",
      "type": "string",
    },
    {
      "name": "county",
      "type": "string",
    },
    {
      "name": "url",
      "type": "string",
    }
  ]
}

def process_data_chunks(data, chunk_size, api_call_function, country):
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
      api_call_function(chunk, country=country)
      print(f"Chunk {i // chunk_size + 1} processed successfully.")
    except Exception as e:
      print(f"Error processing chunk {i // chunk_size + 1}: {e}")

def xata_api(data_chunk, country):
  
  # API call logic
  try:
    resp = xata.records().bulk_insert(f"{country}_branches", {"records": data_chunk})
    # assert resp.status_code == 201, f"Insert Successful. New resource created: {resp.status_code}"
    assert resp.status_code == 200, f"Request Successful. New resource created: {resp.status_code}"
    assert resp.is_success(), f"Bulk Insert Successful: {resp.status_code}"
    print(resp.status_code)
  except Exception as e:
    print(f"API call failed: {e}")

# Replace with your way to get filenames
# country:str = input('Which Country, (kenya, uganda, rwanda): ')

def tableUpdate(country, data_file):
  # Delete existing table and recreate it
  xata.table().delete(f'{country}_branches')
  print(f'{country} Existing Table Deleted')

  assert xata.table().create(f'{country}_branches').is_success()
  print(f'{country} New Table Created')

  assert xata.table().set_schema(f'{country}_branches', table_schema).is_success()
  print(f'{country} Table Schema Updated')

  # Access data by column name
  for index, row in data_file.iterrows():
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
    
  # sleep(10)
  process_data_chunks(data=data, chunk_size=1000, api_call_function=xata_api, country=country)

def main(arg):
  match arg:
    case 'ke':
      # Read the records csv
      df = pd.read_csv(f"./records/kenya_branches.csv")
      tableUpdate(country='kenya', data_file=df)
    case 'ug':
      df = pd.read_csv(f"./records/uganda_branches.csv")
      tableUpdate(country='uganda', data_file=df)
    case 'rw':
      df = pd.read_csv(f"./records/rwanda_branches.csv")
      tableUpdate(country='rwanda', data_file=df)
    case _:
      inputError = True
      print('Must be ke, ug or rw')
      
if len(sys.argv) > 1:
   main(arg=sys.argv[1])
else:
  user_input = input('Which Country, (ke, ug, rw): ')
  main(arg=user_input)
  
# match input('Which Country, (ke, ug, rw): '):
#   case 'ke':
#     # Read the records csv
#     df = pd.read_csv(f"./records/kenya_branches.csv")
#     tableUpdate(country='kenya')
#   case 'ug':
#     df = pd.read_csv(f"./records/uganda_branches.csv")
#     tableUpdate(country='uganda')
#   case 'rw':
#     df = pd.read_csv(f"./records/rwanda_branches.csv")
#     tableUpdate(country='rwanda')
#   case _:
#     inputError = True
#     print('Must be ke, ug or rw')
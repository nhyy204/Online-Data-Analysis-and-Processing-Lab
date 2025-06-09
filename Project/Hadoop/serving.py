import pandas as pd
import requests, os, json
from hdfs import InsecureClient
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from urllib.parse import urlparse

# Load environment variables from a .env file
load_dotenv(find_dotenv())

HDFS_NAMENODE = os.getenv('HDFS_NAMENODE')
HDFS_PATH = os.getenv('HDFS_PATH')
HDFS_USER = os.getenv('HDFS_USER')
POWERBI_URL = os.getenv('POWERBI_URL')
LAST_PUSH_TIMESTAMP_PATH = os.getenv('LAST_PUSH_TIMESTAMP_PATH')

def create_hdfs_client(hdfs_namenode, user):
    client = InsecureClient(hdfs_namenode, user=user)
    try:
        client.status('/')
    except Exception:
        raise ConnectionError(f"Could not connect to HDFS at {hdfs_namenode}")
    return client

def get_hdfs_path(hdfs_path_url):
    parsed = urlparse(hdfs_path_url)
    return parsed.path

def load_lastest_data_from_hdfs(hdfs_namenode, hdfs_user, hdfs_path, last_push_timestamp):
    try:
        client = create_hdfs_client(hdfs_namenode, hdfs_user)

        files = client.list(get_hdfs_path(hdfs_path))
        csv_files = [f for f in files if (f.endswith('.csv'))]

        if not csv_files:
            raise Exception("Not exists csv files in HDFS path")
        
        lasted_data = pd.DataFrame()
        hdfs_dir_path = get_hdfs_path(hdfs_path)

        for file in csv_files:
            file_path = hdfs_dir_path.rstrip('/') + '/' + file

            with client.read(file_path, encoding='utf-8') as reader:
                data = reader.read()
            
            df = pd.read_csv(StringIO(data), parse_dates=['Timestamp'])

            if last_push_timestamp is None:
                lasted_data = pd.concat([lasted_data, df], ignore_index=True)
            else:
                filtered_data = df[df['Timestamp'] > last_push_timestamp]
                lasted_data = pd.concat([lasted_data, filtered_data], ignore_index=True)

        return lasted_data
    except FileNotFoundError as ex:
        raise FileNotFoundError(f"Error reading from HDFS: {ex}")
    
    except Exception as ex:
        raise Exception(f"Error: {ex}")
    
def load_last_push_timestamp(file_path):
    try:
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r') as file:
            timestamp_str = file.read().strip()

        if not timestamp_str:
            return None
        
        return datetime.strptime(timestamp_str, '%d-%m-%Y %H:%M')
    except ValueError:
            raise ValueError(f"Invalid timestamp format in {file_path}. Expected format: 'DD-MM-YYYY HH:MM'")

    except Exception as ex:
        raise Exception(f"Error: {ex}")
    
def save_last_push_timestamp(file_path, df):
    try:
        if df.empty or 'Timestamp' not in df.columns:
            return 
        
        last_timestamp = df['Timestamp'].max().strftime("%d-%m-%Y %H:%M")

        with open(file_path, 'w') as file:
            file.write(last_timestamp)
    except Exception as ex:
        raise Exception(f"Error: {ex}")

def push_data_to_powerBI(data, powerBI_url, batch_size=10):
    try:
        if data.empty:
            return
        
        if 'Timestamp' in data.columns:
            data = data.drop(columns='Timestamp')

        data = data.fillna('')
        data = data.infer_objects(copy=False)
        records = data.to_dict(orient='records')

        header = {
            'Content-Type': 'application/json',
        }

        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            payload = {
                "rows": batch
            }

            response = requests.post(powerBI_url, json=payload, headers=header)

            if response.status_code != 200:
                raise Exception(f"Failed to push data to Power BI: {response.text}")
            
            print(f"Pushed batch {i // batch_size + 1} of {len(records) // batch_size + 1} to Power BI. Records: {len(batch)}")

    except Exception as ex:
        raise Exception(f"Error: {ex}")

    
def main():
    try:
        last_push_timestamp = load_last_push_timestamp(LAST_PUSH_TIMESTAMP_PATH)
        lasted_data = load_lastest_data_from_hdfs(HDFS_NAMENODE, HDFS_USER, HDFS_PATH, last_push_timestamp)
        if not lasted_data.empty:
            push_data_to_powerBI(lasted_data, POWERBI_URL)
            save_last_push_timestamp(LAST_PUSH_TIMESTAMP_PATH, lasted_data)
        else:
            print("No data to push")
    except Exception as ex:
        raise Exception(f"Error: {ex}")
    
if __name__ == "__main__":
    main()
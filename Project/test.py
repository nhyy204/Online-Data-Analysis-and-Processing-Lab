import os, subprocess
from dotenv import load_dotenv, find_dotenv
# Load environment variables from a .env file
load_dotenv(find_dotenv())

serving_path = os.getenv('SERVING_SCRIPT')
print(serving_path)

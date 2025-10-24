import os
import logging
from rich import print

from amp.client import Client

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_url = os.getenv('AMP_SERVER_URL')
if server_url:
    logger.info(f"Using AMP_SERVER_URL from environment: {server_url}")
else:
    server_url = 'grpc://127.0.0.1:80'
    logger.warning(f"AMP_SERVER_URL not found in environment variables. Falling back to localhost: {server_url}")

client = Client(server_url)

df = client.get_sql('SELECT * FROM arb_firehose.logs LIMIT 10', read_all=True).to_pandas()
print(df)
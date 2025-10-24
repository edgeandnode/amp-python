import os
import logging
from typing import List, Optional

import typer
from rich import print

from amp.client import Client

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def query(table: str, columns: Optional[List[str]] = None, where: Optional[List[str]] = None, limit: Optional[int] = 1, decode_bytes: bool = typer.Option(False, "--decode-bytes", help="Automatically decode bytes columns to hex strings")):
    server_url = os.getenv('AMP_SERVER_URL')
    if server_url:
        logger.info(f"Using AMP_SERVER_URL from environment: {server_url}")
    else:
        server_url = 'grpc://127.0.0.1:80'
        logger.warning(f"AMP_SERVER_URL not found in environment variables. Falling back to localhost: {server_url}")
    
    client = Client(server_url)
    
    print('send ze query :rocket:')
    columns_query = '*' if not columns else columns
    where_query = '' if not where else f'where {where}'
    limit_query = 'limit 1' if not limit else f'limit {limit}'
    query_body = f'select {columns_query} from {table} {where_query} {limit_query}'
    print(f'... {query_body}\n')

    df = client.get_sql(query_body, read_all=True).to_pandas()
    # Optional: decode bytes columns to hex strings
    if decode_bytes:
        print('decoding bytes columns to hex strings')
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, bytes)).any():
                df[col] = df[col].apply(lambda x: '0x' + x.hex() if isinstance(x, bytes) else x)
                
    print('results:')
    print(df)

if __name__ == '__main__':
    app()
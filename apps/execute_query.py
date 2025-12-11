from rich import print

from amp.client import Client

# Replace with your remote server URL
# Format: grpc://hostname:port or grpc+tls://hostname:port for TLS
SERVER_URL = "grpc://34.27.238.174:80"

# Option 1: No authentication (if server doesn't require it)
# client = Client(url=SERVER_URL)

# Option 2: Use explicit auth token
# client = Client(url=SERVER_URL, auth_token='your-token-here')

# Option 3: Use environment variable AMP_AUTH_TOKEN
# export AMP_AUTH_TOKEN="your-token-here"
# client = Client(url=SERVER_URL)

# Option 4: Use auto-refreshing auth from shared auth file (recommended)
# Uses ~/.amp/cache/amp_cli_auth (shared with TypeScript CLI)
client = Client(url=SERVER_URL, auth=True)

df = client.get_sql('select * from eth_firehose.logs limit 1', read_all=True).to_pandas()
print(df)

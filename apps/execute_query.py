from rich import print

from amp.client import Client

client = Client('grpc://127.0.0.1:80')

df = client.get_sql('select * from eth_firehose.logs limit 1', read_all=True).to_pandas()
print(df)

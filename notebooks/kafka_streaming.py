import marimo

__generated_with = '0.17.0'
app = marimo.App(width='medium')


@app.cell
def _():
    import marimo as mo

    from amp.client import Client

    return Client, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Kafka Streaming Example

    This notebook demonstrates continuous streaming from Flight SQL to Kafka with reorg detection.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup""")
    return


@app.cell
def _(Client):
    client = Client('grpc://127.0.0.1:1602')
    return (client,)


@app.cell
def _(client):
    client.configure_connection(
        'my_kafka',
        'kafka',
        {'bootstrap_servers': 'localhost:9092', 'client_id': 'amp-streaming-client', 'key_field': 'block_num'},
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Streaming Query

    This query uses `SETTINGS stream = true` to continuously stream new blocks as they arrive.
    The loader will automatically handle blockchain reorganizations.
    """
    )
    return


@app.cell
def _(client):
    streaming_results = client.sql(
        """
        SELECT
            block_num,
            log_index
        FROM anvil.logs
        """
    ).load(
        'my_kafka',
        'eth_logs_stream',
        stream=True,
        create_table=True,
    )
    return (streaming_results,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Monitor Stream

    This cell will continuously print results as they arrive. It starts a Kafka consumer to print
    the results as they come in.
    """
    )
    return


@app.cell
def _(streaming_results):
    import json
    import threading

    from kafka import KafkaConsumer

    def consume_kafka():
        consumer = KafkaConsumer(
            'eth_logs_stream',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        print('Kafka Consumer started')
        for message in consumer:
            print(f'Consumed: {message.value}')

    consumer_thread = threading.Thread(target=consume_kafka, daemon=True)
    consumer_thread.start()

    print('Kafka Producer started')
    for result in streaming_results:
        print(f'Produced: {result}')
    return


if __name__ == '__main__':
    app.run()

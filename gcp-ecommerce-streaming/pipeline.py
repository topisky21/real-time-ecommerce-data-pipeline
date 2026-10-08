import argparse
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)


class ParseOrder(beam.DoFn):

    def process(self, message):

        try:
            order = json.loads(message.decode("utf-8"))

            # Add Dataflow ingestion timestamp
            order["ingestion_timestamp"] = (
                datetime.now(timezone.utc).isoformat()
            )

            yield order

        except Exception as error:

            print(f"Invalid JSON message: {error}")


def run():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription path"
    )

    parser.add_argument(
        "--raw_table",
        required=True,
        help="BigQuery raw table"
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
    )

    pipeline_options.view_as(
        StandardOptions
    ).streaming = True

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        orders = (
            pipeline

            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )

            | "Parse JSON"
            >> beam.ParDo(ParseOrder())
        )

        (
            orders

            | "Write Raw Orders to BigQuery"
            >> beam.io.WriteToBigQuery(
                known_args.raw_table,

                schema={
                    "fields": [
                        {
                            "name": "order_id",
                            "type": "STRING",
                            "mode": "REQUIRED",
                        },
                        {
                            "name": "customer_id",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "product_id",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "product_name",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "category",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "quantity",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "unit_price",
                            "type": "FLOAT",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "total_amount",
                            "type": "FLOAT",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "order_timestamp",
                            "type": "TIMESTAMP",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "country",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "ingestion_timestamp",
                            "type": "TIMESTAMP",
                            "mode": "NULLABLE",
                        },
                    ]
                },

                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                ),

                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
            )
        )


if __name__ == "__main__":
    run()
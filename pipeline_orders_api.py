import dlt
import requests

@dlt.resource(
    name              = "orders",
    write_disposition = "append",
    primary_key       = "order_id"
)
def orders_from_api(
    updated_at = dlt.sources.incremental("order_date", initial_value="2026-01-01")
):
    url = "http://172.20.14.29:8080/orders_api.json"

    response = requests.get(url, timeout=10)
    response.raise_for_status()      # raises an error if the server returns 4xx or 5xx

    orders = response.json()         # parse JSON → list of dicts

    for order in orders:
        if order["order_date"] > updated_at.last_value:
            yield order              # only yield rows newer than the last run

pipeline = dlt.pipeline(
    pipeline_name = "orders_api_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"   # replace <yourname> with your name
)

load_info = pipeline.run(orders_from_api())
print(load_info)

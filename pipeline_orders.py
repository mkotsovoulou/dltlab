import dlt
import pandas as pd

@dlt.resource(
    name               = "orders",
    write_disposition  = "append",
    primary_key        = "order_id"
)
def orders_incremental(
    updated_at = dlt.sources.incremental("order_date", initial_value="2026-01-01")
):
    df = pd.read_csv("files/orders.csv")
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date.astype(str)

    # Filter to only rows newer than the last known value
    new_rows = df[df["order_date"] > updated_at.last_value]
    yield new_rows.to_dict(orient="records")

pipeline = dlt.pipeline(
    pipeline_name = "orders_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"
)

load_info = pipeline.run(orders_incremental())
print(load_info)

# Add a few new rows to files/orders.csv with dates after 2026-06-20 (book_id 23 = Kafka on the Shore, book_id 21 = The Plague):

# 1016,7,23,1,14.99,2026-06-22
# 1017,3,21,1,11.99,2026-06-22
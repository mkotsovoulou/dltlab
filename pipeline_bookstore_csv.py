import dlt
import pandas as pd

@dlt.resource(name="authors", write_disposition="replace", primary_key="author_id")
def authors_from_csv():
    df = pd.read_csv("files/authors.csv")
    df["born"] = df["born"].astype(int)
    yield df.to_dict(orient="records")

@dlt.resource(name="books", write_disposition="merge", primary_key="book_id")
def books_from_csv():
    df = pd.read_csv("files/books.csv")
    df["available"] = df["available"].astype(str).str.lower() == "true"
    # .astype(str)  → ensures the value is a string regardless of how pandas read it
    # .str.lower()  → lowercases it so "True", "true", "TRUE" all become "true"
    # == "true"     → compares each value and returns True or False
    df["year"]      = df["year"].astype(int)
    df["rating"]    = df["rating"].astype(float)
    yield df.to_dict(orient="records")

pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"
)

load_info = pipeline.run([authors_from_csv(), books_from_csv()])
print(load_info)
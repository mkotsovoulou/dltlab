# pipeline_authors.py
import dlt

# pipeline_authors.py
import dlt

@dlt.resource(name="authors", write_disposition="replace", primary_key="author_id")
def authors_resource():
    yield [
        {"author_id": 1, "name": "George Orwell",      "nationality": "British",  "born": 1903},
        {"author_id": 2, "name": "Fyodor Dostoevsky",  "nationality": "Russian",  "born": 1821},
        {"author_id": 3, "name": "Franz Kafka",        "nationality": "Czech",    "born": 1883},
        {"author_id": 4, "name": "Ernest Hemingway",   "nationality": "American", "born": 1899},
    ]


pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"
)

load_info = pipeline.run(authors_resource())
print(load_info)
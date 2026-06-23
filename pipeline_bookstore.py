# pipeline_bookstore.py
import dlt

@dlt.resource(name="authors", write_disposition="replace", primary_key="author_id")
def authors_resource():
    yield [
        {"author_id": 1, "name": "George Orwell",      "nationality": "British",  "born": 1903},
        {"author_id": 2, "name": "Fyodor Dostoevsky",  "nationality": "Russian",  "born": 1821},
        {"author_id": 3, "name": "Ernest Hemingway",   "nationality": "American", "born": 1899},
        {"author_id": 4, "name": "Franz Kafka",        "nationality": "Czech",    "born": 1883},
    ]

@dlt.resource(name="books", write_disposition="merge", primary_key="book_id")
def books_resource():
    yield [
        {"book_id": 1, "title": "1984",                 "author_id": 1, "genre": "Dystopian",    "year": 1949, "rating": 4.8},
        {"book_id": 2, "title": "Animal Farm",          "author_id": 1, "genre": "Satire",       "year": 1945, "rating": 4.5},
        {"book_id": 3, "title": "Crime and Punishment", "author_id": 2, "genre": "Psychological","year": 1866, "rating": 4.7},
        {"book_id": 4, "title": "The Metamorphosis",    "author_id": 4, "genre": "Absurdist",    "year": 1915, "rating": 4.5},
        {"book_id": 5, "title": "The Old Man and Sea",  "author_id": 3, "genre": "Literary",     "year": 1952, "rating": 4.3},
    ]

@dlt.source
def bookstore_source():
    return [authors_resource(), books_resource()]

pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"
)

load_info = pipeline.run(bookstore_source())
print(load_info)
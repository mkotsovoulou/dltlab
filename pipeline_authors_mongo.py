import dlt
from pymongo import MongoClient

@dlt.resource(name="authors", write_disposition="replace", primary_key="author_id")
def authors_from_mongo():
    client = MongoClient("mongodb://admin:yourpassword@172.20.14.29:27017/?authSource=admin")
    db     = client["bookstore"]

    # {"_id": 0} excludes the MongoDB ObjectId — dlt cannot infer its type
    for doc in db["authors"].find({}, {"_id": 0}):
        yield doc

    client.close()

pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"   # replace <yourname> with your name
)

load_info = pipeline.run(authors_from_mongo())
print(load_info)

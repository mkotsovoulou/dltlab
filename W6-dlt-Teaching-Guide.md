# ITC 6050 — dlt Teaching Guide

## Read this before the Week 5 Lab

> **Purpose:** This guide teaches you what dlt is, how it works, and what every concept and command does — so that when you run your first pipeline you understand why each piece exists.

---

## 1. What is dlt?

**dlt** (data load tool) is an open-source Python library that lets you build data pipelines by writing plain Python. It handles the **Extract** and **Load** steps in an ELT pipeline — pulling data from a source and depositing it into a destination database, automatically handling schema creation, type inference, and incremental loading.

It was created to solve the most painful part of data engineering: getting data reliably from A to B without writing hundreds of lines of boilerplate.

### What dlt does for you automatically

- Infers the schema from your data (column names, data types)
- Creates tables in the destination if they don't exist
- Evolves the schema when new columns appear in the source
- Tracks pipeline state so incremental loads only fetch new data
- Handles nested JSON by flattening it into relational tables
- Normalises data types across sources

### What dlt is NOT

- It does **not** transform data — that is dbt's job
- It does **not** orchestrate schedules — use Airflow or Prefect for that
- It does **not** replace your database — it loads data into one

---

## 2. Where dlt fits: the ELT stack

```
External sources
  (CSV files, APIs, databases, event streams)
        │
        │  Extract + Load  ← THIS IS WHERE dlt LIVES
        ▼
  Raw / staging layer in your database
  (shop.orders, shop.customer, raw.events …)
        │
        │  Transform  (dbt)
        ▼
  Analytics layer
  (dim_customer, fct_orders …)
        │
        │  Serve
        ▼
  BI tools / dashboards
```

dlt deposits raw data into your database exactly as it arrives. dbt then cleans and models that data into analytics-ready tables. The two tools are designed to work together.

---

## 3. Core Concepts

### Pipeline

A **pipeline** is the top-level object that connects a source to a destination. It controls where data lands and tracks state between runs.

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_<yourname>"   # schema created in Postgres
)
```

Key parameters:

| Parameter | What it does |
|---|---|
| `pipeline_name` | Unique name — dlt uses it to track state between runs |
| `destination` | Where to write data (`postgres`, `duckdb`, `bigquery`, etc.) |
| `dataset_name` | Schema/dataset name created in the destination |

---

### Source

A **source** is a function decorated with `@dlt.source` that groups one or more related resources together. Think of it as a connector to a system (e.g. "the bookstore database").

```python
@dlt.source
def bookstore_source():
    return [authors_resource(), books_resource()]
```

---

### Resource

A **resource** is a function decorated with `@dlt.resource` that yields the actual data — rows, records, or documents. It defines what data to extract and how.

```python
@dlt.resource(name="authors", write_disposition="replace", primary_key="author_id")
def authors_resource():
    authors = [
        {"author_id": 1, "name": "George Orwell",      "nationality": "British", "born": 1903},
        {"author_id": 2, "name": "Fyodor Dostoevsky",  "nationality": "Russian", "born": 1821},
        {"author_id": 3, "name": "Franz Kafka",        "nationality": "Czech",   "born": 1883},
    ]
    yield authors
```

Key resource parameters:

| Parameter | What it does |
|---|---|
| `name` | Table name in the destination |
| `write_disposition` | How to handle existing data (see Section 6) |
| `primary_key` | Column(s) that uniquely identify a row |
| `columns` | Optional explicit schema definition |

---

### Destination

A **destination** is where dlt writes the data. dlt supports many destinations out of the box:

| Destination | Install | Use case |
|---|---|---|
| `postgres` | `pip install dlt[postgres]` | Course database |
| `duckdb` | `pip install dlt[duckdb]` | Local analytics, no server needed |
| `bigquery` | `pip install dlt[bigquery]` | Google Cloud |
| `snowflake` | `pip install dlt[snowflake]` | Snowflake |
| `filesystem` | built-in | Write to CSV / Parquet files |

For this course we use **postgres**.

---

## 4. Project Setup and Version Control

Before writing any code, set up your project folder and connect it to GitHub. This ensures your work is version-controlled and your credentials are never accidentally pushed.

### Create the project folder

```bash
mkdir dltLab
cd dltLab
```

### Create the Git repository on GitHub

1. Go to [github.com](https://github.com) → **New repository**
2. Name it `dltlab`
3. Leave it **completely empty** — no README, no .gitignore, no license
4. Click **Create repository**

> Create the GitHub repo before `git init` so you can push immediately with no conflicts.

### Initialise git locally

```bash
git init
```

### Create the .gitignore file

Create a file called `.gitignore` in the root of your project with at minimum:

```
.dlt/secrets.toml
__pycache__/
*.pyc
.env
```

### Create the credentials file

```bash
mkdir .dlt
```

Create `.dlt/secrets.toml` with your database credentials (see Section 4a below). Because `.dlt/secrets.toml` is in `.gitignore`, git will never track it.

### First commit and push to GitHub

```bash
git add .
git status          # verify secrets.toml does NOT appear here
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/dltlab.git
git push -u origin main
```

Your project is now on GitHub with no credentials exposed.

---

## 4a. Installation and Setup

```bash
pip3 install "dlt[postgres]"
```

> **Note — zsh users:** wrap the package name in quotes. zsh interprets square brackets as glob patterns and will throw `zsh: no matches found` without them.

Verify:

```bash
python3 -c "import dlt; print(dlt.__version__)"
```

### Credentials

dlt reads database credentials from environment variables or a `secrets.toml` file. Never hard-code credentials in your pipeline script.

**Option A — environment variables:**

```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://admin:secret@172.20.14.29:5432/shop_lab"
```

**Option B — secrets.toml (recommended):**

Create `.dlt/secrets.toml` in your project folder:

```
[destination.postgres.credentials]
host     = "172.20.14.29"
port     = 5432
database = "shop_lab"
username = "admin"
password = "your_password"
```

> **Never commit `.dlt/secrets.toml` to git.** Add it to `.gitignore`.

### Shared classroom database — use your own schema

Everyone in this course connects to the same Postgres server. To avoid overwriting each other's data, **each student must use their own `dataset_name`** by appending their first name (or student ID):

```python
pipeline = dlt.pipeline(
    pipeline_name = "bookstore_pipeline",
    destination   = "postgres",
    dataset_name  = "raw_bookstore_mk"   # replace 'mk' with your own name
)
```

This creates a separate schema for each one of you (`raw_bookstore_mk`, `raw_bookstore_papadopoulos`, etc.). Everyone can work independently on the same server without interfering with each other.

> **Convention for this course:** `raw_bookstore_<yourname>` for dlt, `dbt_dev_<yourname>` for dbt. Use the same suffix in both labs so your dbt sources point to your own dlt schema.

---

## 5. Pipeline Progression

This lab builds up in four steps. Each script teaches one new concept and leaves the database in a state that the next script builds on. Run them in order.

```
Step 1 — pipeline_authors.py      → 4 authors loaded       (replace)
Step 2 — pipeline_bookstore.py    → 4 authors + 5 books    (replace + merge)
Step 3 — pipeline_bookstore_csv.py → 15 authors + 35 books (replace + merge + schema evolution)
Step 4 — pipeline_orders.py       → 17 orders              (incremental append)
```

---

## 6. Write Dispositions

The `write_disposition` parameter controls what happens to existing data when the pipeline runs again.

### `replace`

Drops and recreates the table on every run. Good for small reference tables that should be fully refreshed.

```python
@dlt.resource(name="authors", write_disposition="replace")
```

### `append`

Adds new rows without removing existing ones. Good for event logs or immutable records.

```python
@dlt.resource(name="order_events", write_disposition="append")
```

### `merge`

Updates existing rows and inserts new ones based on a `primary_key`. This is an upsert — the most powerful disposition for keeping tables in sync with a source.

`merge` **requires a `primary_key`** so dlt knows which column to match on. Without it, dlt cannot tell whether an incoming row is new or an update to an existing one.

```python
@dlt.resource(name="books", write_disposition="merge", primary_key="book_id")
def books_resource():
    yield [
        {"book_id": 1, "title": "1984",        "rating": 4.8},
        {"book_id": 2, "title": "Animal Farm", "rating": 4.5},
    ]
```

Run the pipeline twice — the table will still have the same number of rows, not double. dlt updates the existing row if the primary key matches, or inserts it if it's new.

| Disposition | Existing rows | New rows | Use when |
|---|---|---|---|
| `replace` | Deleted | Inserted | Full refresh, small tables |
| `append` | Kept | Inserted | Event logs, immutable data |
| `merge` | Updated | Inserted | Keeping tables in sync |

---

## 7. Step 1 — Your First Pipeline (`pipeline_authors.py`)

Load 4 authors into Postgres using `write_disposition="replace"`. This is the simplest possible pipeline — one resource, hardcoded data.

```python
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
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run(authors_resource())
print(load_info)
```

```bash
python3 pipeline_authors.py
```

dlt will infer the schema, create the `raw_bookstore_<yourname>` schema and `authors` table, and load all 4 rows.

Verify:
```sql
SELECT * FROM raw_bookstore_<yourname>.authors;
-- expected: 4 rows
```

---

## 8. Step 2 — Multiple Resources (`pipeline_bookstore.py`)

Add a second resource for books and run both together using `@dlt.source`. Books use `merge` so the table is ready for the CSV pipeline in the next step.

```python
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
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run(bookstore_source())
print(load_info)
```

```bash
python3 pipeline_bookstore.py
```

Verify:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.authors;  -- expected: 4
SELECT COUNT(*) FROM raw_bookstore_<yourname>.books;    -- expected: 5
SELECT * FROM raw_bookstore_<yourname>.books;           -- no available column yet
```

---

## 9. Step 3 — Load from CSV (`pipeline_bookstore_csv.py`)

Load the full dataset from CSV files: 15 authors and 35 books. This also demonstrates **schema evolution** — the `available` column does not exist yet in `books`, and dlt adds it automatically.

```python
# pipeline_bookstore_csv.py
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
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run([authors_from_csv(), books_from_csv()])
print(load_info)
```

```bash
python3 pipeline_bookstore_csv.py
```

Verify — notice the row counts and the new `available` column:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.authors;           -- expected: 15
SELECT COUNT(*) FROM raw_bookstore_<yourname>.books;             -- expected: 35
SELECT COUNT(*), COUNT(available) FROM raw_bookstore_<yourname>.books;
-- COUNT(*) = 35, COUNT(available) = 35  ← available column was added automatically
```

> **Why this matters for dbt:** The dbt `relationships` test checks that every `author_id` in `stg_books` exists in `stg_authors`. Without running this step, only 4 authors exist and 24 books fail the test. After this step all 15 authors are loaded and all tests pass.

Verify:

```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.authors;  -- expected: 15
SELECT COUNT(*) FROM raw_bookstore_<yourname>.books;    -- expected: 35
```

### Schema evolution in action

The JSON pipeline created the `books` table with 5 rows and no `available` column. When the CSV pipeline runs with `write_disposition="merge"`, dlt detects the new column and adds it automatically:

```sql
-- After JSON pipeline: 5 rows, no available column
-- After CSV pipeline:  35 rows, available column added automatically
SELECT COUNT(*), COUNT(available) FROM raw_bookstore_<yourname>.books;
```

---

## 10. Step 4 — Incremental Loading (`pipeline_orders.py`)

Incremental loading means only fetching data that is **new since the last run**. This is critical for large tables — you don't want to reload 500K orders every hour when only 1000 new ones arrived.

dlt tracks state automatically using a cursor column (usually a timestamp or an auto-incrementing ID).

We will simulate this using `files/orders.csv`, which contains 15 orders dated between January and June 2026.

### Run 1 — Load the initial batch

On the first run we load orders up to and including May 2026 by filtering the DataFrame manually before yielding:

```python
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
    pipeline_name = "orders_pipeline",   # separate from bookstore_pipeline to keep state isolated
    destination   = "postgres",
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run(orders_incremental())
print(load_info)
```

**Run 1:** `updated_at.last_value` = `"2026-01-01"` (the `initial_value`). All 15 rows are newer than this, so all 15 are loaded.

Verify:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.orders;
-- expected: 15
```

### Run 2 — Simulate new data arriving

Append these two new rows to `files/orders.csv` (book_id 23 = Kafka on the Shore, book_id 21 = The Plague):

```
1016,7,23,1,14.99,2026-06-22
1017,3,21,1,11.99,2026-06-22
```

Run the pipeline again **without changing any code**:

```bash
python3 pipeline_orders.py
```

**Run 2:** dlt remembers that the last loaded `order_date` was `2026-06-20`. It filters the CSV and loads only the 2 new rows.

Verify:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.orders;
-- expected: 17

SELECT * FROM raw_bookstore_<yourname>.orders ORDER BY order_date DESC LIMIT 5;
```

### How dlt tracks state

| Run | `last_value` used | Rows loaded |
|-----|------------------|-------------|
| 1   | `2026-01-01` (initial) | 15 |
| 2   | `2026-06-20` (max from Run 1) | 2 |
| 3+  | max date from previous run | only new rows |

dlt stores this state in the `_dlt_pipeline_state` table in your destination schema. You never need to manage it manually.

> **Important:** `write_disposition` must be `"append"` or `"merge"` for incremental loading to work. Using `"replace"` wipes the pipeline state on every run.

---

## 11. Step 5 — Load from a Web Service (`pipeline_orders_api.py`)

So far all data came from local files. In the real world, data usually arrives from a **REST API** — an HTTP endpoint that returns JSON. This step replaces `files/orders.csv` with a JSON file hosted on a web server, keeping everything else identical.

### What changes vs the CSV version

| | CSV pipeline | API pipeline |
|---|---|---|
| Data source | `files/orders.csv` on disk | `http://172.20.14.29:8080/orders_api.json` over HTTP |
| How we read it | `pd.read_csv(...)` | `requests.get(url).json()` |
| Result | list of dicts | list of dicts |
| Incremental logic | same | same |
| dlt resource | same | same |

The data shape is identical — dlt does not care whether the rows came from a file or a network call.

### The JSON file on the server

The server at `http://172.20.14.29:8080/orders_api.json` returns the same 15 orders as `orders.csv`, in JSON format:

```json
[
  {"order_id": 1001, "customer_id": 5,  "book_id": 1,  "quantity": 1, "total": 12.99, "order_date": "2026-01-15"},
  {"order_id": 1002, "customer_id": 8,  "book_id": 2,  "quantity": 2, "total": 18.50, "order_date": "2026-01-22"},
  ...
  {"order_id": 1015, "customer_id": 1,  "book_id": 33, "quantity": 1, "total": 10.99, "order_date": "2026-06-20"}
]
```

You can verify the endpoint is live before running your pipeline:

```bash
curl http://172.20.14.29:8080/orders_api.json
```

### The pipeline

```python
# pipeline_orders_api.py
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
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run(orders_from_api())
print(load_info)
```

### What each new line does

| Line | Purpose |
|---|---|
| `requests.get(url, timeout=10)` | Makes an HTTP GET request to the URL. `timeout=10` raises an error if the server doesn't respond within 10 seconds. |
| `response.raise_for_status()` | Checks the HTTP status code. Raises `HTTPError` for 4xx/5xx responses so the pipeline fails fast with a clear error instead of silently loading bad data. |
| `response.json()` | Parses the response body as JSON and returns a Python list of dicts — exactly what dlt expects. |
| `for order in orders: if ... > updated_at.last_value` | Same incremental filter as the CSV version. The API returns all orders; we filter to only the new ones. |

### Run 1

Reset the pipeline state first (so we don't conflict with the CSV pipeline state):

```bash
rm -rf ~/.dlt/pipelines/orders_api_pipeline
```

```bash
python3 pipeline_orders_api.py
```

**Run 1:** `updated_at.last_value` = `"2026-01-01"`. All 15 orders are newer, so all 15 are loaded.

Verify:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.orders;
-- expected: 15
```

### Run 2 — Simulate new data arriving

The instructor adds two new orders to `orders_api.json` on the server. Run the pipeline again **without changing any code**:

```bash
python3 pipeline_orders_api.py
```

**Run 2:** dlt remembers the last `order_date` from Run 1. Only the 2 new orders are fetched and loaded.

Verify:
```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.orders;
-- expected: 17
```

### Why this matters

This is how most real pipelines work — the source is a REST API, not a file. The pattern is always the same:

```
HTTP GET → parse JSON → filter by cursor → yield to dlt
```

Whether the API is a bookstore, Stripe, Salesforce, or a custom microservice, the dlt resource looks the same. Only the URL and the field names change.

---

## 12. Step 6 — Load from MongoDB (`pipeline_authors_mongo.py`)

So far all sources have been flat files or HTTP endpoints. Many production systems store data in **document databases** like MongoDB. This step loads 5 new authors from a MongoDB collection, demonstrating how dlt handles a NoSQL source.

### The source data

5 new authors have been inserted into the `bookstore.authors` collection on the classroom MongoDB server:

```
author_id: 16  Haruki Murakami      Japanese   born 1949
author_id: 17  Toni Morrison        American   born 1931
author_id: 18  Chinua Achebe        Nigerian   born 1930
author_id: 19  Leo Tolstoy          Russian    born 1828
author_id: 20  Simone de Beauvoir   French     born 1908
```

You can verify they are there before running the pipeline using **MongoDB Compass**:

1. Open MongoDB Compass
2. Click **New connection**
3. Paste this connection string:
   ```
   mongodb://admin:yourpassword@172.20.14.29:27017
   ```
4. The TLS/SSL warning at the bottom is expected — this is a classroom server on a private network. Ignore it and click **Save & Connect**
5. In the left panel, expand **bookstore** → click **authors**
6. You should see 20 documents — the original 15 plus the 5 new authors

To filter only the new ones, click **Add Filter** and enter:
```json
{ "author_id": { "$gte": 16 } }
```

### What changes vs the previous sources

| | CSV | REST API | MongoDB |
|---|---|---|---|
| Source | file on disk | HTTP endpoint | document collection |
| Library | `pandas` | `requests` | `pymongo` |
| Read method | `pd.read_csv()` | `response.json()` | `collection.find()` |
| Result | list of dicts | list of dicts | cursor of dicts |
| dlt resource | same | same | same |

MongoDB documents are already Python dicts — no parsing step needed.

### One thing to handle: `_id`

MongoDB adds an `_id` field (an `ObjectId`) to every document automatically. dlt does not know how to infer a schema from `ObjectId`, so we exclude it using `{"_id": 0}` in the projection.

### The pipeline

```python
# pipeline_authors_mongo.py
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
    dataset_name  = "raw_bookstore_<yourname>"
)

load_info = pipeline.run(authors_from_mongo())
print(load_info)
```

> **Connection string explained:** `mongodb://admin:yourpassword@172.20.14.29:27017/?authSource=admin`
> - `admin:yourpassword` — credentials (provided in class)
> - `?authSource=admin` — tells MongoDB to look for the user in the `admin` database, which is where root users are stored. Without this, the connection fails with an authentication error even with correct credentials.

Install `pymongo` if you haven't already:

```bash
pip3 install pymongo
```

### Run the pipeline

```bash
python3 pipeline_authors_mongo.py
```

Because `write_disposition="replace"`, this drops the existing `authors` table and reloads it with all authors now in MongoDB — the original 15 plus the 5 new ones.

Verify:

```sql
SELECT COUNT(*) FROM raw_bookstore_<yourname>.authors;
-- expected: 20

SELECT * FROM raw_bookstore_<yourname>.authors
WHERE author_id >= 16
ORDER BY author_id;
-- expected: 5 rows (Murakami, Morrison, Achebe, Tolstoy, de Beauvoir)
```

### Why `replace` here?

The MongoDB collection is the **authoritative source** for authors — it already contains all 20. Using `replace` means the Postgres table is always a complete, clean copy of what is in MongoDB. If someone updates a nationality or birth year directly in MongoDB, the next pipeline run picks up the change automatically.

Use `merge` instead if you want to preserve dlt metadata columns (`_dlt_load_id`, `_dlt_id`) across runs, or if the collection is too large to reload in full each time.

### The full source progression

After running all six pipelines you have covered every major source type:

| Step | Script | Source type | Technique |
|---|---|---|---|
| 1 | `pipeline_authors.py` | Python list | hardcoded data |
| 2 | `pipeline_bookstore.py` | Python list | multiple resources |
| 3 | `pipeline_bookstore_csv.py` | CSV file | pandas + schema evolution |
| 4 | `pipeline_orders.py` | CSV file | incremental loading |
| 5 | `pipeline_orders_api.py` | REST API (JSON) | `requests` + incremental |
| 6 | `pipeline_authors_mongo.py` | MongoDB collection | `pymongo` + replace |

---

## 13. Schema Inference and Evolution

dlt inspects the data you yield and automatically determines:
- Column names
- Data types (`STRING`, `BIGINT`, `FLOAT`, `BOOL`, `TIMESTAMP`, etc.)
- Nullable vs required

```python
# dlt infers this schema automatically from:
{"order_id": 1001, "customer_id": 5, "order_date": "2024-06-01", "total": 149.99, "paid": True}

# Inferred schema:
# order_id    BIGINT    NOT NULL
# customer_id BIGINT    NOT NULL
# order_date  TEXT (or TIMESTAMP if it recognises the format)
# total       FLOAT
# paid        BOOL
```

### Schema evolution

If a new field appears in your data on a later run, dlt **adds the column** to the existing table automatically — without data loss. This is called schema evolution and is one of dlt's most powerful features.

```python
# Run 1: {"book_id": 1, "title": "1984", "rating": 4.8}
# Run 2: {"book_id": 1, "title": "1984", "rating": 4.8, "pages": 328}
# → dlt adds the 'pages' column to the table automatically
```

---

## 14. The dlt Pipeline State

Every time a pipeline runs, dlt records:
- Which resources ran and how many rows were loaded
- The last cursor value for incremental resources
- Any schema changes made

This state is stored as a JSON blob in `_dlt_pipeline_state` in your destination schema. You can also inspect it locally:

```bash
# Show pipeline state
dlt pipeline bookstore_pipeline info

# Show all pipelines
dlt pipeline --list

# Drop pending packages from a stuck pipeline
dlt pipeline <pipeline_name> drop-pending-packages

# Delete pipeline state entirely (full reset)
# Mac / Linux:
rm -rf ~/.dlt/pipelines/<pipeline_name>

# Windows (Command Prompt):
rmdir /s /q C:\Users\<your_username>\.dlt\pipelines\<pipeline_name>
# Or navigate to C:\Users\<your_username>\.dlt\pipelines\ in File Explorer and delete the folder manually.
```

---

## 15. Connecting dlt to dbt

The natural pattern is: **dlt loads raw data → dbt transforms it**.

```
CSV / API / database
        │
        │  dlt  (loads into raw_bookstore_<yourname> schema)
        ▼
raw_bookstore_<yourname>.authors
raw_bookstore_<yourname>.books
        │
        │  dbt  (models/staging/stg_books.sql)
        ▼
dbt_dev_<yourname>.stg_books
dbt_dev_<yourname>.fct_orders
```

In your dbt `sources.yml`, point to the schema that dlt wrote into:

```yaml
sources:
  - name: raw
    schema: raw_bookstore_<yourname>   # ← the dlt dataset_name
    tables:
      - name: books
      - name: authors
```

Then in a staging model:

```sql
SELECT * FROM {{ source('raw', 'books') }}
```

This is the full ELT pattern — dlt handles Extract & Load, dbt handles Transform.

---

## 16. Common Errors and Fixes

**`ModuleNotFoundError: No module named 'dlt'`**
Run `pip install dlt[postgres]` and make sure your virtual environment is activated.

**`ConnectionRefusedError` or `could not connect to server`**
Check your credentials in `.dlt/secrets.toml`. Verify the Postgres server is running and accessible from your machine.

**`Table already exists` errors on `replace`**
This is normal — dlt drops and recreates the table. If you see errors during the drop, another session may have an open transaction. Close other DB connections and retry.

**Schema mismatch / column type errors**
dlt inferred a type on the first run that conflicts with new data. Use explicit column definitions in `@dlt.resource(columns={...})` to override inference for sensitive columns.

**Incremental cursor not advancing**
Make sure `write_disposition="append"` is set — incremental loading does not work with `replace` (which wipes state on every run).

**Pipeline stuck with pending packages / `DatabaseUndefinedRelation` error**
A failed run leaves a pending load package behind. Every subsequent run tries to re-execute it and fails on the same missing table. Fix:

```bash
# Step 1 — drop the stuck pending package
dlt pipeline <pipeline_name> drop-pending-packages

# Step 2 — if the table was dropped in Postgres, drop it cleanly first
# (run in DBeaver)
DROP TABLE IF EXISTS raw_bookstore_<yourname>.<table_name>;

# Step 3 — re-run the pipeline
python3 pipeline_bookstore.py
```

**Pipeline state out of sync / incremental loads 0 rows**
The local state remembers the last cursor value. If you drop and recreate a table but the state still holds the old max value, dlt finds nothing new to load. Reset the state completely:

```bash
rm -rf ~/.dlt/pipelines/<pipeline_name>
```

Then re-run — the cursor resets to `initial_value` and all rows are loaded fresh.

**`column "author_id" of relation "authors" contains null values`**
The `authors` table was originally created by the JSON pipeline with `write_disposition="replace"`, which does not enforce primary key constraints. When the CSV pipeline tries to run `merge` on the same table, Postgres refuses to add the NOT NULL constraint. Fix: drop the pending package, drop the table, and re-run so dlt creates it fresh with the correct constraints.

```bash
dlt pipeline bookstore_pipeline drop-pending-packages
```

Then in DBeaver:
```sql
DROP TABLE IF EXISTS raw_bookstore_<yourname>.authors;
```

Then re-run:
```bash
python3 pipeline_bookstore_csv.py
```

**`zsh: no matches found: dlt[postgres]`**
zsh interprets square brackets as glob patterns. Always quote the package name:

```bash
pip3 install "dlt[postgres]"
```

---

*You are now ready to build your first dlt pipeline. The combination of dlt (load) + dbt (transform) is the modern ELT stack used by data teams worldwide.*

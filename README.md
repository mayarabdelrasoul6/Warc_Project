# CC-NEWS Arabic/English/French Data Pipeline

## What this project does

This project builds a dataset of Arabic, English, and French news articles
by streaming **CC-NEWS** WARC files (from Common Crawl) directly over HTTP,
extracting clean article text, detecting language and Arabic dialect with
Hugging Face models, and storing the results in a structured **PostgreSQL**
database. A separate loader then turns those stored records into
**LangChain** `Document` chunks, ready for embedding/RAG use cases.

In short:

```
Common Crawl (CC-NEWS)  →  Parse WARC  →  Extract article fields
     →  Detect language / dialect  →  Filter (ar/en/fr)
     →  Store in PostgreSQL  →  Load + chunk for LangChain
```

## Main components

- **`parsers/`** — Reads raw WARC data (either a local `.warc.gz` file or
  a live HTTP stream from Common Crawl) and yields individual HTML page
  records.
- **`extractors/`** — Turns a raw HTML record into structured fields
  (title, author, publish date, clean text, word/char counts, etc.), and
  runs the Hugging Face language + Arabic dialect detection models.
- **`db/`** — Owns the PostgreSQL schema (`websites`, `authors`, `pages`,
  `metadata`, `content`) and handles connecting to the DB and writing
  batches of records into it.
- **`utils`** — Shared helpers used across the project: text
  decoding/encoding validation, Arabic text validation, language-code
  filtering, console-friendly Arabic text rendering, memory usage
  reporting.
- **`pipeline/stream_to_db.py`** — The main production entry point. Ties
  parsing, extraction, language filtering, and DB writing together into
  one streaming run from CC-NEWS straight into PostgreSQL.
- **`loader/langchain_loader.py`** — A downstream, read-only consumer.
  Pulls finished records back out of PostgreSQL and converts them into
  LangChain `Document` objects, then splits them into chunks for
  embedding/RAG pipelines.

## Supporting / exploratory scripts

- **CC-NEWS preview script** — Same CC-NEWS streaming source as the main
  pipeline, but only prints sample records to the console (no DB writes,
  no batching). Useful for eyeballing what a given month's crawl looks
  like before running the full pipeline.
- **Local-file pipeline / benchmark script** — Works against a WARC file
  already downloaded to disk instead of streaming from Common Crawl.
  Used to benchmark different parser/extractor combinations (warcio vs.
  FastWARC, BeautifulSoup vs. selectolax) for speed and memory usage.
- **DB summary script** — Prints row counts per table and a small sample
  of joined records, mainly as a sanity check that a pipeline run
  actually landed data in PostgreSQL.

## End-to-end data flow

1. **Fetch manifest** — Get the list of CC-NEWS WARC files for a given
   year/month from Common Crawl.
2. **Stream & parse** — Each WARC file is streamed over HTTP (not
   downloaded to disk) and parsed into individual HTML page records.
3. **Extract** — Cheap, non-ML extraction pulls out title, author, dates,
   links, and clean body text from each page; pages that are too short
   are dropped immediately.
4. **Detect language & dialect** — Surviving pages are grouped into
   batches and run through Hugging Face models to detect language, and,
   for Arabic pages, dialect.
5. **Filter** — Only Arabic, English, and French pages are kept; everything
   else is dropped at this stage.
6. **Store** — Kept records are grouped into batches and written into
   PostgreSQL across the `websites`, `authors`, `pages`, `metadata`, and
   `content` tables.
7. **Load for downstream use** — The LangChain loader reads the stored
   pages back out (joining across all the tables), wraps each into a
   `Document`, and splits it into overlapping text chunks with metadata
   preserved, ready to be embedded into a vector store.

## Database schema (high level)

- `websites` — one row per domain.
- `authors` — one row per distinct author name.
- `pages` — one row per article, linked to a website and (optionally) an
  author.
- `metadata` — per-page metadata: title, published date, language,
  Arabic dialect, word/char counts, links, headings.
- `content` — per-page cleaned article text.

## Running it

```bash
# One-off run: stream a month of CC-NEWS into PostgreSQL
python pipeline/stream_to_db.py

# Check what landed in the DB
python db/db_summary.py

# Load stored articles into LangChain Documents + chunks
python langchain.py
```

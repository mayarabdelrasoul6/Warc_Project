import sys
import os
import time
import hashlib
import gzip
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from parsers.parsers import parse_warc_stream_fastwarc   # CC-NEWS: live network stream, not local file
from extractors.extractors import extract_html_fields, detect_languages_batch, detect_dialects_batch
from db.db_handler import init_db, save_batch_records, get_connection
from utils import is_target_language

# --- CC-NEWS source config 
YEAR = "2026"
MONTH = "05"
NUM_FILES = 1      # how many CC-NEWS WARC files (paths) to stream through
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
PATHS_URL = f"https://data.commoncrawl.org/crawl-data/CC-NEWS/{YEAR}/{MONTH}/warc.paths.gz"

DB_BATCH_SIZE = 500
HF_BATCH_SIZE = 32


def make_record_id(record, url, text):
    """Fallback: generate a stable id when WARC-Record-ID is missing, so
    records never collide/overwrite each other in the DB."""
    rid = record.get("record_id", "")
    if rid:
        return rid
    basis = (url or "") + (text[:200] if text else "")
    return "gen-" + hashlib.md5(basis.encode("utf-8", errors="ignore")).hexdigest()


def get_cc_news_paths():
    """Fetches and decompresses the CC-NEWS WARC paths manifest, same as
    the cc_news_preview script, and returns the first NUM_FILES full URLs."""
    print(f"Fetching CC-NEWS WARC paths manifest for {YEAR}/{MONTH}...")
    req = urllib.request.Request(PATHS_URL, headers=HEADERS)
    with urllib.request.urlopen(req) as response:
        gz_bytes = response.read()
        paths_list = gzip.decompress(gz_bytes).decode("utf-8").splitlines()

    selected_paths = paths_list[:NUM_FILES]
    return [f"https://data.commoncrawl.org/{p}" for p in selected_paths]


def flush_hf_batch(pending, db_batch):
    """Runs batched language/dialect detection, then filters to ar/en/fr
    only (via is_target_language) before moving items into db_batch.
    Returns how many items were dropped by the language filter."""
    if not pending:
        return 0

    texts = [p["clean_text"] for p in pending]
    languages = detect_languages_batch(texts)

    arabic_positions = [i for i, lang in enumerate(languages) if lang == "ar"]
    if arabic_positions:
        arabic_texts = [texts[i] for i in arabic_positions]
        dialects = detect_dialects_batch(arabic_texts)
        for pos, dialect in zip(arabic_positions, dialects):
            pending[pos]["arabic_dialect"] = dialect

    filtered_out = 0
    for item, lang in zip(pending, languages):
        item["language"] = lang
        if is_target_language(lang):
            db_batch.append(item)
        else:
            filtered_out += 1

    pending.clear()
    return filtered_out


def run_streaming_pipeline():
    init_db()

    try:
        warc_urls = get_cc_news_paths()
    except Exception as e:
        print(f"Failed to fetch CC-NEWS manifest: {e}")
        return

    print(f"Selected {len(warc_urls)} CC-NEWS WARC files for streaming.")
    print("Starting Optimized Streaming Pipeline into PostgreSQL...")
    start_time = time.perf_counter()

    conn = get_connection()

    pending_hf = []
    db_batch = []
    total_stored = 0
    skipped = 0
    skipped_lang = 0
    db_errors = 0

    try:
        for file_index, file_url in enumerate(warc_urls, start=1):
            print(f"\n[{file_index}/{len(warc_urls)}] Streaming: {file_url}")
            try:
                stream_req = urllib.request.Request(file_url, headers=HEADERS)
                with urllib.request.urlopen(stream_req) as stream_response:
                    record_iter = parse_warc_stream_fastwarc(stream_response)

                    for record in record_iter:
                        try:
                            fields = extract_html_fields(record)
                        except Exception as e:
                            print(f"\n[EXTRACT ERROR] {e}")
                            skipped += 1
                            continue

                        word_count = fields.get("word_count", 0)
                        clean_text = fields.get("clean_text", "")
                        if word_count < 80 or not clean_text:
                            skipped += 1
                            continue

                        url = record.get("url", "")

                        pending_hf.append({
                            "record_id": make_record_id(record, url, clean_text),
                            "url": url,
                            "title": fields.get("title", "N/A"),
                            "author": fields.get("author", "N/A"),
                            "published_date": fields.get("published_date", "N/A"),
                            "cleaned_text": clean_text,
                            "clean_text": clean_text,
                            "word_count": word_count,
                            "char_count": fields.get("char_count", 0),
                            "links_count": fields.get("links_count", 0),
                            "headings": fields.get("headings_sample", []),
                            "arabic_dialect": "N/A",
                        })

                        if len(pending_hf) >= HF_BATCH_SIZE:
                            skipped_lang += flush_hf_batch(pending_hf, db_batch)

                        if len(db_batch) >= DB_BATCH_SIZE:
                            try:
                                save_batch_records(db_batch, conn=conn)
                                total_stored += len(db_batch)
                            except Exception as e:
                                db_errors += 1
                                print(f"\n[DB ERROR] Failed to save batch of {len(db_batch)} records: {e}")
                            finally:
                                db_batch = []
                            print(
                                f"Stored: {total_stored:,} | Skipped: {skipped:,} | "
                                f"Skipped(lang): {skipped_lang:,} | DB errors: {db_errors}",
                                end="\r"
                            )

            except Exception as e:
                print(f"\n[STREAM ERROR] Failed streaming {file_url}: {e}")
                continue

        # Flush whatever's left after all files are done.
        skipped_lang += flush_hf_batch(pending_hf, db_batch)
        if db_batch:
            try:
                save_batch_records(db_batch, conn=conn)
                total_stored += len(db_batch)
            except Exception as e:
                db_errors += 1
                print(f"\n[DB ERROR] Failed to save final batch of {len(db_batch)} records: {e}")

    finally:
        conn.close()

    end_time = time.perf_counter()
    print(f"\nCompleted Processing!")
    print(f"Total Saved Records    : {total_stored:,}")
    print(f"Total Skipped (extract): {skipped:,}")
    print(f"Total Skipped (lang)   : {skipped_lang:,}")
    print(f"Batches Failed         : {db_errors}")
    print(f"Total Execution Time   : {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    run_streaming_pipeline()
import gzip
import urllib.request
from extractors.extractors import extract_selectolax
from parsers.parsers import parse_warc_stream_fastwarc
from utils import is_target_language, fix_arabic_console_text

YEAR = "2026"
MONTH = "05"
NUM_FILES = 1
MAX_SAMPLES_PER_LANG = 3

PATHS_URL = f"https://data.commoncrawl.org/crawl-data/CC-NEWS/{YEAR}/{MONTH}/warc.paths.gz"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def run_streaming_pipeline():
    """Streams WARC files from Common Crawl CC-NEWS and extracts article data."""
    print(f"Fetching CC-NEWS WARC paths manifest for {YEAR}/{MONTH}...")
    req = urllib.request.Request(PATHS_URL, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            gz_bytes = response.read()
            paths_list = gzip.decompress(gz_bytes).decode("utf-8").splitlines()
    except Exception as e:
        print(f"Failed to fetch CC-NEWS manifest: {e}")
        return

    selected_paths = paths_list[:NUM_FILES]
    print(f"Selected {len(selected_paths)} WARC files for streaming.\n")

    lang_sample_counts = {"ar": 0, "en": 0, "fr": 0}
    total_processed_articles = 0

    for index, path in enumerate(selected_paths, start=1):
        file_url = f"https://data.commoncrawl.org/{path}"
        print(f"\n[{index}/{NUM_FILES}] Processing Stream: {file_url}\n" + "=" * 70)

        try:
            stream_req = urllib.request.Request(file_url, headers=HEADERS)
            with urllib.request.urlopen(stream_req) as stream_response:

                for raw_record in parse_warc_stream_fastwarc(stream_response):
                    extracted = extract_selectolax(raw_record)

                    lang = extracted.get("language", "")
                    word_cnt = extracted.get("word_count", 0)

                    # Filter valid languages and enforce quality length threshold (80 words)
                    if is_target_language(lang) and word_cnt >= 80:
                        total_processed_articles += 1

                        if lang in lang_sample_counts and lang_sample_counts[lang] < MAX_SAMPLES_PER_LANG:
                            lang_sample_counts[lang] += 1

                            title = extracted.get("title", "N/A")
                            author = extracted.get("author", "N/A")
                            headings = extracted.get("headings_sample", [])
                            clean_text = extracted.get("clean_text") or ""
                            preview_text = clean_text[:150]

                            # Format Arabic strings for proper RTL console rendering
                            if lang == "ar":
                                title = fix_arabic_console_text(title)
                                author = fix_arabic_console_text(author)
                                headings = [fix_arabic_console_text(h) for h in headings]
                                preview_text = fix_arabic_console_text(preview_text)

                            print(f" SAMPLE [{lang.upper()} #{lang_sample_counts[lang]}]")
                            print(f"  • Title          : {title}")
                            print(f"  • Author         : {author}")
                            print(f"  • Pub Date       : {extracted.get('published_date', 'N/A')}")
                            print(f"  • WARC Date      : {extracted.get('warc_date', 'N/A')}")
                            print(f"  • Language       : {lang.upper()}")

                            if lang == "ar":
                                print(f"  • Arabic Dialect : {extracted.get('arabic_dialect', 'N/A')}")

                            print(f"  • Word Count     : {word_cnt} words")
                            print(f"  • Char Count     : {extracted.get('char_count', 0)} chars")
                            print(f"  • HTML Size      : {extracted.get('html_size_bytes', 0)} bytes")
                            print(f"  • Outbound Links : {extracted.get('links_count', 0)}")
                            print(f"  • Headings Sample: {headings}")
                            print(f"  • URL            : {extracted.get('url', 'N/A')}")
                            print(f"  • Text Preview   : {preview_text}...")
                            print("-" * 70)

        except Exception as e:
            print(f"Error streaming file {path}: {e}")

    print("\nPipeline Stream Finished.")
    print(f"Total Target Articles Processed: {total_processed_articles}")
    print(f"Samples Printed per Language: {lang_sample_counts}")

if __name__ == "__main__":
    run_streaming_pipeline()
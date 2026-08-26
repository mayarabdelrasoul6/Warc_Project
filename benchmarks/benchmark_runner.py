
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from parsers.parsers import parse_warc_fastwarc, parse_warc_warcio
from extractors.extractors import extract_selectolax, extract_bs4_lxml
from utils import get_rss_memory_mb, inspect_record_sample

WARC_FILE = os.path.join(BASE_DIR, "data", "raw_data_1gb.warc.gz")

def run_detailed_benchmark(benchmark_name: str, parser_fn, extractor_fn=None):
    print("\n" + "="*50)
    print(f"   {benchmark_name}")
    print("="*50)
    
    start_time = time.perf_counter()
    
    total_records = 0
    processed = 0
    errors = 0
    skipped = 0
    total_html_bytes = 0
    peak_memory_mb = get_rss_memory_mb()
    
    try:
        for record in parser_fn(WARC_FILE):
            total_records += 1
            try:
                if extractor_fn:
                    extracted = extractor_fn(record)
                    
                    
                    words = extracted["word_count"]
                    chars = extracted.get("text_length", extracted.get("char_count", 0))

                    if words < 20 and chars < 100:
                        skipped += 1
                        continue
                    
                    total_html_bytes += extracted["html_size_bytes"]
                else:
                    total_html_bytes += len(record["raw_bytes"])
                
                processed += 1
                
                if processed % 1000 == 0:
                    current_mem = get_rss_memory_mb()
                    if current_mem > peak_memory_mb:
                        peak_memory_mb = current_mem

            except UnicodeDecodeError:
                skipped += 1
            except Exception:
                errors += 1
    except Exception:
        errors += 1

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    peak_memory_mb = max(peak_memory_mb, get_rss_memory_mb())
    
    avg_html_kb = (total_html_bytes / processed / 1024) if processed > 0 else 0
    speed = (processed / elapsed) if elapsed > 0 else 0

    print(f" Total Records : {total_records}")
    print(f" Processed     : {processed}")
    print(f" Skipped       : {skipped}")
    print(f" Errors        : {errors}")
    print(f" Average HTML  : {avg_html_kb:.2f} KB")
    print(f" Peak Memory   : {peak_memory_mb:.2f} MB")
    print(f" Elapsed Time  : {elapsed:.2f} sec")
    print(f" Speed         : {speed:.2f} records/sec")
    print("="*50)
    
    return {
        "name": benchmark_name,
        "speed": speed,
        "memory": peak_memory_mb,
        "errors": errors,
        "skipped": skipped,
        "avg_html": avg_html_kb
    }

if __name__ == "__main__":
    inspect_record_sample(parse_warc_fastwarc(WARC_FILE), extractor_fn=extract_selectolax)
    
    results = []
    
    # STAGE 1: Parsers
    results.append(run_detailed_benchmark("warcio (Parser Only)", parse_warc_warcio))
    results.append(run_detailed_benchmark("FastWARC (Parser Only)", parse_warc_fastwarc))
    
    # STAGE 2: Extractors
    results.append(run_detailed_benchmark("FastWARC + BS4 (lxml)", parse_warc_fastwarc, extract_bs4_lxml))
    results.append(run_detailed_benchmark("FastWARC + Selectolax", parse_warc_fastwarc, extract_selectolax))

    # SUMMARY TABLE
    print("\n" + "="*65)
    print("                  BENCHMARK SUMMARY TABLE")
    print("="*65)
    print(f"{'Benchmark Name':<28} | {'Rec/sec':<10} | {'RAM (MB)':<9} | {'Avg Size':<8}")
    print("-" * 65)
    for r in results:
        print(f"{r['name']:<28} | {r['speed']:<10.1f} | {r['memory']:<9.1f} | {r['avg_html']:<6.1f} KB")
    print("="*65 + "\n")

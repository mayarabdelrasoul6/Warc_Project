# benchmark.py

## Overview
A standalone performance-comparison script — not part of the production pipeline. It runs the two WARC parsers (`warcio` vs `FastWARC`) and the two HTML extractors (`BeautifulSoup+lxml` vs `selectolax`) against the same local WARC file, measuring throughput (records/sec), peak memory (RSS), and average HTML size for each, then prints a summary table so you can decide which combination to use in production.

It does not:
- Write to the database
- Process CC-NEWS live streams (local file only, via `WARC_FILE`)
- Affect anything in `pipeline/stream_to_db.py` — this is a dev/benchmarking tool only

## Dependencies
- `parsers.parsers` → `parse_warc_fastwarc`, `parse_warc_warcio`
- `extractors.extractors` → `extract_selectolax`, `extract_bs4_lxml`
- `utils` → `get_rss_memory_mb`, `inspect_record_sample`
- Standard library: `sys`, `os`, `time`

## Configuration
| Constant | Meaning |
|---|---|
| `WARC_FILE` | Path to the local WARC file used for every benchmark run: `<project>/data/raw_data_1gb.warc.gz`. Same file is reused across all 4 benchmarks so results are comparable. |

## Functions

| Function | Input | Output | Description |
|---|---|---|---|
| `run_detailed_benchmark(benchmark_name, parser_fn, extractor_fn=None)` | a name for the printed report, a parser generator function (e.g. `parse_warc_fastwarc`), an optional extractor function (e.g. `extract_selectolax`) | a results dict (`name`, `speed`, `memory`, `errors`, `skipped`, `avg_html`) | Runs `parser_fn(WARC_FILE)` end-to-end, optionally passing every yielded record through `extractor_fn`. Tracks: total records seen, how many were processed vs skipped (extractor reported <20 words **and** <100 chars) vs errored (any exception), total HTML bytes (for computing average page size), and peak RSS memory sampled every 1,000 processed records. If `extractor_fn` is omitted, it only measures raw parsing speed (no HTML processing at all). Prints a formatted block of stats and returns them as a dict for the final summary table. |

`if __name__ == "__main__"` block (script entry point, not a function, but this is the actual benchmark plan):
1. `inspect_record_sample(...)` — prints 2 full sample records (via `extract_selectolax`) so you can visually sanity-check extraction quality before trusting the numbers below.
2. **Stage 1 — parser-only benchmarks**: `warcio` vs `FastWARC`, no extraction, just raw record iteration.
3. **Stage 2 — parser+extractor benchmarks**: `FastWARC + BeautifulSoup/lxml` vs `FastWARC + selectolax` (FastWARC only, since it already won Stage 1 in practice).
4. Prints a combined summary table comparing records/sec, peak RAM, and average HTML size across all 4 runs.
## Used by
Standalone script — run directly (`python benchmark.py`). Not imported by anything else in the project.
<img width="557" height="642" alt="image" src="https://github.com/user-attachments/assets/c7ee6591-569c-4744-8060-4b32a3455b2f" />
<img width="555" height="642" alt="image" src="https://github.com/user-attachments/assets/80f3963b-b20b-4851-9bae-f898f93ccd18" />


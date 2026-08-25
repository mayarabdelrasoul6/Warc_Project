import os

# FIX: the old code did `os.environ["HF_HOME"] = "D:/huggingface_cache"`.
# That is a Windows-only, and NOT a true absolute path ("D:/..." only means
# a drive root on Windows -- on Linux/mac it's just a relative folder named
# "D:"). Every time this script ran from a different working directory (or
# on a different machine/OS), transformers couldn't find the previous
# cache, so it silently re-downloaded both models from scratch. That is
# almost certainly the real reason the pipeline "takes forever to load".
#
# Fix: build a stable, absolute, cross-platform cache path once, next to
# this project, and only set HF_HOME if the user/environment hasn't
# already configured one themselves.
_DEFAULT_HF_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".hf_cache")
os.makedirs(_DEFAULT_HF_CACHE, exist_ok=True)
os.environ.setdefault("HF_HOME", _DEFAULT_HF_CACHE)

import re
import json
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
from utils import (
    decode_and_validate,
    is_valid_arabic_text
)

ARABIC_PATTERN = re.compile(r'[\u0600-\u06FF]')
LATIN_PATTERN = re.compile(r'[a-zA-Z]')

hf_lang_detector = None
arabic_dialect_detector = None

HF_BATCH_SIZE = 32  # how many texts the HF pipeline groups into one forward pass

def get_lang_detector():
    """Lazy initialization for Language Detector.
    NOTE: batch_size lets the pipeline internally chunk a list of texts
    into forward passes instead of doing one text at a time - this is
    what makes detect_languages_batch() below actually fast.
    """
    global hf_lang_detector
    if hf_lang_detector is None:
        from transformers import pipeline
        hf_lang_detector = pipeline(
            "text-classification",
            model="papluca/xlm-roberta-base-language-detection",
            top_k=1,
            truncation=True,
            max_length=128,
            batch_size=HF_BATCH_SIZE
        )
    return hf_lang_detector

def get_dialect_detector():
    """Lazy initialization for Arabic Dialect Detector. See note above."""
    global arabic_dialect_detector
    if arabic_dialect_detector is None:
        from transformers import pipeline
        arabic_dialect_detector = pipeline(
            "text-classification",
            model="CAMeL-Lab/bert-base-arabic-camelbert-mix-did",
            top_k=1,
            truncation=True,
            max_length=128,
            batch_size=HF_BATCH_SIZE
        )
    return arabic_dialect_detector

UNWANTED_TAGS = ["script", "style", "noscript", "header", "footer", "svg", "nav"]

def detect_language_hf(text: str) -> str:
    """Language detection using Hugging Face with pre-filtering."""
    if not text or len(text) < 20:
        return "unknown"

    has_arabic = bool(ARABIC_PATTERN.search(text[:300]))
    has_latin = bool(LATIN_PATTERN.search(text[:300]))

    if not (has_arabic or has_latin):
        return "ignored"

    try:
        detector = get_lang_detector()
        sample_text = text[:250]
        prediction = detector(sample_text)[0][0]['label'].lower()

        if prediction.startswith(("ar", "arabic")):
            if is_valid_arabic_text(text):
                return "ar"
            else:
                return "corrupted"
        elif prediction.startswith(("en", "english")):
            return "en"
        elif prediction.startswith(("fr", "french")):
            return "fr"
        return prediction
    except Exception:
        return "unknown"

def detect_arabic_dialect(text: str) -> str:
    """Detects Arabic dialect using Hugging Face."""
    if not text:
        return "MSA"
    try:
        detector = get_dialect_detector()
        sample_text = text[:250]
        prediction = detector(sample_text)[0][0]['label']
        return prediction
    except Exception:
        return "MSA"

def detect_languages_batch(texts: List[str]) -> List[str]:
    """
    Batched version of detect_language_hf(): runs ONE HF forward pass
    (internally chunked by HF_BATCH_SIZE) for a whole list of texts
    instead of one pipeline call per document. This is the main speedup
    for the streaming pipeline.
    """
    if not texts:
        return []

    results = ["unknown"] * len(texts)

    # Same cheap pre-filter as the single-text version: skip the model
    # entirely for text that's too short or has no Arabic/Latin script.
    run_idx = []
    samples = []
    for i, text in enumerate(texts):
        if not text or len(text) < 20:
            continue
        has_arabic = bool(ARABIC_PATTERN.search(text[:300]))
        has_latin = bool(LATIN_PATTERN.search(text[:300]))
        if not (has_arabic or has_latin):
            results[i] = "ignored"
            continue
        run_idx.append(i)
        samples.append(text[:250])

    if not samples:
        return results

    try:
        detector = get_lang_detector()
        predictions = detector(samples)
    except Exception:
        for i in run_idx:
            results[i] = "unknown"
        return results

    for pos, i in enumerate(run_idx):
        try:
            label = predictions[pos][0]['label'].lower()
        except Exception:
            results[i] = "unknown"
            continue
        if label.startswith(("ar", "arabic")):
            results[i] = "ar" if is_valid_arabic_text(texts[i]) else "corrupted"
        elif label.startswith(("en", "english")):
            results[i] = "en"
        elif label.startswith(("fr", "french")):
            results[i] = "fr"
        else:
            results[i] = label

    return results

def detect_dialects_batch(texts: List[str]) -> List[str]:
    """Batched version of detect_arabic_dialect() for a list of Arabic texts."""
    if not texts:
        return []
    try:
        detector = get_dialect_detector()
        samples = [t[:250] if t else "" for t in texts]
        predictions = detector(samples)
        return [p[0]['label'] if p else "MSA" for p in predictions]
    except Exception:
        return ["MSA"] * len(texts)

def extract_publication_date(tree: HTMLParser) -> str:
    """Extracts publication date via Meta tags, JSON-LD, or <time> tag."""
    meta_selectors = [
        ("meta[property='article:published_time']", "content"),
        ("meta[name='pubdate']", "content"),
        ("meta[name='publishdate']", "content"),
        ("meta[name='date']", "content"),
        ("meta[name='DC.date.issued']", "content"),
        ("meta[name='parsely-pub-date']", "content")
    ]

    for selector, attr in meta_selectors:
        node = tree.css_first(selector)
        if node and node.attributes.get(attr):
            date_val = node.attributes.get(attr).strip()
            if date_val:
                return date_val

    for script_node in tree.css("script[type='application/ld+json']"):
        raw = script_node.text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if isinstance(item, dict):
                date_val = item.get("datePublished") or item.get("dateCreated")
                if date_val:
                    return str(date_val).strip()

    time_node = tree.css_first("time")
    if time_node:
        datetime_val = time_node.attributes.get("datetime")
        if datetime_val:
            return datetime_val.strip()
        text_val = time_node.text(strip=True)
        if text_val:
            return text_val

    return "N/A"

def extract_date_from_text_fallback(tree: HTMLParser) -> str:
    """Fallback: Regex search over clean body text for date patterns."""
    body_text = tree.body.text() if tree.body else tree.text()
    date_match = re.search(
        r'\b(19\d\d|20\d\d)[-/.](0?[1-9]|1[0-2])[-/.](0?[1-9]|[12]\d|3[01])\b',
        body_text
    )
    if date_match:
        return date_match.group(0)
    return "N/A"

def extract_author(tree: HTMLParser) -> str:
    """Extracts author information via Meta tags or JSON-LD."""
    author_selectors = [
        ("meta[name='author']", "content"),
        ("meta[property='article:author']", "content"),
        ("meta[name='parsely-author']", "content"),
        ("meta[name='dc.creator']", "content"),
        ("meta[name='author_name']", "content")
    ]

    for selector, attr in author_selectors:
        node = tree.css_first(selector)
        if node and node.attributes.get(attr):
            val = node.attributes.get(attr).strip()
            if val:
                return val

    for script_node in tree.css("script[type='application/ld+json']"):
        raw = script_node.text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
            candidates = data if isinstance(data, list) else [data]
            for item in candidates:
                if isinstance(item, dict) and "author" in item:
                    author_data = item["author"]
                    if isinstance(author_data, dict) and "name" in author_data:
                        return str(author_data["name"]).strip()
                    elif isinstance(author_data, list) and len(author_data) > 0:
                        first_author = author_data[0]
                        if isinstance(first_author, dict):
                            return str(first_author.get("name", "N/A")).strip()
                        return str(first_author).strip()
                    elif isinstance(author_data, str):
                        return author_data.strip()
        except (json.JSONDecodeError, ValueError):
            continue

    return "N/A"

def extract_html_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 1 of extraction: pure HTML parsing/cleaning (title, author, dates,
    links, clean_text, word_count). Deliberately does NOT call any Hugging
    Face model, so it's cheap and safe to run per-record in a tight loop.
    Language/dialect detection is done separately, in batches, by
    detect_languages_batch() / detect_dialects_batch() below - see
    extract_selectolax() for the old single-record, all-in-one version
    (still used by scripts that process one record at a time, e.g. the
    CC-NEWS preview script).
    """
    html_str, status = decode_and_validate(record["raw_bytes"])

    if status != "success" or not html_str:
        return {"language": "corrupted", "word_count": 0, "clean_text": ""}

    tree = HTMLParser(html_str)

    title_node = tree.css_first("title")
    title = title_node.text(strip=True) if title_node else "N/A"

    for tag in tree.css(", ".join(UNWANTED_TAGS)):
        tag.decompose()

    body = tree.body
    raw_text = body.text(separator=' ', strip=True) if body else tree.text(separator=' ', strip=True)
    clean_text = " ".join(raw_text.split())

    word_cnt = len(clean_text.split())
    if word_cnt < 80:
        return {"language": "too_short", "word_count": word_cnt, "clean_text": ""}

    pub_date = extract_publication_date(tree)
    if pub_date == "N/A":
        pub_date = extract_date_from_text_fallback(tree)

    author = extract_author(tree)

    return {
        "title": title,
        "author": author,
        "url": record["url"],
        "warc_date": record["warc_date"],
        "published_date": pub_date,
        "clean_text": clean_text,
        "word_count": word_cnt,
        "char_count": len(clean_text),
        "html_size_bytes": len(record["raw_bytes"]),
        "links_count": len(tree.css("a")),
        "headings_sample": [node.text(strip=True) for node in tree.css("h1, h2, h3")][:3]
    }

def extract_selectolax(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extractor with quality filtering and Hugging Face language detection.
    Kept for callers that process one record at a time (no batching)."""
    html_str, status = decode_and_validate(record["raw_bytes"])

    if status != "success" or not html_str:
        return {"language": "corrupted", "word_count": 0, "clean_text": ""}

    tree = HTMLParser(html_str)

    title_node = tree.css_first("title")
    title = title_node.text(strip=True) if title_node else "N/A"

    for tag in tree.css(", ".join(UNWANTED_TAGS)):
        tag.decompose()

    body = tree.body
    raw_text = body.text(separator=' ', strip=True) if body else tree.text(separator=' ', strip=True)
    clean_text = " ".join(raw_text.split())

    word_cnt = len(clean_text.split())
    if word_cnt < 80:
        return {"language": "too_short", "word_count": word_cnt, "clean_text": ""}

    language = detect_language_hf(clean_text)

    arabic_dialect = "N/A"
    if language == "ar":
        if not is_valid_arabic_text(clean_text):
            return {"language": "corrupted", "word_count": word_cnt, "clean_text": ""}
        arabic_dialect = detect_arabic_dialect(clean_text)

    pub_date = extract_publication_date(tree)
    if pub_date == "N/A":
        pub_date = extract_date_from_text_fallback(tree)

    author = extract_author(tree)

    return {
        "title": title,
        "author": author,
        "url": record["url"],
        "warc_date": record["warc_date"],
        "published_date": pub_date,
        "clean_text": clean_text,
        "language": language,
        "arabic_dialect": arabic_dialect,
        "word_count": word_cnt,
        "char_count": len(clean_text),
        "html_size_bytes": len(record["raw_bytes"]),
        "links_count": len(tree.css("a")),
        "headings_sample": [node.text(strip=True) for node in tree.css("h1, h2, h3")][:3]
    }

def extract_bs4_lxml(record: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy/Benchmark Extractor fallback."""
    html_str, status = decode_and_validate(record["raw_bytes"])
    if not html_str:
        return {"clean_text": "", "word_count": 0, "text_length": 0}

    soup = BeautifulSoup(html_str, 'lxml')

    title = soup.title.get_text(strip=True) if soup.title else "N/A"

    for tag in soup(UNWANTED_TAGS):
        tag.decompose()

    body = soup.body
    text = body.get_text(separator=' ', strip=True) if body else soup.get_text(separator=' ', strip=True)
    clean_text = " ".join(text.split())
    headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2', 'h3'])]
    links_count = len(soup.find_all('a'))

    return {
        "title": title,
        "url": record["url"],
        "warc_date": record["warc_date"],
        "clean_text": clean_text,
        "text_length": len(clean_text),
        "word_count": len(clean_text.split()),
        "html_size_bytes": len(record["raw_bytes"]),
        "links_count": links_count,
        "headings_sample": headings[:3]
    }
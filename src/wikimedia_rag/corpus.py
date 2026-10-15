"""Pure corpus rules; no network or Databricks dependencies."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import quote, urlparse


RULES = "enwiki; namespace=0; edit/new; nonempty title; [start,end); count DESC,title ASC"


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("Use explicit UTC timestamps (Z or +00:00)")
    return parsed.astimezone(timezone.utc)


def interval(start, end):
    start, end = utc(start), utc(end)
    if start >= end:
        raise ValueError("start must be before end")
    return start, end


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid SQL identifier")
    return value


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_titles(titles, limit):
    if titles is None or titles == []:
        return []
    if (not isinstance(titles, (list, tuple)) or len(titles) != limit
            or any(not isinstance(t, str) or not t.strip() or t != t.strip() for t in titles)
            or len(set(titles)) != len(titles)):
        raise ValueError("Explicit titles must be unique nonempty strings matching article_limit")
    return sorted(titles)


def corpus_id(start, end, limit, titles=None):
    a, b = interval(start, end)
    if not 1 <= limit <= 40:
        raise ValueError("article_limit must be 1..40")
    selection = normalize_titles(titles, limit)
    suffix = "|titles=" + json.dumps(selection, ensure_ascii=False) if selection else ""
    return digest(f"{a.isoformat()}|{b.isoformat()}|{limit}|{RULES}{suffix}")[:24]


def rank_events(events, start, end):
    a, b = interval(start, end)
    counts = Counter((e["wiki"], e["title"]) for e in events
                     if e.get("wiki") == "enwiki" and e.get("namespace") == 0
                     and e.get("event_type") in ("edit", "new")
                     and isinstance(e.get("title"), str) and e["title"].strip()
                     and a <= utc(e["event_time"]) < b)
    return [{"wiki": w, "title": t, "event_count": n}
            for (w, t), n in sorted(counts.items(), key=lambda item: (-item[1], item[0][1]))]


def summary_url(title):
    return "https://en.wikipedia.org/api/rest_v1/page/summary/" + quote(title, safe="")


def parse_summary(payload, fetched_at):
    if not isinstance(payload, dict) or payload.get("type") != "standard":
        raise ValueError("missing, disambiguation or unsupported page type")
    title, page_id, extract = payload.get("title"), payload.get("pageid"), payload.get("extract")
    if not isinstance(title, str) or not title.strip() or type(page_id) is not int or page_id <= 0:
        raise ValueError("Invalid canonical title/pageid")
    if not isinstance(extract, str) or not extract.strip():
        raise ValueError("Empty extract")
    source = payload.get("content_urls", {}).get("desktop", {}).get("page", "")
    url = urlparse(source)
    if url.scheme != "https" or url.hostname != "en.wikipedia.org" or not url.path.startswith("/wiki/"):
        raise ValueError("Invalid source URL")
    description = payload.get("description", "")
    if not isinstance(description, str):
        raise ValueError("Invalid description")
    text = f"{title}\n{description}\n{extract.strip()}"
    return dict(document_id=f"enwiki:{page_id}", wiki="enwiki", page_id=page_id,
                canonical_title=title, description=description, extract=extract.strip(),
                source_url=source, revision=str(payload.get("revision", "")),
                fetched_at=fetched_at, document_type="wikipedia_summary", text=text,
                content_hash=digest(text), response_json=json.dumps(payload, ensure_ascii=False))


def cached_document(cached, now, ttl_hours, refresh):
    if refresh not in ("missing", "expired", "force") or ttl_hours <= 0:
        raise ValueError("Invalid refresh policy/TTL")
    if not cached or refresh == "force":
        return None
    if refresh == "missing" or (utc(now) - utc(cached["fetched_at"])).total_seconds() < ttl_hours * 3600:
        return cached
    return None


def chunk_document(document, corpus, start, end, size=1800):
    if size < 100:
        raise ValueError("chunk size too small")
    # Fixed character windows keep embeddings bounded; summaries are usually short.
    text = document["text"]
    return [dict(chunk_id=digest(f"{corpus}|{document['document_id']}|{pos // size}"),
                 document_id=document["document_id"], corpus_id=corpus,
                 text=text[pos:pos + size], content_hash=digest(text[pos:pos + size]),
                 source_url=document["source_url"], title=document["canonical_title"],
                 document_type=document["document_type"], fetched_at=document["fetched_at"],
                 revision=document["revision"], start_utc=start, end_utc=end)
            for pos in range(0, len(text), size)]


def generation(chunks):
    return digest(json.dumps(sorted((c["chunk_id"], c["content_hash"], c["revision"], c["source_url"],
                                   c["fetched_at"]) for c in chunks)))[:24]


def chunk_diff(old, new):
    """Reference reconciliation: unchanged rows are not rewritten."""
    before = {c["chunk_id"]: c for c in old}
    after = {c["chunk_id"]: c for c in new}
    return ([c for key, c in after.items() if before.get(key) != c], sorted(before.keys() - after.keys()))

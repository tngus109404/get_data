#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_daily_news_gdelt.py

- GDELT DOC 2.1 API로 특정 날짜/기간 뉴스 URL 리스트를 가져온 뒤
- 각 URL에서 본문/description/authors/publish_time/site_name(가능하면) 추출
- "틸다 호환 CSV"로 저장

Output columns (TILDA order):
[id,title,doc_url,all_text,authors,publish_date,meta_site_name,key_word,
 filter_status,description,named_entities,triples,article_embedding]

Notes:
- publish_date: ISO 8601 'YYYY-MM-DDTHH:MM:SS' (초까지)
  - 우선순위: (페이지 메타/JSON-LD publish time) -> (GDELT seendate) -> (fallback date T00:00:00)
- description: og:description > twitter:description > meta description > 본문 앞부분
- authors: JSON-LD author > meta author/article:author > 본문 byline(간단 패턴)
- meta_site_name: og:site_name > JSON-LD publisher name > domain fallback

Guardrails:
- --max_total_candidates: (dedupe 전) 전체 후보 URL 누적 상한 (0이면 제한 없음)
- --max_pages_per_keyword: 키워드당 ArtList 페이지 상한 (0이면 제한 없음)
- --max_runtime_sec: 전체 실행 시간 상한(초, 0이면 제한 없음)

New:
- --drop_bad_pages: 본문 파싱이 막혀서 네비/구독/로그인/쿠키/JS 문구로 채워진 페이지는 row 자체를 제외
- --strict_date: --date 모드에서 publish_date가 정확히 그 날짜인 row만 유지(기본 ON)
"""

import argparse
import csv
import html
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests


# ✅ 틸다 실제 CSV 컬럼 순서
DEFAULT_OUT_COLUMNS = [
    "id",
    "title",
    "doc_url",
    "all_text",
    "authors",
    "publish_date",
    "meta_site_name",
    "key_word",
    "filter_status",
    "description",
    "named_entities",
    "triples",
    "article_embedding",
]

# ✅ 틸다 키워드에 최대한 맞춤(phrase 검색용 쌍따옴표 포함)
DEFAULT_KEYWORDS = [
    "corn and (price or demand or supply or inventory)",
    "rice and (price or demand or supply or inventory)",
    "wheat and (price or demand or supply or inventory)",
    "soybean and (price or demand or supply or inventory)",
    "united states department of agriculture",
    "national agricultural statistics service",
    "\"soybean production\"",
    "\"soybean oil\" and (production or outputs or supplies or supply or biofuel or biodiesel or demand or price)",
    "\"soy oil\" and (production or outputs or supplies or supply or biofuel or biodiesel or demand or price)",
    "sorghum and (price or demand or supply or inventory)",
]

TRACKING_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid", "ref", "ref_src", "ref_url",
    "cexp_id", "cexp_var", "_f", "cmpid", "ito", "icid",
    "guccounter", "guce_referrer", "guce_referrer_sig",
}

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TOO_SHORT_HINT = "keyword that was too short"
STOPWORDS_FOR_REWRITE = {"of", "the", "a", "an"}
PAREN_HINT = "Parentheses may only be used around OR'd statements"


# -----------------------------
# bad-parse / blocked-page detection
# -----------------------------
BAD_HEAD_PATTERNS = [
    # hard blocks / bot checks
    "access denied",
    "request blocked",
    "forbidden",
    "error 403",
    "error 404",
    "error 502",
    "just a moment",
    "checking your browser",
    "verify you are human",
    "captcha",
    "cloudflare",

    # paywall / auth gates
    "subscribe",
    "subscription",
    "sign in",
    "log in",
    "login",
    "register",
    "create an account",
    "already a subscriber",
    "manage your subscription",

    # cookie / consent overlays
    "we use cookies",
    "cookie policy",
    "accept cookies",
    "manage cookies",
    "your privacy choices",

    # JS required pages
    "enable javascript",
    "please enable javascript",
]

BAD_HEAD_COMBO_RULES = [
    (["skip to content", "subscribe"], "nav_subscribe"),
    (["skip to content", "sign in"], "nav_signin"),
    (["skip to content", "log in"], "nav_login"),
    (["we use cookies", "accept"], "cookie_banner"),
]


# -----------------------------
# logging helpers
# -----------------------------
def now_ts() -> str:
    return time.strftime("%H:%M:%S", time.localtime())


def log(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(f"[{now_ts()}] {msg}", flush=True)


# -----------------------------
# basic helpers
# -----------------------------
def clean_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\x00", " ")
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def trunc_plain(s: str, maxlen: int) -> str:
    s = clean_text(s)
    if len(s) <= maxlen:
        return s
    return s[:maxlen].rstrip()


def trunc_with_ellipsis(s: str, maxlen: int, ellipsis: str = "...") -> str:
    s = clean_text(s)
    if len(s) <= maxlen:
        return s
    if maxlen <= len(ellipsis):
        return ellipsis[:maxlen]
    return (s[: maxlen - len(ellipsis)].rstrip() + ellipsis)


def normalize_url(u: Any) -> str:
    u = clean_text(u)
    if not u:
        return ""
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = k.lower()
            if kl.startswith("utm_") or kl in TRACKING_KEYS:
                continue
            q.append((k, v))
        query = urlencode(q, doseq=True)

        frag = ""  # drop fragment
        return urlunparse((scheme, netloc, path, p.params, query, frag))
    except Exception:
        return u


def strip_www(netloc: str) -> str:
    n = (netloc or "").lower()
    return n[4:] if n.startswith("www.") else n


def meta_site_name_from_domain_or_url(domain: str, url: str) -> str:
    d = clean_text(domain).strip()
    if d:
        d = strip_www(d.lower())
        return d[:1].upper() + d[1:]  # marketscreener.com -> Marketscreener.com

    try:
        netloc = strip_www(urlparse(url).netloc)
        if not netloc:
            return ""
        return netloc[:1].upper() + netloc[1:]
    except Exception:
        return ""


def normalize_site_name(site: str) -> str:
    """
    og:site_name 등이 'https://...' 같은 형태로 오는 경우가 있어서 정규화.
    """
    s = clean_text(site)
    if not s:
        return ""

    # URL 형태면 netloc로 축약
    if "://" in s:
        try:
            netloc = strip_www(urlparse(s).netloc)
            if netloc:
                return netloc[:1].upper() + netloc[1:]
        except Exception:
            return s

    # 도메인/경로처럼 보이면 도메인만
    if ("/" in s) and (" " not in s) and (":" not in s):
        try:
            netloc = strip_www(urlparse("https://" + s).netloc)
            if netloc:
                return netloc[:1].upper() + netloc[1:]
        except Exception:
            pass

    return s


def looks_like_blocked_or_nav_page(text: str, *, fallback_used: bool) -> Optional[str]:
    """
    파싱 결과(text)가 '본문'이 아니라
    - 구독/로그인/쿠키/봇체크/JS요구/네비게이션 덩어리로 보이는지 휴리스틱 체크.
    return: None(정상) or reason(str)
    """
    t = clean_text(text)
    if not t:
        return "empty_text"

    head = t[:650].lower()

    # combo rules: 오탐 방지(강한 신호)
    for musts, reason in BAD_HEAD_COMBO_RULES:
        if all(m in head for m in musts):
            return reason

    # single keyword patterns: fallback/짧은 본문에서만 강하게 의심
    if (fallback_used and len(t) < 1200) or (len(t) < 700):
        for p in BAD_HEAD_PATTERNS:
            if p in head:
                return f"pattern:{p}"

    # fallback인데 길이도 짧고 네비 느낌이 강하면 제외
    if fallback_used and len(t) < 800 and ("skip to content" in head or "subscribe" in head or "sign in" in head):
        return "short_fallback_nav"

    return None


# -----------------------------
# datetime helpers
# -----------------------------
def _parse_ymd(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def ymd_range_to_gdelt_range(date_from: str, date_to: str) -> Tuple[str, str]:
    d1 = _parse_ymd(date_from)
    d2 = _parse_ymd(date_to)
    if d2 < d1:
        raise ValueError(f"date_to must be >= date_from (got {date_from} ~ {date_to})")
    y1, m1, d1v = date_from.split("-")
    y2, m2, d2v = date_to.split("-")
    start = f"{y1}{m1}{d1v}000000"
    end = f"{y2}{m2}{d2v}235959"
    return start, end


def yyyymmdd_to_range(date_str: str) -> Tuple[str, str]:
    y, m, d = date_str.split("-")
    start = f"{y}{m}{d}000000"
    end = f"{y}{m}{d}235959"
    return start, end


def normalize_publish_datetime(value: str) -> str:
    """
    다양한 입력을 'YYYY-MM-DDTHH:MM:SS'로 정규화.
    - 14자리 seendate(YYYYMMDDhhmmss)
    - ISO 8601 with Z/offset
    - 'YYYY-MM-DD' only -> T00:00:00
    """
    s = clean_text(value)
    if not s:
        return ""

    # GDELT seendate: YYYYMMDDhhmmss
    if re.fullmatch(r"\d{14}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}T{s[8:10]}:{s[10:12]}:{s[12:14]}"

    # date only
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s + "T00:00:00"

    s2 = s.strip()

    # convert space to 'T' once if it looks like datetime
    if "T" not in s2 and re.search(r"\d{2}:\d{2}", s2):
        s2 = s2.replace(" ", "T", 1)

    # Z -> +00:00
    if s2.endswith("Z"):
        s2 = s2[:-1] + "+00:00"

    # remove fractional seconds (both before offset and without)
    s2 = re.sub(r"\.\d+(?=[+-]\d{2}:\d{2}$)", "", s2)
    s2 = re.sub(r"\.\d+", "", s2)

    # Try fromisoformat
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass

    # Regex fallback: YYYY-MM-DD ... HH:MM(:SS)?
    m = re.search(r"(\d{4})-(\d{2})-(\d{2}).*?(\d{2}):(\d{2})(?::(\d{2}))?", s)
    if m:
        y, mo, d, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), (m.group(6) or "00")
        try:
            dt = datetime(int(y), int(mo), int(d), int(hh), int(mm), int(ss))
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return ""

    return ""


def publish_datetime_from_seendate(seendate: str, fallback_date: str) -> str:
    """
    seendate: YYYYMMDDhhmmss -> YYYY-MM-DDTHH:MM:SS
    fallback_date: YYYY-MM-DD -> YYYY-MM-DDT00:00:00
    """
    s = clean_text(seendate)
    if re.fullmatch(r"\d{14}", s):
        return normalize_publish_datetime(s)
    fb = clean_text(fallback_date)
    if "T" in fb:
        return normalize_publish_datetime(fb)
    return normalize_publish_datetime(fb) or (fb + "T00:00:00")


# -----------------------------
# GDELT query helpers
# -----------------------------
def build_gdelt_query(keyword: str, sourcelang: str) -> str:
    k = clean_text(keyword)
    sl = clean_text(sourcelang)

    if not sl or sl.strip().lower() == "all":
        return k

    return f"{k} sourcelang:{sl}"


def rewrite_query_if_too_short(query: str) -> str:
    q = clean_text(query)
    toks = [t for t in re.split(r"\s+", q) if t]
    kept = []
    for t in toks:
        tl = t.lower()
        if len(tl) <= 2:
            continue
        if tl in STOPWORDS_FOR_REWRITE:
            continue
        kept.append(t)
    return " ".join(kept).strip()


def sanitize_query_if_paren_error(query: str) -> str:
    q = clean_text(query)
    q2 = q.replace("(", " ").replace(")", " ")
    q2 = re.sub(r"\s+", " ", q2).strip()
    return q2


def request_json_with_retries(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
    base_backoff: float,
    verbose: bool,
    print_fail: bool,
    ctx: str,
) -> Optional[Dict[str, Any]]:
    backoff = base_backoff
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
        except Exception as e:
            if print_fail:
                log(f"[{ctx}] request failed: {type(e).__name__}: {e} -> sleep {backoff:.1f}s retry ({attempt}/{max_retries})", verbose)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

        if r.status_code == 429:
            if print_fail:
                log(f"[{ctx}] HTTP 429 -> sleep {backoff:.1f}s retry ({attempt}/{max_retries})", verbose)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

        if r.status_code != 200:
            if print_fail:
                body = (r.text or "")[:200].replace("\n", " ")
                log(f"[{ctx}] HTTP {r.status_code} -> stop; body='{body}'", verbose)
            return None

        try:
            return r.json()
        except Exception:
            body_full = r.text or ""
            low = body_full.lower()

            if TOO_SHORT_HINT in low:
                if print_fail:
                    body = body_full[:200].replace("\n", " ")
                    log(f"[{ctx}] keyword too short hint detected; body='{body}'", verbose)
                return {"__KEYWORD_TOO_SHORT__": True, "__RAW_BODY__": body_full}

            if PAREN_HINT.lower() in low:
                if print_fail:
                    body = body_full[:200].replace("\n", " ")
                    log(f"[{ctx}] parentheses hint detected; body='{body}'", verbose)
                return {"__PAREN_ERROR__": True, "__RAW_BODY__": body_full}

            if print_fail:
                body = body_full[:200].replace("\n", " ")
                log(f"[{ctx}] JSON parse fail -> sleep {backoff:.1f}s retry ({attempt}/{max_retries}); body='{body}'", verbose)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
            continue

    return None


def gdelt_fetch_articles_for_keyword(
    keyword: str,
    startdt: str,
    enddt: str,
    max_records: int,  # 0=unlimited
    sleep_sec: float,
    timeout: int,
    sourcelang: str,
    verbose: bool,
    print_fail: bool,
    max_pages_per_keyword: int,  # 0=unlimited
    max_retries: int = 5,
) -> Tuple[List[Dict[str, Any]], str]:
    out: List[Dict[str, Any]] = []

    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    startrecord = 1
    per_page = 250
    pages = 0

    headers = {"User-Agent": USER_AGENT}

    original_query = build_gdelt_query(keyword, sourcelang)
    query = original_query

    target_txt = "unlimited" if max_records == 0 else str(max_records)
    log(f"[GDELT] keyword='{keyword}' start (target max={target_txt}) -> query='{query}'", verbose)

    while True:
        if max_pages_per_keyword > 0 and pages >= max_pages_per_keyword:
            if print_fail:
                log(f"[GDELT:{keyword}] hit max_pages_per_keyword={max_pages_per_keyword} -> stop", verbose)
            break

        if max_records > 0:
            remaining = max_records - len(out)
            if remaining <= 0:
                break
            this_page = min(per_page, remaining)
        else:
            this_page = per_page

        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "startdatetime": startdt,
            "enddatetime": enddt,
            "maxrecords": str(this_page),
            "startrecord": str(startrecord),
            "sort": "HybridRel",
        }

        ctx = f"GDELT:{keyword}"
        js = request_json_with_retries(
            url=base,
            params=params,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            base_backoff=1.9,
            verbose=verbose,
            print_fail=print_fail,
            ctx=ctx,
        )

        if not js:
            break

        if js.get("__KEYWORD_TOO_SHORT__"):
            rewritten = rewrite_query_if_too_short(query)
            if rewritten and rewritten != query:
                log(f"[GDELT] keyword too short -> rewrite query: '{query}' -> '{rewritten}'", verbose)
                query = rewritten
                continue
            break

        if js.get("__PAREN_ERROR__"):
            sanitized = sanitize_query_if_paren_error(query)
            if sanitized and sanitized != query:
                log(f"[GDELT] parentheses issue -> sanitize query: '{query}' -> '{sanitized}'", verbose)
                query = sanitized
                continue
            break

        arts = js.get("articles") or []
        if not arts:
            break

        for a in arts:
            out.append(
                {
                    "title": clean_text(a.get("title") or ""),
                    "url": clean_text(a.get("url") or ""),
                    "seendate": clean_text(a.get("seendate") or ""),
                    "domain": clean_text(a.get("domain") or ""),
                }
            )

        pages += 1
        startrecord += len(arts)

        if verbose:
            log(f"[GDELT] keyword='{keyword}' fetched so far: {len(out)} (pages={pages})", verbose)

        if sleep_sec > 0:
            time.sleep(sleep_sec)

        if len(arts) < this_page:
            break

    log(f"[GDELT] keyword='{keyword}' done (got={len(out)})", verbose)
    return out, query


# -----------------------------
# HTML metadata extraction
# -----------------------------
_META_TAG_RE = re.compile(r"(?is)<meta\s+[^>]*?>")
_SCRIPT_LDJSON_RE = re.compile(
    r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
)


def _parse_tag_attrs(tag: str) -> Dict[str, str]:
    # key can include ":" "-" "."
    attrs = {}
    for k, v in re.findall(r'([a-zA-Z0-9:_\.\-]+)\s*=\s*["\'](.*?)["\']', tag, flags=re.I | re.S):
        attrs[k.lower()] = html.unescape(v).strip()
    return attrs


def _collect_jsonld_objects(jsval: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(jsval, dict):
        out.append(jsval)
        # sometimes graph
        g = jsval.get("@graph")
        if isinstance(g, list):
            for x in g:
                if isinstance(x, dict):
                    out.append(x)
    elif isinstance(jsval, list):
        for x in jsval:
            if isinstance(x, dict):
                out.append(x)
    return out


def _is_article_type(t: Any) -> bool:
    if not t:
        return False
    if isinstance(t, str):
        tl = t.lower()
        return any(k in tl for k in ["newsarticle", "article", "reportagenewsarticle", "blogposting"])
    if isinstance(t, list):
        return any(_is_article_type(x) for x in t)
    return False


def _extract_author_names_from_jsonld(obj: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    author = obj.get("author")
    if not author:
        return names

    def add_author(a: Any):
        if isinstance(a, str):
            n = clean_text(a)
            if n:
                names.append(n)
        elif isinstance(a, dict):
            n = clean_text(a.get("name") or a.get("headline") or "")
            if n:
                names.append(n)
        elif isinstance(a, list):
            for xx in a:
                add_author(xx)

    add_author(author)

    # de-dup preserving order
    seen = set()
    uniq = []
    for n in names:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    return uniq


def _extract_publisher_name_from_jsonld(obj: Dict[str, Any]) -> str:
    pub = obj.get("publisher")
    if isinstance(pub, str):
        return clean_text(pub)
    if isinstance(pub, dict):
        return clean_text(pub.get("name") or "")
    if isinstance(pub, list):
        for x in pub:
            if isinstance(x, dict):
                n = clean_text(x.get("name") or "")
                if n:
                    return n
            elif isinstance(x, str):
                n = clean_text(x)
                if n:
                    return n
    return ""


def _extract_published_time_from_jsonld(obj: Dict[str, Any]) -> str:
    for k in ["datePublished", "dateCreated", "dateModified", "dateUpdated"]:
        v = obj.get(k)
        if isinstance(v, str) and clean_text(v):
            return clean_text(v)
    return ""


def _extract_byline_from_text(text: str) -> str:
    # 아주 단순 byline 패턴: 초반에서만 탐색
    head = clean_text(text)[:600]
    # e.g. "By JOSH FUNK" / "By Matt Lavietes | NBC News"
    m = re.search(r"\bBy\s+([A-Z][A-Za-z\.\-\'\s]{2,80})(?:\s*\||\s*[-–—]|,|\n|$)", head)
    if not m:
        return ""
    cand = clean_text(m.group(1))
    if len(cand) < 3:
        return ""
    return cand


def extract_text_fallback(html_text: str) -> str:
    h = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html_text)
    h = re.sub(r"(?is)<br\s*/?>", "\n", h)
    h = re.sub(r"(?is)</p\s*>", "\n", h)
    h = re.sub(r"(?is)<.*?>", " ", h)
    h = html.unescape(h)
    h = re.sub(r"[ \t\r\f\v]+", " ", h)
    h = re.sub(r"\n\s*\n+", "\n", h)
    return clean_text(h)


def extract_article(url: str, timeout: int) -> Dict[str, str]:
    """
    Returns dict:
      - text
      - description
      - authors
      - site_name
      - published_time
      - og_title (optional)
      - fallback_used ("1" or "0")
    """
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return {"text": "", "description": "", "authors": "", "site_name": "", "published_time": "", "og_title": "", "fallback_used": "1"}
        html_text = r.text or ""
    except Exception:
        return {"text": "", "description": "", "authors": "", "site_name": "", "published_time": "", "og_title": "", "fallback_used": "1"}

    meta_name = {}
    meta_prop = {}
    og_title = ""

    # meta tags
    for tag in _META_TAG_RE.findall(html_text):
        attrs = _parse_tag_attrs(tag)
        if not attrs:
            continue
        content = clean_text(attrs.get("content") or "")
        if not content:
            continue

        n = clean_text(attrs.get("name") or "").lower()
        p = clean_text(attrs.get("property") or "").lower()
        itemprop = clean_text(attrs.get("itemprop") or "").lower()

        if n:
            meta_name.setdefault(n, content)
        if p:
            meta_prop.setdefault(p, content)
        if itemprop:
            meta_name.setdefault(itemprop, content)

    # title candidates
    og_title = meta_prop.get("og:title", "") or meta_name.get("og:title", "")
    if not og_title:
        m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
        if m:
            og_title = clean_text(m.group(1))

    # description candidates
    desc = (
        meta_prop.get("og:description", "")
        or meta_name.get("twitter:description", "")
        or meta_name.get("description", "")
        or meta_name.get("og:description", "")
    )

    # site_name candidates
    site_name = (
        meta_prop.get("og:site_name", "")
        or meta_name.get("og:site_name", "")
        or meta_name.get("application-name", "")
    )

    # publish time candidates (page)
    published_time = (
        meta_prop.get("article:published_time", "")
        or meta_prop.get("og:published_time", "")
        or meta_name.get("pubdate", "")
        or meta_name.get("publish_date", "")
        or meta_name.get("publish-date", "")
        or meta_name.get("timestamp", "")
        or meta_name.get("date", "")
    )

    # authors candidates (page meta)
    meta_author = (
        meta_name.get("author", "")
        or meta_prop.get("article:author", "")
        or meta_name.get("article:author", "")
        or meta_name.get("byl", "")
    )

    # JSON-LD extraction
    jsonld_authors: List[str] = []
    jsonld_publisher = ""
    jsonld_pubtime = ""
    for block in _SCRIPT_LDJSON_RE.findall(html_text):
        raw = block.strip()
        if not raw:
            continue
        try:
            jsval = json.loads(raw)
        except Exception:
            raw2 = raw.strip().strip("<!--").strip("-->")
            try:
                jsval = json.loads(raw2)
            except Exception:
                continue

        for obj in _collect_jsonld_objects(jsval):
            if not isinstance(obj, dict):
                continue
            if not _is_article_type(obj.get("@type")):
                continue

            if not jsonld_authors:
                jsonld_authors = _extract_author_names_from_jsonld(obj)
            if not jsonld_publisher:
                jsonld_publisher = _extract_publisher_name_from_jsonld(obj)
            if not jsonld_pubtime:
                jsonld_pubtime = _extract_published_time_from_jsonld(obj)

            if jsonld_authors and jsonld_publisher and jsonld_pubtime:
                break

    # choose best authors
    authors = ""
    if jsonld_authors:
        authors = ", ".join([a for a in jsonld_authors if a])
    elif meta_author:
        authors = clean_text(meta_author)

    # byline fallback
    if not authors:
        plain = extract_text_fallback(html_text)
        authors = _extract_byline_from_text(plain)

    # choose best site_name
    if (not site_name) and jsonld_publisher:
        site_name = clean_text(jsonld_publisher)

    # choose best published_time
    if jsonld_pubtime:
        published_time = jsonld_pubtime or published_time

    # main text extraction
    text = ""
    try:
        import trafilatura  # type: ignore
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
        else:
            text = trafilatura.extract(html_text, include_comments=False, include_tables=False) or ""
        text = clean_text(text)
    except Exception:
        pass

    fallback_used = False
    if not text:
        fallback_used = True
        text = extract_text_fallback(html_text)

    return {
        "text": text,
        "description": clean_text(desc),
        "authors": clean_text(authors),
        "site_name": clean_text(site_name),
        "published_time": clean_text(published_time),
        "og_title": clean_text(og_title),
        "fallback_used": "1" if fallback_used else "0",
    }


# -----------------------------
# output row
# -----------------------------
@dataclass
class TildaRow:
    id: int
    title: str
    doc_url: str
    all_text: str
    authors: str
    publish_date: str
    meta_site_name: str
    key_word: str
    filter_status: str
    description: str
    named_entities: str
    triples: str
    article_embedding: str

    def to_dict(self) -> Dict[str, Any]:
        # ✅ None이 "None" 문자열로 저장되는 문제 방지: 항상 문자열로 저장(빈칸 허용)
        return {
            "id": self.id,
            "title": self.title or "",
            "doc_url": self.doc_url or "",
            "all_text": self.all_text or "",
            "authors": self.authors or "",
            "publish_date": self.publish_date or "",
            "meta_site_name": self.meta_site_name or "",
            "key_word": self.key_word or "",
            "filter_status": self.filter_status or "",
            "description": self.description or "",
            "named_entities": self.named_entities or "[]",
            "triples": self.triples or "[]",
            "article_embedding": self.article_embedding or "",
        }


def build_tilda_row(
    idx: int,
    keyword: str,
    title: str,
    url: str,
    domain: str,
    seendate_iso: str,
    extracted: Dict[str, str],
    all_text_maxlen: int,
    description_maxlen: int,
) -> TildaRow:
    doc_url = normalize_url(url)

    # title: GDELT title 우선, 비면 og_title
    t = clean_text(title)
    if not t:
        t = clean_text(extracted.get("og_title") or "")

    # publish_date: page published_time 우선, 없으면 seendate
    pub = normalize_publish_datetime(extracted.get("published_time") or "")
    if not pub:
        pub = normalize_publish_datetime(seendate_iso) or seendate_iso

    # meta_site_name: og:site_name/JSON-LD publisher 우선, 없으면 domain fallback
    site = normalize_site_name(extracted.get("site_name") or "")
    if not site:
        site = meta_site_name_from_domain_or_url(domain, doc_url)

    # authors
    authors = clean_text(extracted.get("authors") or "")

    # description: og/twitter/meta description 우선, 없으면 본문 앞부분
    desc_src = extracted.get("description") or extracted.get("text") or ""
    description = trunc_with_ellipsis(desc_src, description_maxlen, ellipsis="...")

    # all_text
    at_src = extracted.get("text") or ""
    if not clean_text(at_src):
        at_src = clean_text(f"{t}\n\n{description}")
    all_text = trunc_plain(at_src, all_text_maxlen)

    return TildaRow(
        id=idx,
        title=t,
        doc_url=doc_url,
        all_text=all_text,
        authors=authors,
        publish_date=pub,
        meta_site_name=site,
        key_word=keyword,
        filter_status="",        # later
        description=description,
        named_entities="[]",     # later
        triples="[]",            # later
        article_embedding="",    # later
    )


def write_csv(out_path: str, rows: List[TildaRow]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DEFAULT_OUT_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r.to_dict())


# -----------------------------
# main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", default="", help="YYYY-MM-DD (single day)")
    g.add_argument("--date_from", default="", help="YYYY-MM-DD (range start)")

    ap.add_argument("--date_to", default="", help="YYYY-MM-DD (range end). required when --date_from is used.")
    ap.add_argument("--out", required=True, help="output csv path")

    ap.add_argument("--keywords_file", default="", help="optional: one keyword per line")

    ap.add_argument("--max_per_keyword", type=int, default=300)

    ap.add_argument("--max_total_candidates", type=int, default=50000,
                    help="(guardrail) stop if total candidates (pre-dedupe) reaches this. 0=unlimited")
    ap.add_argument("--max_pages_per_keyword", type=int, default=200,
                    help="(guardrail) stop per keyword after N pages. 0=unlimited")
    ap.add_argument("--max_runtime_sec", type=int, default=0,
                    help="(guardrail) stop whole run after N seconds. 0=unlimited")

    ap.add_argument("--dedupe_by_url", type=int, default=1, help="1=dedupe by normalized url across keywords")
    ap.add_argument("--all_text_maxlen", type=int, default=215)
    ap.add_argument("--description_maxlen", type=int, default=260)
    ap.add_argument("--sleep", type=float, default=0.1, help="polite delay between GDELT pages")
    ap.add_argument("--http_timeout", type=int, default=20)

    ap.add_argument("--sourcelang", default="English",
                    help="GDELT language filter. e.g., English. Use ALL to disable filtering.")

    ap.add_argument("--workers", type=int, default=1)

    ap.add_argument("--verbose", type=int, default=0)
    ap.add_argument("--log_every", type=int, default=10, help="progress log every N articles during page fetch")
    ap.add_argument("--print_fail", type=int, default=0, help="1=print errors/retries")

    # ✅ new options
    ap.add_argument("--drop_bad_pages", type=int, default=1,
                    help="1=drop pages that look blocked/paywalled/nav-only (bad parse). 0=keep them")
    ap.add_argument("--strict_date", type=int, default=1,
                    help="1=when --date is used, keep only rows whose publish_date starts with that date. 0=disable")

    args = ap.parse_args()
    verbose = bool(int(args.verbose))
    print_fail = bool(int(args.print_fail))

    # resolve date range
    if args.date:
        date_from = args.date
        date_to = args.date
        startdt, enddt = yyyymmdd_to_range(args.date)
        fallback_date = args.date
    else:
        if not args.date_to:
            raise ValueError("--date_to is required when using --date_from")
        date_from = args.date_from
        date_to = args.date_to
        startdt, enddt = ymd_range_to_gdelt_range(date_from, date_to)
        fallback_date = date_to

    # keywords
    keywords = DEFAULT_KEYWORDS
    if args.keywords_file:
        with open(args.keywords_file, "r", encoding="utf-8") as f:
            kws = [line.strip() for line in f if line.strip()]
        if kws:
            keywords = kws

    t0 = time.time()

    def time_exceeded() -> bool:
        if int(args.max_runtime_sec) <= 0:
            return False
        return (time.time() - t0) >= int(args.max_runtime_sec)

    max_total = int(args.max_total_candidates)
    max_pages_kw = int(args.max_pages_per_keyword)
    max_per_kw = int(args.max_per_keyword)

    if args.date:
        log(f"[START] date={args.date} keywords={len(keywords)} max_per_keyword={max_per_kw} sourcelang={args.sourcelang}", verbose)
    else:
        log(f"[START] range={date_from}~{date_to} keywords={len(keywords)} max_per_keyword={max_per_kw} sourcelang={args.sourcelang}", verbose)

    log(f"[GUARD] max_total_candidates={max_total} max_pages_per_keyword={max_pages_kw} max_runtime_sec={args.max_runtime_sec}", verbose)

    # 1) fetch candidates
    candidates: List[Tuple[str, Dict[str, Any]]] = []
    rewritten_notes: List[Tuple[str, str]] = []

    for i, kw in enumerate(keywords, start=1):
        if time_exceeded():
            log("[GUARD] max_runtime_sec reached during candidate fetch -> stop", verbose)
            break

        if max_total > 0 and len(candidates) >= max_total:
            log(f"[GUARD] max_total_candidates reached (candidates={len(candidates)}) -> stop", verbose)
            break

        log(f"[1/3] ({i}/{len(keywords)}) fetching list from GDELT...", verbose)

        arts, final_query = gdelt_fetch_articles_for_keyword(
            keyword=kw,
            startdt=startdt,
            enddt=enddt,
            max_records=max_per_kw,
            sleep_sec=args.sleep,
            timeout=args.http_timeout,
            sourcelang=args.sourcelang,
            verbose=verbose,
            print_fail=print_fail,
            max_pages_per_keyword=max_pages_kw,
        )

        if final_query != build_gdelt_query(kw, args.sourcelang):
            rewritten_notes.append((build_gdelt_query(kw, args.sourcelang), final_query))

        for a in arts:
            candidates.append((kw, a))
            if max_total > 0 and len(candidates) >= max_total:
                break

        log(f"[GDELT] after keyword {i}/{len(keywords)}: candidates={len(candidates)}", verbose)

        if max_total > 0 and len(candidates) >= max_total:
            log(f"[GUARD] max_total_candidates reached (candidates={len(candidates)}) -> stop", verbose)
            break

    # 2) dedupe by normalized url (optional)
    seen = set()
    deduped: List[Tuple[str, Dict[str, Any]]] = []
    skipped_dupe = 0
    skipped_empty_url = 0

    for kw, a in candidates:
        u = normalize_url(a.get("url") or "")
        if not u:
            skipped_empty_url += 1
            continue
        if int(args.dedupe_by_url) == 1:
            if u in seen:
                skipped_dupe += 1
                continue
            seen.add(u)
        a2 = dict(a)
        a2["url"] = u
        deduped.append((kw, a2))

    log(f"[2/3] candidates={len(candidates)} -> deduped={len(deduped)} (skipped_dupe={skipped_dupe}, skipped_empty_url={skipped_empty_url})", verbose)

    # 3) fetch pages
    log(f"[3/3] fetching article pages... workers={args.workers}", verbose)

    rows_out: List[Optional[TildaRow]] = [None] * len(deduped)
    fails = 0
    dropped = 0
    done = 0
    log_every = max(1, int(args.log_every))

    def _worker(job_idx: int, kw: str, a: Dict[str, Any]) -> Tuple[int, Optional[TildaRow], Optional[str]]:
        url = a.get("url") or ""
        title = a.get("title") or ""
        domain = a.get("domain") or ""
        seendate = a.get("seendate") or ""

        seendate_iso = publish_datetime_from_seendate(seendate, fallback_date=fallback_date)

        try:
            extracted = extract_article(url, timeout=args.http_timeout)

            # ✅ blocked/nav/paywall-like page drop
            fallback_used = (clean_text(extracted.get("fallback_used") or "") == "1")
            reason = looks_like_blocked_or_nav_page(extracted.get("text") or "", fallback_used=fallback_used)
            if int(args.drop_bad_pages) == 1 and reason:
                return job_idx, None, f"DROP_BAD_PAGE:{reason}"

            row = build_tilda_row(
                idx=job_idx + 1,
                keyword=kw,
                title=title,
                url=url,
                domain=domain,
                seendate_iso=seendate_iso,
                extracted=extracted,
                all_text_maxlen=args.all_text_maxlen,
                description_maxlen=args.description_maxlen,
            )
            return job_idx, row, None
        except Exception as e:
            # 에러는 최소 row라도 남김(기존 정책 유지)
            row = build_tilda_row(
                idx=job_idx + 1,
                keyword=kw,
                title=title,
                url=url,
                domain=domain,
                seendate_iso=seendate_iso,
                extracted={"text": "", "description": "", "authors": "", "site_name": "", "published_time": "", "og_title": "", "fallback_used": "1"},
                all_text_maxlen=args.all_text_maxlen,
                description_maxlen=args.description_maxlen,
            )
            return job_idx, row, f"{type(e).__name__}: {e}"

    if args.workers <= 1:
        for j, (kw, a) in enumerate(deduped):
            if time_exceeded():
                log("[GUARD] max_runtime_sec reached during page fetch -> stop", verbose)
                break

            job_idx, row, err = _worker(j, kw, a)

            if err:
                if err.startswith("DROP_BAD_PAGE:"):
                    dropped += 1
                    if print_fail:
                        log(f"[DROP] ({job_idx+1}/{len(deduped)}) url={a.get('url','')} reason={err}", verbose)
                else:
                    fails += 1
                    if print_fail:
                        log(f"[FAIL] ({job_idx+1}/{len(deduped)}) url={a.get('url','')} err={err}", verbose)

            rows_out[job_idx] = row
            done += 1
            if verbose and done % log_every == 0:
                log(f"[PROGRESS] {done}/{len(deduped)} done (fails={fails} dropped={dropped})", verbose)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = []
            for j, (kw, a) in enumerate(deduped):
                futures.append(ex.submit(_worker, j, kw, a))

            for fut in as_completed(futures):
                if time_exceeded():
                    log("[GUARD] max_runtime_sec reached during page fetch -> stop (remaining futures will be ignored)", verbose)
                    break

                job_idx, row, err = fut.result()

                if err:
                    if err.startswith("DROP_BAD_PAGE:"):
                        dropped += 1
                        if print_fail:
                            log(f"[DROP] ({job_idx+1}/{len(deduped)}) url={deduped[job_idx][1].get('url','')} reason={err}", verbose)
                    else:
                        fails += 1
                        if print_fail:
                            log(f"[FAIL] ({job_idx+1}/{len(deduped)}) url={deduped[job_idx][1].get('url','')} err={err}", verbose)

                rows_out[job_idx] = row
                done += 1
                if verbose and done % log_every == 0:
                    log(f"[PROGRESS] {done}/{len(deduped)} done (fails={fails} dropped={dropped})", verbose)

    # id를 최종적으로 1..N 재부여(순서 안정) + None 스킵
    final_rows: List[TildaRow] = []
    for r in rows_out:
        if r is None:
            continue
        final_rows.append(r)

    # ✅ strict_date: --date 모드에서 publish_date가 해당 날짜인 것만 유지
    if args.date and int(args.strict_date) == 1:
        before = len(final_rows)
        final_rows = [r for r in final_rows if clean_text(r.publish_date).startswith(args.date)]
        after = len(final_rows)
        log(f"[STRICT_DATE] keep only {args.date}: {before} -> {after}", verbose)

    # id 재부여
    for i, r in enumerate(final_rows, start=1):
        r.id = i

    write_csv(args.out, final_rows)
    log(f"[DONE] wrote: {args.out} (rows={len(final_rows)} fails={fails} dropped={dropped})", verbose)

    if rewritten_notes:
        log("[NOTE] some keywords were auto-rewritten/sanitized by GDELT error handling:", verbose)
        for before, after in rewritten_notes:
            log(f"  - '{before}' -> '{after}'", verbose)


if __name__ == "__main__":
    main()

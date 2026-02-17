"""ad_fetcher — Internal Meta Ads Library fetcher implementation

Uses the internal Playwright-based cookie/token solution and meta_ads_library.py
to fetch ads directly from Meta's GraphQL API, bypassing RapidAPI.

The implementation fetches ads sorted by recent uploads and automatically handles:
- Cookie generation and refresh via Playwright
- Pagination with cursor tracking
- Date filtering (only returns ads newer than cutoff_timestamp)
- doc_id candidate trials and fallback on errors
"""

import os
import json
import time
import random
from typing import List, Dict, Optional

# Configuration for rate limiting and reliability
FIRST = 15  # results per page (reduced to avoid rate limits)
BASE_SLEEP = 8.0  # seconds between pages (INCREASED for aggressive rate limit protection)
JITTER = 2.0  # random jitter to avoid pattern detection
MAX_RETRIES = 3  # retry attempts on failure
BACKOFF_FACTOR = 3.0  # exponential backoff multiplier (increased)
MAX_PAGES = 20  # maximum pages to fetch per run (safety limit)
RATE_LIMIT_BACKOFF = 120  # seconds to wait after rate limit error (2 minutes)
RATE_LIMIT_CACHE_FILE = 'rate_limit_cache.json'  # Track rate limited pages
REFRESH_COOLDOWN = 300  # seconds between cookie refresh attempts (5 minutes)
POST_REFRESH_DELAY = 8  # seconds to wait after cookie refresh (let session settle)
RATE_LIMIT_COOLDOWN_HOURS = 24  # hours to wait before retrying rate-limited pages

# Global state for refresh cooldown
_last_refresh_time = 0
_rate_limit_cache = {}  # page_id -> timestamp of last rate limit


def _get_cookie_file(page_id: str) -> str:
    """Get page-specific cookie file path"""
    return f"cookies_page_{page_id}.json"


def _load_rate_limit_cache():
    """Load rate limit cache from file"""
    global _rate_limit_cache
    if os.path.exists(RATE_LIMIT_CACHE_FILE):
        try:
            with open(RATE_LIMIT_CACHE_FILE, 'r') as f:
                _rate_limit_cache = json.load(f)
        except Exception:
            _rate_limit_cache = {}
    return _rate_limit_cache

def _save_rate_limit_cache():
    """Save rate limit cache to file"""
    try:
        with open(RATE_LIMIT_CACHE_FILE, 'w') as f:
            json.dump(_rate_limit_cache, f)
    except Exception as e:
        print(f"[ad_fetcher] Failed to save rate limit cache: {e}")

def _is_rate_limited(page_id: str) -> tuple:
    """Check if page_id was recently rate limited. Returns (is_limited, remaining_hours)"""
    _load_rate_limit_cache()
    if page_id in _rate_limit_cache:
        last_limit_time = _rate_limit_cache[page_id]
        elapsed = time.time() - last_limit_time
        cooldown_seconds = RATE_LIMIT_COOLDOWN_HOURS * 3600
        if elapsed < cooldown_seconds:
            remaining_hours = (cooldown_seconds - elapsed) / 3600
            return True, remaining_hours
    return False, 0

def _mark_rate_limited(page_id: str):
    """Mark a page as rate limited"""
    _load_rate_limit_cache()
    _rate_limit_cache[page_id] = time.time()
    _save_rate_limit_cache()
    print(f"[ad_fetcher] 🚨 Page {page_id} marked as rate limited for {RATE_LIMIT_COOLDOWN_HOURS}h")


def fetch_ads_for_page(page_id: str, cutoff_timestamp: int):
    """Fetch ads for `page_id` newer than `cutoff_timestamp` using internal solution.

    Args:
        page_id: Facebook page id
        cutoff_timestamp: Unix epoch seconds (only fetch ads with start_date >= this)

    Returns:
        tuple: (ads, logs, raw_responses) where:
            - ads: List of ad dicts
            - logs: List[str] of log messages
            - raw_responses: List[dict] of raw API responses for debugging
    """
    global _last_refresh_time
    from meta_ads_recent import fetch_recent_uploads
    
    # Collect logs and raw responses to return
    logs = []
    raw_responses = []
    def log(msg):
        print(msg)
        logs.append(msg)

    log(f"[ad_fetcher] Starting fetch for page_id={page_id}, cutoff={cutoff_timestamp}")
    
    # Check if this page is currently rate limited
    is_limited, remaining_hours = _is_rate_limited(page_id)
    if is_limited:
        log(f"[ad_fetcher] ⚠️ Page {page_id} is in rate limit cooldown. {remaining_hours:.1f}h remaining.")
        log("[ad_fetcher] Skipping fetch to avoid further rate limiting.")
        return [], logs, raw_responses
    
    # PROACTIVELY refresh cookies at the start to ensure we have fresh tokens
    log(f"[ad_fetcher] Proactively refreshing cookies for page {page_id}...")
    if _refresh_cookies(page_id):
        _last_refresh_time = time.time()
        log("[ad_fetcher] Cookie refresh completed successfully")
        # Wait after refresh to let the session settle and avoid immediate rate limiting
        log(f"[ad_fetcher] Waiting {POST_REFRESH_DELAY}s for session to stabilize...")
        time.sleep(POST_REFRESH_DELAY)
    else:
        log("[ad_fetcher] WARNING: Initial cookie refresh failed, will use existing cookies")
    
    # Load cookies and doc_id (now freshly refreshed)
    cookies, current_doc_id = _load_tokens(page_id)
    
    if not cookies:
        log("[ad_fetcher] ERROR: No cookies available after refresh attempt")
        return [], logs, raw_responses

    # Fetch pages with retry logic and rate limiting
    collected = []
    cursor = None
    page = 0
    consecutive_failures = 0

    # Continue paging until we naturally run out of pages or no new items
    while True:
        page += 1
        
        # Try request with exponential backoff retry logic
        resp = None
        got_429 = False
        for retry in range(MAX_RETRIES):
            if retry > 0:
                backoff_delay = BASE_SLEEP * (BACKOFF_FACTOR ** retry) + random.uniform(0, JITTER * 2)
                log(f"[ad_fetcher] Page {page} retry {retry}/{MAX_RETRIES-1} after {backoff_delay:.2f}s")
                time.sleep(backoff_delay)
            
            try:
                resp = fetch_recent_uploads(
                    country="IN",
                    page_id=page_id,
                    cursor=cursor,
                    first=FIRST,
                    lsd=None,
                    doc_id=current_doc_id,
                    cookies=cookies
                )
                if resp:
                    # Check if response contains rate limit error in GraphQL errors
                    if 'errors' in resp:
                        for error in resp.get('errors', []):
                            error_msg = error.get('message', '')
                            error_code = error.get('code', 0)
                            if 'rate limit' in error_msg.lower() or error_code == 1675004:
                                log(f"[ad_fetcher] ⚠️ GraphQL RATE LIMIT ERROR (code {error_code}) - page {page_id} is rate limited!")
                                log(f"[ad_fetcher] Marking page as rate limited for {RATE_LIMIT_COOLDOWN_HOURS}h")
                                _mark_rate_limited(page_id)
                                log(f"[ad_fetcher] Backing off {RATE_LIMIT_BACKOFF}s before returning")
                                time.sleep(RATE_LIMIT_BACKOFF)
                                got_429 = True
                                resp = None
                                # Return immediately - no point continuing
                                log(f"[ad_fetcher] Stopping fetch - rate limited")
                                return collected, logs, raw_responses
                    
                    if resp:  # Still valid after error check
                        consecutive_failures = 0  # reset on success
                        # Store raw response for debugging
                        raw_responses.append({
                            'page': page,
                            'cursor': cursor,
                            'response': resp
                        })
                        break
                # Check if response is None due to 429 (rate limit)
                # The fetch_recent_uploads function returns None on 429
            except Exception as e:
                error_str = str(e)
                log(f"[ad_fetcher] Request exception on page {page}: {e}")
                # Detect 429 rate limit errors
                if '429' in error_str or 'rate limit' in error_str.lower() or 'too many requests' in error_str.lower():
                    log(f"[ad_fetcher] ⚠️ RATE LIMIT DETECTED - implementing {RATE_LIMIT_BACKOFF}s backoff")
                    got_429 = True
                    # Aggressive backoff on rate limit
                    time.sleep(RATE_LIMIT_BACKOFF)
                continue
        
        # Handle 429 errors with immediate cookie refresh (bypass cooldown)
        if got_429 and not resp:
            time_since_refresh = time.time() - _last_refresh_time
            if time_since_refresh >= REFRESH_COOLDOWN:
                log(f"[ad_fetcher] 429 detected - attempting emergency cookie refresh for page {page_id}...")
                if _refresh_cookies(page_id):
                    _last_refresh_time = time.time()
                    cookies, current_doc_id = _load_tokens(page_id)
                    log("[ad_fetcher] Emergency refresh succeeded, retrying request...")
                    try:
                        resp = fetch_recent_uploads(
                            country="IN",
                            page_id=page_id,
                            cursor=cursor,
                            first=FIRST,
                            lsd=None,
                            doc_id=current_doc_id,
                            cookies=cookies
                        )
                        if resp:
                            consecutive_failures = 0
                            got_429 = False
                    except Exception as e:
                        print(f"[ad_fetcher] Post-429-refresh request failed: {e}")
            else:
                print(f"[ad_fetcher] 429 detected but refresh cooldown active ({int(time_since_refresh)}s since last refresh)")

        # Handle failures with automatic cookie refresh and doc_id trials
        if not resp:
            consecutive_failures += 1
            print(f"[ad_fetcher] Page {page} failed after {MAX_RETRIES} retries (consecutive: {consecutive_failures})")
            
            # Circuit breaker: refresh cookies only after 4 consecutive failures AND cooldown period elapsed
            should_refresh = (consecutive_failures >= 4 and 
                            (time.time() - _last_refresh_time) >= REFRESH_COOLDOWN)
            
            if should_refresh:
                print(f"[ad_fetcher] Circuit breaker triggered: {consecutive_failures} consecutive failures, attempting cookie refresh for page {page_id}...")
                if _refresh_cookies(page_id):
                    _last_refresh_time = time.time()
                    # Reload tokens after refresh
                    cookies, current_doc_id = _load_tokens(page_id)
                    print("[ad_fetcher] Cookie refresh succeeded, retrying request...")
                    
                    # Retry immediately after refresh
                    try:
                        resp = fetch_recent_uploads(
                            country="IN",
                            page_id=page_id,
                            cursor=cursor,
                            first=FIRST,
                            lsd=None,
                            doc_id=current_doc_id,
                            cookies=cookies
                        )
                        if resp:
                            consecutive_failures = 0
                    except Exception as e:
                        print(f"[ad_fetcher] Post-refresh request failed: {e}")
            elif consecutive_failures >= 4:
                print(f"[ad_fetcher] Refresh cooldown active (last refresh: {int(time.time() - _last_refresh_time)}s ago)")
            
            # If still no response, try doc_id candidates
            if not resp:
                print("[ad_fetcher] Trying doc_id candidates...")
                cookie_file = _get_cookie_file(page_id)
                resp = _try_doc_id_candidates(
                    page_id=page_id,
                    cursor=cursor,
                    first=FIRST,
                    cookies=cookies,
                    cookies_file=cookie_file,
                    current_doc_id=current_doc_id
                )
                # Update current_doc_id if candidates succeeded
                if resp:
                    cookies, current_doc_id = _load_tokens(page_id)
                    print(f"[ad_fetcher] Doc ID candidates succeeded, updated to: {current_doc_id}")
            
            # Stop if still failing after all recovery attempts
            if not resp:
                if consecutive_failures >= 6:
                    print(f"[ad_fetcher] Circuit breaker: {consecutive_failures} consecutive failures, stopping")
                    break
                else:
                    print(f"[ad_fetcher] Continuing to next page despite failure...")
                    continue

        # Check for rate limit errors in response
        if "errors" in resp:
            errors = resp.get("errors", [])
            for error in errors:
                if "Rate limit" in str(error.get("message", "")):
                    print(f"[ad_fetcher] RATE LIMIT detected on page {page}: {error}")
                    print(f"[ad_fetcher] Backing off for 60 seconds...")
                    time.sleep(60)
                    break
        
        # Extract edges from response
        conn = resp.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {})
        edges = conn.get("edges", [])
        page_info = conn.get("page_info", {})

        # Debug: log response structure
        if page == 1 and not edges:
            print(f"[ad_fetcher] DEBUG: Response keys: {list(resp.keys())}")
            if "data" in resp:
                print(f"[ad_fetcher] DEBUG: data keys: {list(resp.get('data', {}).keys())}")
                if "ad_library_main" in resp.get("data", {}):
                    ad_lib = resp["data"]["ad_library_main"]
                    print(f"[ad_fetcher] DEBUG: ad_library_main keys: {list(ad_lib.keys())}")

        log(f"[ad_fetcher] Page {page}: Found {len(edges)} edges in response")

        # Process items and filter by cutoff (matching reference implementation)
        page_new_items = 0
        page_old_items = 0
        for edge_idx, edge in enumerate(edges):
            node = edge.get("node", {})
            collated = node.get("collated_results", [])
            if page == 1 and edge_idx == 0:
                log(f"[ad_fetcher] DEBUG: First edge node keys: {list(node.keys())}")
                log(f"[ad_fetcher] DEBUG: First edge has {len(collated)} collated_results")
            
            for item in collated:
                sd = item.get("start_date")
                # If no start_date, include it (can't compare)
                if sd is None:
                    transformed = _transform_item(item)
                    if transformed:
                        collected.append(transformed)
                        page_new_items += 1
                    continue
                
                # If item is newer or equal to cutoff, include it
                if sd >= cutoff_timestamp:
                    transformed = _transform_item(item)
                    if transformed:
                        collected.append(transformed)
                        page_new_items += 1
                else:
                    # This item is older than cutoff; count but don't append
                    page_old_items += 1

        log(f"[ad_fetcher] Page {page}: {page_new_items} new items, {page_old_items} old items (total: {len(collected)})")

        # Stop only when page contains NO items within cutoff window (matching reference)
        if page_new_items == 0:
            log("[ad_fetcher] Page contains no items within cutoff window, stopping pagination")
            break

        # Check pagination
        if not page_info.get("has_next_page"):
            log("[ad_fetcher] No more pages available")
            break
        cursor = page_info.get("end_cursor")
        if not cursor:
            log("[ad_fetcher] No cursor returned")
            break

        # Apply rate-limit delay with jitter between pages
        delay = BASE_SLEEP + random.uniform(0, JITTER * 2)
        time.sleep(delay)

    log(f"[ad_fetcher] Completed: {len(collected)} total ads fetched")
    return collected, logs, raw_responses


def _load_tokens(page_id: str):
    """Load cookies and doc_id from page-specific file. Returns (cookies, doc_id)."""
    cookies = None
    current_doc_id = None
    cookie_file = _get_cookie_file(page_id)
    if os.path.exists(cookie_file):
        try:
            with open(cookie_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                cookies = data.get("cookies")
                current_doc_id = data.get("doc_id")
        except Exception as e:
            print(f"[ad_fetcher] Failed to load tokens: {e}")
    return cookies, current_doc_id


def _refresh_cookies(page_id: str) -> bool:
    """Refresh cookies using Playwright for specific page_id. Returns True if successful."""
    try:
        from fetch_cookies_playwright import fetch_and_save_cookies
        cookie_file = _get_cookie_file(page_id)
        print(f"[ad_fetcher] Refreshing cookies for page {page_id} -> {cookie_file}")
        fetch_and_save_cookies(output_file=cookie_file, headless=True, page_id=page_id)
        print("[ad_fetcher] Cookie refresh completed successfully")
        return True
    except Exception as e:
        print(f"[ad_fetcher] Cookie refresh failed: {e}")
        return False


def _transform_item(item: dict) -> Optional[Dict]:
    """Transform internal item format to expected output format (matching extract_ad_item)."""
    ad_archive_id = item.get("ad_archive_id")
    page_id = item.get("page_id")

    # Extract video URL from snapshot
    video_hd_url = None
    snapshot = item.get("snapshot") or {}
    page_name = snapshot.get("page_name")

    # First check videos array (most common location)
    videos = snapshot.get("videos") or []
    for video in videos:
        if video.get("video_hd_url"):
            video_hd_url = video.get("video_hd_url")
            break
        if video.get("video_sd_url"):
            video_hd_url = video.get("video_sd_url")
            break

    # If not found in videos, check cards
    if not video_hd_url:
        cards = snapshot.get("cards") or []
        for card in cards:
            if card.get("video_hd_url"):
                video_hd_url = card.get("video_hd_url")
                break
            if card.get("video_sd_url"):
                video_hd_url = card.get("video_sd_url")
                break

    # Build ad URL
    ad_url = f"https://www.facebook.com/ads/library/?id={ad_archive_id}" if ad_archive_id else None

    start_date = item.get("start_date")
    end_date = item.get("end_date")

    # Return None if critical fields are missing
    if not all([ad_archive_id, page_id, page_name, ad_url]):
        return None

    return {
        "ad_archive_id": str(ad_archive_id),
        "page_id": str(page_id),
        "video_hd_url": video_hd_url,
        "page_name": page_name,
        "start_date": start_date,
        "end_date": end_date,
        "ad_url": ad_url,
    }


def _try_doc_id_candidates(
    page_id: str,
    cursor: Optional[str],
    first: int,
    cookies: Optional[dict],
    cookies_file: str,
    current_doc_id: Optional[str]
) -> Optional[dict]:
    """Try doc_id candidates from network logs and fallback list if initial request failed."""
    from meta_ads_recent import fetch_recent_uploads
    import re

    # Gather candidates from network_log.json
    candidates = []
    try:
        if os.path.exists("network_log.json"):
            with open("network_log.json", "r", encoding="utf-8") as f:
                hits = json.load(f)
                for hit in hits:
                    post = hit.get("post", "")
                    for m in re.findall(r"doc_id\W?(\d{6,30})", post):
                        candidates.append(m)
    except Exception:
        pass

    # Also check cookies.json network_hits
    try:
        if os.path.exists(cookies_file):
            with open(cookies_file, "r", encoding="utf-8") as f:
                cdata = json.load(f)
                for hit in cdata.get("network_hits", []) or []:
                    post = hit.get("post", "")
                    for m in re.findall(r"doc_id\W?(\d{6,30})", post):
                        candidates.append(m)
    except Exception:
        pass

    # Append known fallback doc_ids
    fallback_list = ["25464068859919530", "9755915494515334", "25863916039859122", "29650582277919185"]
    candidates.extend(fallback_list)

    # Deduplicate while preserving order
    seen = set()
    candidates = [x for x in candidates if not (x in seen or seen.add(x))]

    print(f"[ad_fetcher] Trying {len(candidates)} doc_id candidates...")
    
    # Try each candidate with rate limiting
    for idx, cand in enumerate(candidates, 1):
        if not cand or cand == current_doc_id:
            continue
        
        print(f"[ad_fetcher] Candidate {idx}/{len(candidates)}: {cand}")
        
        # Try with retry logic
        for retry in range(MAX_RETRIES):
            try:
                resp = fetch_recent_uploads(
                    country="IN",
                    page_id=page_id,
                    cursor=cursor,
                    first=first,
                    lsd=None,
                    doc_id=cand,
                    cookies=cookies
                )
                if resp:
                    print(f"[ad_fetcher] ✓ Candidate {cand} worked! Persisting...")
                    # Persist working doc_id to cookies.json
                    try:
                        if os.path.exists(cookies_file):
                            with open(cookies_file, "r", encoding="utf-8") as f:
                                cd = json.load(f)
                        else:
                            cd = {}
                        cd["doc_id"] = cand
                        with open(cookies_file, "w", encoding="utf-8") as f:
                            json.dump(cd, f, indent=2)
                    except Exception as e:
                        print(f"[ad_fetcher] Failed to persist doc_id: {e}")
                    return resp
                
                # Wait before retry if no response
                if retry < MAX_RETRIES - 1:
                    backoff_delay = BASE_SLEEP * (BACKOFF_FACTOR ** retry) + random.uniform(0, JITTER * 2)
                    time.sleep(backoff_delay)
                    
            except Exception as e:
                if retry < MAX_RETRIES - 1:
                    backoff_delay = BASE_SLEEP * (BACKOFF_FACTOR ** retry) + random.uniform(0, JITTER * 2)
                    print(f"[ad_fetcher] Candidate {cand} attempt {retry+1} failed: {str(e)[:50]}, retrying...")
                    time.sleep(backoff_delay)
                continue
        
        # Add delay between candidates
        if idx < len(candidates):
            time.sleep(BASE_SLEEP + random.uniform(0, JITTER))

    print("[ad_fetcher] All doc_id candidates exhausted")
    return None


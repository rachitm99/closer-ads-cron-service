"""
Fetch recent ads until a cutoff date (inclusive) and save a flattened list of ad items.

Production-ready script with rate-limit protection, exponential backoff, and jitter.

Usage:
    python meta_ads_recent_until.py
"""

import json
import time
import datetime
import os
import random
from meta_ads_recent import fetch_recent_uploads

# ---------------------
# CONFIGURATION (edit here)
# ---------------------
# Target page configuration
PAGE_ID = "444025482768886"  # Facebook page ID to fetch ads for (REQUIRED)

# Date cutoff configuration — set either DAYS or DATE (not both)
DAYS = 10            # e.g., 10 or 30 — set to None to disable
DATE = None          # e.g., "2026-01-01" (YYYY-MM-DD). Set to None to disable

# Optional: provide INITIAL_CURSOR to start pagination where you like
INITIAL_CURSOR = None  # e.g., "AQHSW7qi563ySi42..." or None to start from first page

# Pagination & rate limiting
FIRST = 30           # results per page (10-30 recommended to balance API load)
MAX_PAGES = None     # No max limit; set to an integer to cap pages if desired
OUT = "recent_items.json"  # output file for flattened ad items
RAW_RESPONSES_OUT = "raw_responses.json"  # output file for raw API responses
COOKIES_FILE = "cookies.json"

# Rate limit protection (increased delays and jitter to avoid detection)
BASE_SLEEP = 1.2     # minimum seconds between pages (increased from 0.6)
JITTER = 0.4         # random jitter (0 to JITTER seconds) added to each delay
MAX_RETRIES = 3      # max retries per page on failure
BACKOFF_FACTOR = 2.0 # exponential backoff multiplier on retries
# ---------------------


def _try_doc_id_candidates_with_backoff(
    page_id: str,
    cursor: str,
    first: int,
    cookies: list,
    cookies_file: str,
    current_doc_id: str,
    base_sleep: float,
    jitter: float
) -> dict:
    """
    Try doc_id candidates from network logs with rate-limit protection.
    Returns the successful response dict or None if all candidates fail.
    """
    import re
    
    # Gather doc_id candidates from network logs
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

    # Also inspect cookies.json network_hits
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

    # Append fallback doc_ids (common ones observed)
    fallback_list = ["25464068859919530", "9755915494515334", "25863916039859122", "29650582277919185"]
    candidates.extend(fallback_list)

    # Deduplicate while preserving order
    seen = set()
    candidates = [x for x in candidates if not (x in seen or seen.add(x))]

    print(f"  🔍 Found {len(candidates)} doc_id candidates to try")
    
    for idx, cand in enumerate(candidates, 1):
        if not cand or cand == current_doc_id:
            continue
        
        print(f"    [{idx}/{len(candidates)}] Trying doc_id: {cand}")
        
        # Retry with exponential backoff for each candidate
        for retry in range(MAX_RETRIES):
            try:
                resp_try = fetch_recent_uploads(
                    page_id=page_id,
                    country="IN",
                    first=first,
                    cursor=cursor,
                    lsd=None,
                    doc_id=cand,
                    cookies=cookies
                )
                
                if resp_try:
                    print(f"    ✓ Candidate doc_id {cand} worked! Persisting to cookies...")
                    
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
                        print(f"    ⚠ Failed to persist doc_id: {e}")
                    
                    return resp_try
                
                # If no response but no exception, wait before retry
                if retry < MAX_RETRIES - 1:
                    backoff_delay = base_sleep * (BACKOFF_FACTOR ** retry) + random.uniform(0, jitter * 2)
                    time.sleep(backoff_delay)
                    
            except Exception as e:
                if retry < MAX_RETRIES - 1:
                    backoff_delay = base_sleep * (BACKOFF_FACTOR ** retry) + random.uniform(0, jitter * 2)
                    print(f"    ⚠ Attempt {retry + 1}/{MAX_RETRIES} failed: {str(e)[:50]}... retrying in {backoff_delay:.1f}s")
                    time.sleep(backoff_delay)
                else:
                    print(f"    ✗ All retries exhausted for candidate {cand}")
        
        # Add delay between candidates to avoid rate limiting
        if idx < len(candidates):
            time.sleep(base_sleep + random.uniform(0, jitter))
    
    print("  ✗ All doc_id candidates failed")
    return None


if __name__ == "__main__":
    # Validate configuration
    if not PAGE_ID:
        print("ERROR: PAGE_ID is required. Please set it in the configuration section.")
        raise SystemExit(1)

    # Load cookies and current doc_id if available
    cookies = None
    current_doc_id = None
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                cookies = data.get("cookies")
                current_doc_id = data.get("doc_id")
                if cookies:
                    print(f"✓ Loaded cookies from {COOKIES_FILE}")
                if current_doc_id:
                    print(f"✓ Loaded doc_id: {current_doc_id}")
        except Exception as e:
            print(f"⚠ Could not read {COOKIES_FILE}: {e}")
            print("  Proceeding without preloaded cookies (may result in rate limits)")

    if not DAYS and not DATE:
        print("ERROR: No cutoff set. Edit DAYS or DATE in this file to enable fetching until a cutoff.")
        raise SystemExit(1)

    # Compute cutoff epoch (inclusive)
    if DAYS:
        cutoff_epoch = int(time.time()) - int(DAYS) * 86400
        print(f"\n🔍 Fetching ads from last {DAYS} days (cutoff epoch: {cutoff_epoch})")
        print(f"   Page ID: {PAGE_ID}")
        print(f"   Results per page: {FIRST}")
        print(f"   Rate limit protection: {BASE_SLEEP}s base delay + {JITTER}s jitter\n")
    else:
        # parse YYYY-MM-DD as inclusive cutoff at 00:00:00 UTC
        try:
            dt = datetime.datetime.strptime(DATE, "%Y-%m-%d")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            cutoff_epoch = int(dt.timestamp())
            print(f"\n🔍 Fetching ads until {DATE} (cutoff epoch: {cutoff_epoch})")
            print(f"   Page ID: {PAGE_ID}")
            print(f"   Results per page: {FIRST}")
            print(f"   Rate limit protection: {BASE_SLEEP}s base delay + {JITTER}s jitter\n")
        except Exception as e:
            print(f"ERROR: Could not parse date '{DATE}': {e}")
            raise SystemExit(1)

    # Pagination loop with retry logic and rate limit protection
    collected = []
    raw_responses = []  # Store all raw API responses
    if INITIAL_CURSOR:
        cursor = INITIAL_CURSOR
        print(f"▶ Starting from INITIAL_CURSOR: {cursor[:50]}...")
    else:
        cursor = None
        print("▶ Starting from first page (no cursor)")

    page = 0
    done = False
    consecutive_failures = 0  # Track consecutive failures for circuit breaker

    # Continue paging until we run out of pages or stop condition
    while not done:
        page += 1
        print(f"\n[Page {page}] Fetching (cursor={cursor[:50] + '...' if cursor else 'None'})...")
        
        # Attempt request with exponential backoff retries
        resp = None
        for retry in range(MAX_RETRIES):
            if retry > 0:
                # Exponential backoff on retry
                backoff_delay = BASE_SLEEP * (BACKOFF_FACTOR ** retry) + random.uniform(0, JITTER * 2)
                print(f"  Retry {retry}/{MAX_RETRIES-1} after {backoff_delay:.2f}s backoff...")
                time.sleep(backoff_delay)
            
            try:
                resp = fetch_recent_uploads(
                    country="IN",
                    page_id=PAGE_ID,
                    cursor=cursor,
                    first=FIRST,
                    lsd=None,
                    doc_id=current_doc_id,
                    cookies=cookies
                )
                if resp:
                    consecutive_failures = 0  # reset failure counter on success
                    break
            except Exception as e:
                print(f"  ⚠ Request exception: {e}")
                continue

        # Handle persistent failures with doc_id candidate trials
        if not resp:
            consecutive_failures += 1
            print(f"  ✗ Request failed after {MAX_RETRIES} retries (consecutive failures: {consecutive_failures})")
            
            # Circuit breaker: stop if too many consecutive failures
            if consecutive_failures >= 5:
                print("\n⚠ Circuit breaker triggered: Too many consecutive failures. Stopping pagination.")
                print("  This may indicate rate limiting or authentication issues.")
                print("  Consider: 1) Running fetch_cookies_playwright.py to refresh tokens")
                print("            2) Increasing BASE_SLEEP delay")
                print("            3) Waiting before retrying")
                break
            
            print("  Trying doc_id candidates from logs...")
            resp = _try_doc_id_candidates_with_backoff(
                page_id=PAGE_ID,
                cursor=cursor,
                first=FIRST,
                cookies=cookies,
                cookies_file=COOKIES_FILE,
                current_doc_id=current_doc_id,
                base_sleep=BASE_SLEEP,
                jitter=JITTER
            )
            
            # Update current_doc_id if candidates succeeded
            if resp and os.path.exists(COOKIES_FILE):
                try:
                    with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                        current_doc_id = json.load(f).get("doc_id")
                        print(f"  ✓ Updated doc_id to: {current_doc_id}")
                except Exception:
                    pass

        if not resp:
            print("  ✗ Request still failed after all recovery attempts; stopping pagination")
            break
        
        # Reset consecutive failures on success
        consecutive_failures = 0
        
        # Store raw response for debugging
        raw_responses.append({
            "page": page,
            "cursor": cursor,
            "response": resp
        })

        conn = resp.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {})
        edges = conn.get("edges", [])
        page_info = conn.get("page_info", {})
        
        # Debug: show response structure
        print(f"  Debug: Found {len(edges)} edges in response")
        if edges:
            sample_node = edges[0].get("node", {})
            sample_collated = sample_node.get("collated_results", [])
            print(f"  Debug: First edge has {len(sample_collated)} collated_results")

        # Iterate results (edges -> node -> collated_results)
        page_new_items = 0
        page_old_items = 0
        page_has_older = False
        for edge in edges:
            node = edge.get("node", {})
            collated = node.get("collated_results", [])
            for item in collated:
                sd = item.get("start_date")
                # If no start_date, keep the item (can't compare)
                if sd is None:
                    collected.append(item)
                    page_new_items += 1
                    continue

                # If item is newer or equal to cutoff, include it
                if sd >= cutoff_epoch:
                    collected.append(item)
                    page_new_items += 1
                else:
                    # This item is older than cutoff; mark that this page contains older items
                    page_old_items += 1
                    page_has_older = True
                    # Do not append older items; continue scanning other items on the page
                    continue

        print(f"  [Page {page}] Processed: {page_new_items} new items, {page_old_items} old items (total: {len(collected)})")

        # New rule for 'last N days' fetch: continue paging while we keep finding items within the cutoff window
        # Stop when a page contains NO items within the cutoff (i.e., page_new_items == 0)
        if page_new_items == 0:
            print("\n  ✓ Page contains no items within cutoff window; stopping pagination")
            break

        if not page_info.get("has_next_page"):
            print("\n  ✓ No more pages available; pagination complete")
            break

        cursor = page_info.get("end_cursor")
        if not cursor:
            print("\n  ⚠ No end_cursor returned; cannot continue paging")
            break
        
        # Apply rate-limit delay with jitter between pages
        delay = BASE_SLEEP + random.uniform(0, JITTER * 2)
        time.sleep(delay)

    # Save flattened items
    out_path = OUT
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, indent=2, ensure_ascii=False)
    
    # Save raw responses for debugging
    raw_out_path = RAW_RESPONSES_OUT
    with open(raw_out_path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved {len(collected)} ad items to {out_path}")
    print(f"✓ Saved {len(raw_responses)} raw responses to {raw_out_path}")
    print(f"✓ Pagination completed after {page} pages")
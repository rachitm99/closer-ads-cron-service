"""
Fetch Meta Ads Library results sorted by recent uploads and save to `recent_response.json`.

Usage:
    python meta_ads_recent.py

This script reuses `fetch_meta_ads_library` from `meta_ads_library.py` and sets the
sorting mode to SORT_BY_RELEVANCY_MONTHLY_GROUPED (recent uploads grouped by month).
"""

import json
from meta_ads_library import fetch_meta_ads_library


def fetch_recent_uploads(
    country: str = "IN",
    page_id: str = "444025482768886",
    # page_id: str = "308723279160232",
    cursor: str | None = None,
    first: int = 30,
    lsd: str = "AdRdsaUcinmykhxF6NxP9jcCLnM",
    doc_id: str = "25464068859919530",
    cookies: dict | None = None
):
    """Fetch results sorted by recent uploads (relevancy monthly grouped)."""

    return fetch_meta_ads_library(
        country=country,
        page_id=page_id,
        cursor=cursor,
        first=first,
        active_status="active",
        ad_type="ALL",
        media_type="video",
        search_type="page",
        sort_mode="SORT_BY_RELEVANCY_MONTHLY_GROUPED",
        sort_direction="DESCENDING",
        source="FB_LOGO",
        lsd=lsd,
        doc_id=doc_id,
        cookies=cookies,
        refresh_on_429=True  # Enable auto-refresh for production
    )


if __name__ == "__main__":
    import time
    import datetime
    import os

    # ---------------------
    # CONFIGURATION (edit here)
    # ---------------------
    # Set either DAYS or DATE (exclusive). Set both to None to fetch a single page and save the full response.
    DAYS = 10            # e.g., 10 or 30 — set to None to disable
    DATE = None          # e.g., "2026-01-01" (YYYY-MM-DD). Set to None to disable

    FIRST = 30           # results per page
    MAX_PAGES = None      # No max limit; set to an integer to cap pages if desired
    OUT = "recent_items.json"  # output file for flattened ad items
    COOKIES_FILE = "cookies.json"
    SLEEP = 0.6          # seconds to wait between pages
    # ---------------------

    # Load cookies if available
    cookies = None
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                cookies = data.get("cookies")
                if cookies:
                    print(f"Loaded cookies from {COOKIES_FILE}")
        except Exception:
            print(f"Could not read {COOKIES_FILE}; proceeding without preloaded cookies")

    # If no cutoff provided, behave like original script and save the single page response
    # (Restored to original simple behavior)
    print("Fetching recent uploads (single request; cursor=None)...\n")

    # Try single request with no cursor
    result = fetch_recent_uploads(
        country="IN",
        cursor=None,
        first=30,
        lsd=None,
        doc_id=None,
        cookies=None
    )

    # If the request failed, attempt a headless Playwright token refresh once then retry
    if not result:
        print("Initial request failed; attempting headless Playwright refresh and retry...")
        try:
            from meta_ads_library import refresh_cookies_via_playwright, load_tokens_from_file
            ok = refresh_cookies_via_playwright(output_file="cookies.json", headless=True)
            if ok:
                _, _, _ = load_tokens_from_file("cookies.json")
                result = fetch_recent_uploads(
                    country="IN",
                    cursor=None,
                    first=30,
                    lsd=None,
                    doc_id=None,
                    cookies=None
                )
        except Exception as e:
            print(f"Refresh retry failed: {e}")

    if result:
        print("\n=== SUCCESS: recent uploads response received ===")
        # Save full response for inspection
        with open("recent_response.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print("✓ Saved full response to recent_response.json")

        # Flatten ad items and save to recent_items.json (only ad items)
        conn = result.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {})
        edges = conn.get("edges", [])
        items = []
        for edge in edges:
            node = edge.get("node", {})
            collated = node.get("collated_results", [])
            if collated:
                items.extend(collated)

        with open("recent_items.json", "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(items)} flattened ad items to recent_items.json")

        # --- New: attempt to fetch the next two pages (2nd and 3rd) using end_cursor from the previous response ---
        page_info = result.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {}).get("page_info", {})
        end_cursor = page_info.get("end_cursor")
        has_next = page_info.get("has_next_page")
        if end_cursor and has_next:
            print(f"Found end_cursor in first response: {end_cursor}. Will fetch up to two more pages using that cursor.")
            # Iterate pages 2 and 3
            for page_num in (2, 3):
                print(f"Fetching page {page_num} with cursor: {end_cursor}...")
                resp = fetch_recent_uploads(
                    country="IN",
                    cursor=end_cursor,
                    first=30,
                    lsd=None,
                    doc_id=None,
                    cookies=None
                )

                # If request failed, attempt one headless Playwright refresh + retry
                if not resp:
                    print(f"Page {page_num} request failed; attempting headless Playwright refresh and retry...")
                    try:
                        from meta_ads_library import refresh_cookies_via_playwright, load_tokens_from_file
                        ok = refresh_cookies_via_playwright(output_file="cookies.json", headless=True)
                        if ok:
                            _, _, _ = load_tokens_from_file("cookies.json")
                            resp = fetch_recent_uploads(
                                country="IN",
                                cursor=end_cursor,
                                first=30,
                                lsd=None,
                                doc_id=None,
                                cookies=None
                            )
                    except Exception as e:
                        print(f"Refresh retry for page {page_num} failed: {e}")

                if resp:
                    print(f"Page {page_num} response received; flattening and appending items...")
                    conn_p = resp.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {})
                    edges_p = conn_p.get("edges", [])
                    new_items = []
                    for edge in edges_p:
                        node = edge.get("node", {})
                        collated = node.get("collated_results", [])
                        if collated:
                            new_items.extend(collated)

                    if new_items:
                        items.extend(new_items)
                        with open("recent_items.json", "w", encoding="utf-8") as f:
                            json.dump(items, f, indent=2, ensure_ascii=False)
                        print(f"✓ Appended {len(new_items)} items from page {page_num}. Total items now: {len(items)}")
                    else:
                        print(f"Page {page_num} had no collated ad items to append.")

                    # Save the full response for inspection
                    with open(f"recent_response_page{page_num}.json", "w", encoding="utf-8") as f:
                        json.dump(resp, f, indent=2, ensure_ascii=False)
                    print(f"✓ Saved full page {page_num} response to recent_response_page{page_num}.json")

                    # Prepare for next iteration: update cursor & has_next
                    page_info = conn_p.get("page_info", {})
                    end_cursor = page_info.get("end_cursor")
                    has_next = page_info.get("has_next_page")
                    if not end_cursor or not has_next:
                        print(f"No further end_cursor/has_next_page after page {page_num}; stopping additional fetches.")
                        break
                    # Continue to next page_num
                else:
                    print(f"Page {page_num} request failed after retry; stopping further pages.")
                    break
        else:
            print("No end_cursor/next page found in first response; not fetching additional pages.")
    else:
        print("\n=== Failed to fetch recent uploads - check output above for details ===")
    exit(0)

    # Compute cutoff epoch (inclusive). If DAYS provided, cutoff is now - days*86400
    if DAYS:
        cutoff_epoch = int(time.time()) - int(DAYS) * 86400
        print(f"Fetching newest → oldest until items >= {DAYS} days ago (cutoff epoch: {cutoff_epoch})")
    else:
        # parse YYYY-MM-DD as inclusive cutoff at 00:00:00 UTC
        try:
            dt = datetime.datetime.strptime(DATE, "%Y-%m-%d")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            cutoff_epoch = int(dt.timestamp())
            print(f"Fetching newest → oldest until {DATE} (cutoff epoch: {cutoff_epoch})")
        except Exception as e:
            print(f"Could not parse date '{DATE}': {e}")
            raise SystemExit(1)

    # Pagination loop: fetch pages until we reach older-than-cutoff items
    collected = []
    cursor = None  # Start with no cursor for the first page
    page = 0
    done = False

    # Continue paging until a stopping condition is met (no MAX_PAGES cap)
    while not done:
        print(f"\nFetching page {page+1} (cursor={cursor})...")
        resp = fetch_recent_uploads(
            country="IN",
            first=FIRST,
            cursor=cursor,
            lsd=None,
            doc_id=None,
            cookies=cookies
        )

        if not resp:
            print("Request failed or returned empty response; stopping")
            break

        conn = resp.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {})
        edges = conn.get("edges", [])
        page_info = conn.get("page_info", {})

        # Iterate results (edges -> node -> collated_results)
        page_new_items = 0
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
                    # This item is older than cutoff; we can stop overall (inclusive)
                    print(f"Reached older item with start_date {sd} < cutoff {cutoff_epoch}; stopping.")
                    done = True
                    break
            if done:
                break

        print(f"Page {page+1} processed: added {page_new_items} items (total {len(collected)})")

        # Check if we should continue to next page
        if done:
            break

        if not page_info.get("has_next_page"):
            print("No more pages available; finished")
            break

        cursor = page_info.get("end_cursor")
        if not cursor:
            print("No end_cursor returned; cannot continue paging")
            break

        page += 1
        time.sleep(SLEEP)

    # Save flattened items only
    out_path = OUT
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved {len(collected)} ad items to {out_path}")

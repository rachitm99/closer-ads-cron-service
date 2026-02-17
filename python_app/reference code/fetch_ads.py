"""
fetch_ads.py

Fetch ads from the facebook-ads-library-scraper-api (RapidAPI) and append a simplified
JSON list containing selected fields for each ad.

Fields extracted per ad:
- ad_archive_id
- page_id
- video_hd_url (first video if available)
- page_name
- start_date (raw epoch integer from API, if present)
- end_date (raw epoch integer from API, if present)
- ad_url (the raw `url` field from the API response)

Usage examples:
  # set env var RAPIDAPI_KEY or pass --api-key
  python fetch_ads.py --page-id 444025482768886 --output ads_list.json

Requirements:
  pip install requests

"""

import argparse
import os
import json
from datetime import datetime, timezone

import requests

API_HOST = "facebook-ads-library-scraper-api.p.rapidapi.com"
DEFAULT_URL = "https://facebook-ads-library-scraper-api.p.rapidapi.com/company/ads"

# ---------------------------------------------------------------------------
# Hardcoded configuration — edit these values in the file if you prefer not to
# pass them via CLI or environment variables.
# NOTE: For security, avoid committing real API keys to version control.
HARDCODED_CONFIG = {
    "API_KEY": "e84044d9c8mshbbf48b6d2898864p102cfbjsnd2d19beee4cf",  # set your RapidAPI key here
    "PAGE_ID": "444025482768886",
    "OUTPUT": "ads_list.json",
    "COUNTRY": "IN",
    "MEDIA_TYPE": "VIDEO",
    "STATUS": "ACTIVE",
    "TRIM": "false",
    "PAGE": 4,
    "DEDUPE": False,
}
# ---------------------------------------------------------------------------


def extract_item(item):
    ad_archive_id = item.get("ad_archive_id")
    page_id = item.get("page_id")

    # video hd url may exist at snapshot.videos[0].video_hd_url
    video_hd_url = None
    snapshot = item.get("snapshot") or {}
    videos = snapshot.get("videos") or []
    if videos:
        # prefer HD, fall back to SD when HD is not present
        video_hd_url = videos[0].get("video_hd_url") or videos[0].get("video_sd_url")

    # Some ad items use a DCO/display format where video assets are within
    # `snapshot.cards` instead of `snapshot.videos`. If no video found above,
    # search cards and pick the first card that has a video (prefer HD then SD).
    if not video_hd_url:
        cards = snapshot.get("cards") or []
        for c in cards:
            if c.get("video_hd_url"):
                video_hd_url = c.get("video_hd_url")
                break
            if c.get("video_sd_url"):
                # fallback to sd if hd isn't present
                video_hd_url = c.get("video_sd_url")
                break

    # page_name — prefer top-level page_name then snapshot.page_name
    page_name = item.get("page_name") or snapshot.get("page_name")

    # preserve the raw epoch values from the API and expose them as
    # `start_date` and `end_date` (integers), not strings
    start_date = item.get("start_date")
    end_date = item.get("end_date")

    return {
        "ad_archive_id": ad_archive_id,
        "page_id": page_id,
        "video_hd_url": video_hd_url,
        "page_name": page_name,
        "start_date": start_date,
        "end_date": end_date,
        "ad_url": item.get("url"),
    }


def fetch_ads(api_key, page_id, country="IN", media_type="VIDEO", status="ACTIVE", trim="false", page=1):
    """
    Fetch ads and support cursor-based pagination.

    If `page` == 1 (default), a single request is made. If `page` > 1, the function
    will perform up to `page` sequential requests: the first without a `cursor`, then
    subsequent requests include the `cursor` value returned by the previous response
    (as required by the API).

    Returns a dict with combined `results` (list) and the last-seen `cursor`.
    """
    headers = {
        "x-rapidapi-host": API_HOST,
        "x-rapidapi-key": api_key,
    }

    # base params used for every request; do not include 'cursor' here unless set
    base_params = {
        "status": status,
        "trim": trim,
        "country": country,
        "media_type": media_type,
        "pageId": page_id,
    }

    combined_results = []
    cursor = None

    for i in range(max(1, int(page))):
        params = dict(base_params)
        # include cursor for subsequent requests
        if cursor:
            params["cursor"] = cursor

        # perform request
        resp = requests.get(DEFAULT_URL, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        j = resp.json()

        results = j.get("results") or []
        combined_results.extend(results)

        # get next cursor; if none, stop early
        cursor = j.get("cursor")
        if not cursor:
            break

    # return structure similar to original response, but with combined results
    return {"results": combined_results, "cursor": cursor}


def read_existing_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def write_list(path, items):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Fetch ads and append simplified ad entries to JSON file")
    parser.add_argument("--page-id", required=False, default=HARDCODED_CONFIG["PAGE_ID"], help="Page ID to query (e.g., 444025482768886)")
    parser.add_argument("--api-key", help="RapidAPI key (or set RAPIDAPI_KEY env var). If omitted, HARDCODED_CONFIG['API_KEY'] will be used if set.")
    parser.add_argument("--output", default=HARDCODED_CONFIG["OUTPUT"], help="Output JSON list file")
    parser.add_argument("--dedupe", action="store_true", default=HARDCODED_CONFIG["DEDUPE"], help="Remove duplicates by ad_archive_id when appending")
    parser.add_argument("--country", default=HARDCODED_CONFIG["COUNTRY"], help="Country code (default IN)")
    parser.add_argument("--media-type", default=HARDCODED_CONFIG["MEDIA_TYPE"], help="Media type filter (default VIDEO)")
    parser.add_argument("--status", default=HARDCODED_CONFIG["STATUS"], help="Ad status (default ACTIVE)")
    parser.add_argument("--trim", default=HARDCODED_CONFIG["TRIM"], help="Trim parameter (default false)")
    parser.add_argument("--page", type=int, default=HARDCODED_CONFIG["PAGE"], help="Page number for API pagination (default 1)")
    parser.add_argument("--raw-output", default="raw_data.json", help="Write the combined raw API response to this file before parsing")

    args = parser.parse_args()
    api_key = args.api_key or os.environ.get("RAPIDAPI_KEY") or HARDCODED_CONFIG["API_KEY"]
    if not api_key or api_key == "REPLACE_WITH_YOUR_RAPIDAPI_KEY":
        raise SystemExit("Error: No API key provided. Set --api-key, RAPIDAPI_KEY env var, or update HARDCODED_CONFIG['API_KEY'] in the script.")

    try:
        resp = fetch_ads(api_key, args.page_id, country=args.country, media_type=args.media_type, status=args.status, trim=args.trim, page=args.page)
    except requests.RequestException as e:
        raise SystemExit(f"Request failed: {e}")

    # Save raw combined response to file before parsing
    raw_out = args.raw_output
    try:
        with open(raw_out, "w", encoding="utf-8") as rf:
            json.dump(resp, rf, ensure_ascii=False, indent=2)
        print(f"Wrote raw response to {raw_out}")
    except Exception as e:
        print(f"Warning: failed to write raw response to {raw_out}: {e}")

    results = resp.get("results") or []
    extracted = [extract_item(item) for item in results]

    # read existing
    existing = read_existing_list(args.output)

    if args.dedupe:
        # keep by ad_archive_id unique
        existing_map = {it.get("ad_archive_id"): it for it in existing}
        for it in extracted:
            existing_map[it.get("ad_archive_id")] = it
        merged = list(existing_map.values())
        write_list(args.output, merged)
        print(f"Wrote {len(merged)} unique items to {args.output}")
    else:
        combined = existing + extracted
        write_list(args.output, combined)
        print(f"Appended {len(extracted)} items to {args.output} (total {len(combined)})")


if __name__ == "__main__":
    main()

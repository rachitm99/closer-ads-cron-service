import requests
import json
import uuid
import time
import os
import subprocess
from typing import Optional, Tuple


def load_tokens_from_file(path: str = "cookies.json") -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    """Load cookies, lsd and doc_id from a JSON file. Returns (cookies, lsd, doc_id)."""
    if not os.path.exists(path):
        return None, None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Could not read tokens file {path}: {e}")
        return None, None, None
    return data.get("cookies"), data.get("lsd"), data.get("doc_id")


def refresh_cookies_via_playwright(output_file: str = "cookies.json", headless: bool = True) -> bool:
    """Try to refresh cookies by running the Playwright helper (module or script)."""
    try:
        # Prefer direct import if available
        import fetch_cookies_playwright
        fetch_cookies_playwright.fetch_and_save_cookies(output_file=output_file, headless=headless)
        return True
    except Exception:
        # Fallback to subprocess
        try:
            subprocess.run(["python", "fetch_cookies_playwright.py", "--output", output_file] + (["--no-headless"] if not headless else []), check=True)
            return True
        except Exception as e:
            print(f"Could not refresh cookies via Playwright: {e}")
            return False


def fetch_meta_ads_library(
    country="IN",
    page_id="444025482768886",
    cursor=None,
    first=10,
    active_status="active",
    ad_type="ALL",
    media_type="video",
    search_type="page",
    sort_mode="SORT_BY_RELEVANCY_MONTHLY_GROUPED",
    sort_direction="DESCENDING",
    session_id=None,
    query_string="",
    source="FB_LOGO",
    lsd="AdRdsaUcinmykhxF6NxP9jcCrjc",  # From your curl request
    doc_id="25464068859919530",
    cookies=None,  # Optional cookies dict
    cookies_file: str = "cookies.json",
    refresh_on_429: bool = True  # Enable auto-refresh on rate limit by default
):
    """
    Fetch ads from Meta Ads Library via GraphQL API
    
    params:
    - country: Country code (e.g., "IN", "US")
    - page_id: Facebook page ID to view ads for
    - cursor: Pagination cursor (None for first page)
    - first: Number of results per page
    - lsd: Facebook LSD token
    - doc_id: GraphQL document ID
    - cookies: Optional dict of cookies (recommended for avoiding rate limits)
    """
    
    # Auto-generate session_id if not provided
    if session_id is None:
        session_id = str(uuid.uuid4())
    
    # Create a session to persist cookies
    session = requests.Session()

    # Load cookies from file if not provided
    if not cookies and cookies_file:
        file_cookies, file_lsd, file_doc_id = load_tokens_from_file(cookies_file)
        if file_cookies:
            cookies = file_cookies
        if file_lsd and (lsd is None or lsd == ""):
            lsd = file_lsd
        if file_doc_id and (doc_id is None or doc_id == ""):
            doc_id = file_doc_id

    # Add cookies if provided
    if cookies:
        for name, value in cookies.items():
            session.cookies.set(name, value, domain=".facebook.com", path="/")

    url = "https://www.facebook.com/api/graphql/"

    # Complete headers from your curl request
    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.facebook.com",
        "priority": "u=1, i",
        "referer": f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={country}&is_targeted_country=false&media_type=video&search_type=page&sort_data[mode]=relevancy_monthly_grouped&sort_data[direction]=desc&view_all_page_id={page_id}",
        "sec-ch-prefers-color-scheme": "dark",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-full-version-list": '"Not(A:Brand";v="8.0.0.0", "Chromium";v="144.0.7559.97", "Google Chrome";v="144.0.7559.97"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-platform-version": '"19.0.0"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "x-asbd-id": "359341",
        "x-fb-friendly-name": "AdLibrarySearchPaginationQuery",
        "x-fb-lsd": lsd
    }
    
    # GraphQL variables
    variables = {
        "activeStatus": active_status,
        "adType": ad_type,
        "bylines": [],
        "collationToken": None,
        "contentLanguages": [],
        "countries": [country],
        "cursor": cursor,
        "excludedIDs": None,
        "first": first,
        "isTargetedCountry": False,
        "location": None,
        "mediaType": media_type,
        "multiCountryFilterMode": None,
        "pageIDs": [],
        "potentialReachInput": None,
        "publisherPlatforms": [],
        "regions": None,
        "searchType": search_type,
        "sessionID": session_id,
        "sortData": {
            "direction": sort_direction,
            "mode": sort_mode
        },
        "source": source,
        "startDate": None,
        "v": "df17bc",
        "viewAllPageID": page_id,
        "queryString": query_string
    }
    
    # Complete request body from your curl request
    data = {
        "av": "0",
        "__aaid": "0",
        "__user": "0",
        "__a": "1",
        "__req": "b",
        "__hs": "20490.HYP:comet_plat_default_pkg.2.1...0",
        "dpr": "1",
        "__ccg": "GOOD",
        "__rev": "1033033272",
        "__s": "c10plu:9nagcj:k4j1t9",
        "__hsi": "7603650629806450781",
        "__dyn": "7xeUmwlECdwn8K2Wmh0no6u5U4e1Fx-ewSAwHwNw9G2S2q0_EtxG4o0B-qbwgE1EEb87C1xwEwgo9oO0n24oaEd86a3a1YwBgao6C0Mo6i588Etw8WfK1LwPxe2GewbCXwJwmE2eUlwhE2Lw6OyES0gq0K-1LwqobU3Cwr86C1nwf6Eb87u0xE2Two8",
        "__csr": "jgj_q8gxal_HiJ2aiVtfRQXmhdaldqiQAdhpry8x0xC-eGcxNopCBCxi68txG1wxm1DyEOu78lxy4U2JwNxu0BpE6GvwlES15wdC18wJwEwea0Eo3-xq2K210po4W099w0DZw0yMw3P804su05po5e0aXw0Vng02Ucw0vdSi00CT807aG0biwt86a",
        "__hsdp": "l2QG49We2W6yAAhqByrhpO2d8H8cV3e6j2iAgVAy-i4Q2d0Wx-b82B1immtNqaGh9uJ1y0R-0DU5y13wt80zG0TU5S3WewgC0wU562y2C0t23qbDig-pZ0Gw1Tm02G-01jaw0nDE05oG",
        "__hblp": "02h808480zS0eIw3Z86GU0iVw58w13S010sw2wE0yG0pm0dgw1l-02xG16wde0s61vw14G03Tq0sy0c_wFw0z9x61bwkU2qw0O6w1sG0xo",
        "__sjsp": "l2QG49We2W6yAAhqByrhpO2d8H8cV3e6j2iAgVAy-i4Q2d0Wx-b82B1immtNql5h9uJ1y0R-0DU5y13wt80zG0TU5S3WewgC0wU562y0vG3qbDig-pZ0Gw",
        "__comet_req": "94",
        "lsd": lsd,
        "jazoest": "22569",
        "__spin_r": "1033033272",
        "__spin_b": "trunk",
        "__spin_t": "1770362870",
        "__jssesw": "1",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "AdLibrarySearchPaginationQuery",
        "server_timestamps": "true",
        "variables": json.dumps(variables),
        "doc_id": doc_id
    }
    
    # Try request with optional retry & refresh on 429
    max_retries = 2
    last_response = None
    
    # Debug: print variables to verify queryString is excluded
    print(f"[DEBUG] Variables keys: {list(variables.keys())}")
    print(f"[DEBUG] Has queryString: {'queryString' in variables}")
    if 'queryString' in variables:
        print(f"[DEBUG] queryString value: '{variables['queryString']}'")
    
    for attempt in range(max_retries):
        print(f"\nMaking GraphQL request... (attempt {attempt+1})")
        response = session.post(url, headers=headers, data=data)
        last_response = response
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")

        if response.status_code == 200:
            try:
                resp_json = response.json()
                print(f"[DEBUG] Response JSON keys: {list(resp_json.keys()) if isinstance(resp_json, dict) else 'not a dict'}")
                
                # Check for errors in response
                if 'errors' in resp_json:
                    print(f"[DEBUG] GraphQL Errors: {json.dumps(resp_json.get('errors'), indent=2)}")
                
                # Check for data
                if 'data' in resp_json:
                    data_keys = list(resp_json['data'].keys()) if isinstance(resp_json['data'], dict) else []
                    print(f"[DEBUG] Data keys: {data_keys}")
                    
                    # Try to find the search results
                    for key in data_keys:
                        if resp_json['data'][key]:
                            print(f"[DEBUG] Data['{key}'] type: {type(resp_json['data'][key])}")
                            if isinstance(resp_json['data'][key], dict):
                                inner_keys = list(resp_json['data'][key].keys())
                                print(f"[DEBUG] Data['{key}'] keys: {inner_keys}")
                                # If search_results_connection exists, show its structure
                                if 'search_results_connection' in inner_keys:
                                    conn = resp_json['data'][key]['search_results_connection']
                                    print(f"[DEBUG] search_results_connection type: {type(conn)}")
                                    if isinstance(conn, dict):
                                        conn_keys = list(conn.keys())
                                        print(f"[DEBUG] search_results_connection keys: {conn_keys}")
                                        if 'edges' in conn_keys:
                                            edges = conn['edges']
                                            print(f"[DEBUG] edges count: {len(edges) if isinstance(edges, list) else 'not a list'}")
                
                return resp_json
            except json.JSONDecodeError:
                print(f"Failed to parse JSON. Response text: {response.text[:500]}")
                return None

        if response.status_code == 429 and refresh_on_429 and attempt < max_retries - 1:
            print("Received 429. Attempting to refresh cookies via Playwright and retry...")
            success = refresh_cookies_via_playwright(output_file=cookies_file, headless=True)
            if not success:
                print("Cookie refresh failed; aborting retries.")
                break
            # reload tokens from file
            file_cookies, file_lsd, file_doc_id = load_tokens_from_file(cookies_file)
            if file_cookies:
                for name, value in file_cookies.items():
                    session.cookies.set(name, value, domain=".facebook.com", path="/")
            if file_lsd:
                lsd = file_lsd
                headers["x-fb-lsd"] = lsd
            if file_doc_id:
                doc_id = file_doc_id
            time.sleep(1)  # small pause before retry
            continue
        else:
            # Non-retryable error or no refresh requested
            print(f"Request failed with status {response.status_code}.")
            print(f"Error Response: {response.text[:500]}")
            return None

    # If loop ends unexpectedly
    if last_response is not None and last_response.status_code == 200:
        try:
            return last_response.json()
        except Exception:
            return None
    return None


if __name__ == "__main__":
    # Cookies from your curl request
    cookies = {
        "datr": "GE58ac3j9p1GBiu7e8Ftm53K",
        "dpr": "1.5",
        "ps_l": "1",
        "ps_n": "1",
        "rd_challenge": "Q_6hBQNuy9sZQuJCxDJ_M3iCeh3CiKbPJyJxc2v-gcMfco6XZdqMCdKdY9ld2jyK8jWiEFNGfqqmlpil_zaUISxpgoqsY51GkLSl6f5YLH4G1kXue4FtiMXyxGEadw",
        "wd": "1707x791"
    }

    # Example usage with cookies (default sorting)
    print("Fetching Meta Ads Library data (default sorting)...\n")
    result = fetch_meta_ads_library(
        country="IN",
        page_id="444025482768886",
        cursor=None,
        first=10,
        lsd="AdRdsaUcinmykhxF6NxP9jcCrjc",
        cookies=cookies
    )

    if result:
        print("\n" + "="*50)
        print("SUCCESS! Got response:")
        print("="*50)
        print(json.dumps(result, indent=2)[:1000])  # Print first 1000 chars

        # Save full response to file
        with open("response.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print("\n✓ Full response saved to response.json")
    else:
        print("\n" + "="*50)
        print("Request failed - check output above for details")
        print("="*50)

    # --- New: fetch ads sorted by recent uploads (relevancy monthly grouped) ---
    print("\nFetching Meta Ads Library sorted by recent uploads...\n")
    recent = fetch_meta_ads_library(
        country="IN",
        page_id="444025482768886",
        cursor=None,
        first=10,
        active_status="ACTIVE",
        ad_type="ALL",
        media_type="ALL",
        search_type="PAGE",
        sort_mode="SORT_BY_RELEVANCY_MONTHLY_GROUPED",
        sort_direction="DESCENDING",
        lsd="AdRdsaUcinmykhxF6NxP9jcCLnM",
        cookies={
            "datr": "GE58ac3j9p1GBiu7e8Ftm53K",
            "dpr": "1.5",
            "ps_l": "1",
            "ps_n": "1",
            "rd_challenge": "Q_6hBQNuy9sZQuJCxDJ_M3iCeh3CiKbPJyJxc2v-gcMfco6XZdqMCdKdY9ld2jyK8jWiEFNGfqqmlpil_zaUISxpgoqsY51GkLSl6f5YLH4G1kXue4FtiMXyxGEadw",
            "wd": "985x791"
        }
    )

    if recent:
        print("\n" + "="*50)
        print("RECENT UPLOADS: Got response:")
        print("="*50)
        print(json.dumps(recent, indent=2)[:1000])

        with open("recent_response.json", "w", encoding="utf-8") as f:
            json.dump(recent, f, indent=2, ensure_ascii=False)
        print("\n✓ Full recent uploads response saved to recent_response.json")
    else:
        print("\n" + "="*50)
        print("Recent uploads request failed - check output above for details")
        print("="*50)

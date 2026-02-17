import re
import json
import time
import argparse

from playwright.sync_api import sync_playwright


def fetch_and_save_cookies(output_file: str = "cookies.json", headless: bool = True, url: str = None, page_id: str = None) -> dict:
    """Launch Playwright, visit the Ads Library page, extract cookies, lsd and doc_id, and save to a JSON file.

    Args:
        output_file: Path to save cookies JSON
        headless: Whether to run browser headless
        url: Custom URL to visit (overrides page_id)
        page_id: Facebook page ID to generate cookies for (used in URL if url is None)

    Returns the saved dict.
    """
    if url is None:
        # Use provided page_id or fallback to default
        default_page = page_id if page_id else "111982120196545"
        url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=IN&is_targeted_country=false&media_type=all&search_type=page&sort_data[mode]=relevancy_monthly_grouped&sort_data[direction]=desc&view_all_page_id={default_page}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context(user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                                  "Chrome/144.0.0.0 Safari/537.36"), locale="en-GB")
        page = context.new_page()
        
        # Set longer timeout for Facebook's heavy pages
        page.set_default_timeout(90000)  # 90 seconds
        
        print(f"Opening {url} ...")
        # Use 'domcontentloaded' instead of 'networkidle' - less strict, more reliable
        page.goto(url, wait_until="domcontentloaded", timeout=90000)

        # Wait for dynamic content to load
        page.wait_for_timeout(3000)

        # Extract cookies
        cookies_list = context.cookies()
        cookies = {c["name"]: c.get("value") for c in cookies_list}

        # Extract LSD token from page content (fallback)
        html = page.content()
        lsd = None
        m = re.search(r'"LSD",\[\],\{\"token\":\"([^\"]+)\"\}', html)
        if m:
            lsd = m.group(1)
            print("Extracted LSD token from page source.")
        else:
            print("Could not find LSD token in page source (it may be injected via script).")

        # Try to capture doc_id from network responses (POST to /api/graphql/). This is more reliable in headless.
        doc_id = None
        def handle_response(response):
            nonlocal doc_id
            try:
                if "/api/graphql/" in response.url and response.request.method.upper() == "POST":
                    post = response.request.post_data or ""
                    # Try parsing as form-encoded
                    try:
                        from urllib.parse import parse_qs
                        parsed = parse_qs(post)
                        if "doc_id" in parsed and parsed["doc_id"]:
                            doc_id = parsed["doc_id"][0]
                            print(f"Captured doc_id from network request (form): {doc_id}")
                            return
                    except Exception:
                        pass
                    # Fallback: regex search in post body
                    m_net = re.search(r"doc_id=?(\d{6,})", post)
                    if m_net:
                        doc_id = m_net.group(1)
                        print(f"Captured doc_id from network request (regex): {doc_id}")
                        return
                    # As a last resort, try inspecting response body for doc_id
                    try:
                        body = response.text()
                        m_body = re.search(r'"doc_id"\s*:\s*"(\d{6,})"', body)
                        if m_body:
                            doc_id = m_body.group(1)
                            print(f"Captured doc_id from response body: {doc_id}")
                            return
                    except Exception:
                        pass
            except Exception:
                pass

        # Also capture POST bodies to a network log for inspection
        network_hits = []
        def handle_response_and_log(response):
            try:
                if "/api/graphql/" in response.url and response.request.method.upper() == "POST":
                    post = response.request.post_data or ""
                    network_hits.append({
                        "url": response.url,
                        "post": post[:2000]
                    })
                    print("Captured /api/graphql/ POST (logged).")
            except Exception:
                pass
            # then call main handler
            handle_response(response)

        page.on("response", handle_response_and_log)

        # Try to interact with the page to provoke GraphQL requests (search, click)
        try:
            triggered = False
            # Prefer more robust selectors and keyboard events
            search_selectors = ["input[type='search']", "input[placeholder*='Search']", "input[aria-label*='Search']", "input[role='search']"]
            for sel in search_selectors:
                elements = page.query_selector_all(sel)
                if elements:
                    for el in elements:
                        try:
                            el.fill("test")
                            el.press("Enter")
                            triggered = True
                            break
                        except Exception:
                            continue
                    if triggered:
                        break

            if not triggered:
                # Try clicking likely filters / search buttons to trigger network
                buttons = page.query_selector_all("button")
                for b in buttons[:30]:
                    try:
                        text = (b.inner_text() or "").lower()
                        if any(k in text for k in ("search", "apply", "view", "filter", "results", "sort", "see all", "view all")):
                            b.click()
                            page.wait_for_timeout(800)
                            triggered = True
                            break
                    except Exception:
                        continue
            if not triggered:
                # Try clicking anchors that likely open full page results
                anchors = page.query_selector_all("a[href*='view_all_page_id']")
                for a in anchors[:6]:
                    try:
                        a.click()
                        page.wait_for_timeout(1000)
                        triggered = True
                        break
                    except Exception:
                        continue
            if not triggered:
                # If nothing obvious, scroll to bottom multiple times to trigger lazy loads and XHRs
                for i in range(4):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1200)
                triggered = True

            if triggered:
                print("Triggered interactions (search/click/scroll) to provoke GraphQL requests...")
            # Attempt to click a typeahead suggestion or result to provoke the main search pagination GraphQL
            try:
                suggestion_selectors = ["[role='option']", "li[role='option']", "div[role='option']", ".uiTypeaheadResult", ".typeaheadItem"]
                clicked_suggestion = False
                for sel in suggestion_selectors:
                    elements = page.query_selector_all(sel)
                    if elements:
                        for el in elements[:6]:
                            try:
                                el.click()
                                page.wait_for_timeout(1200)
                                clicked_suggestion = True
                                print(f"Clicked suggestion element using selector {sel}")
                                break
                            except Exception:
                                continue
                    if clicked_suggestion:
                        break
                if not clicked_suggestion:
                    # Try clicking first search result card
                    cards = page.query_selector_all("[data-testid='ad-card']")
                    if cards:
                        try:
                            cards[0].click()
                            page.wait_for_timeout(1200)
                            print("Clicked first ad card to provoke further GraphQL requests")
                        except Exception:
                            pass
            except Exception as e:
                print("Could not click suggestion/result elements:", e)
        except Exception as e:
            print("Could not trigger a search interaction:", e)

        # Allow some time for background XHRs to fire and be captured
        timeout_ms = 15000
        interval = 500
        waited = 0
        try:
            while waited < timeout_ms and doc_id is None:
                if page.is_closed():
                    print("Page closed while waiting for network requests.")
                    break
                page.wait_for_timeout(interval)
                waited += interval
        except Exception as e:
            print("Page closed or error while waiting for network events:", e)

        # Final fallback: try HTML-based detection
        if not doc_id:
            m2 = re.search(r'"doc_id"\s*:\s*"(\d{8,})"', html)
            if m2:
                doc_id = m2.group(1)
                print(f"Found doc_id in HTML: {doc_id}")
            else:
                m3 = re.search(r'AdLibrarySearchPaginationQuery[^\d]*(\d{8,})', html)
                if m3:
                    doc_id = m3.group(1)
                    print(f"Found doc_id by query name: {doc_id}")
                else:
                    print("Could not auto-detect doc_id from page source or network requests. You may need to copy it from DevTools.")

        # Analyze network hits to pick the best doc_id if not already captured
        if not doc_id and network_hits:
            # First, prefer any POSTs that mention the exact query friendly name
            preferred = None
            for hit in network_hits:
                post = hit.get("post", "")
                if "AdLibrarySearchPaginationQuery" in post or "AdLibrarySearch" in post:
                    # try to extract doc_id from this preferred post
                    try:
                        from urllib.parse import parse_qs
                        parsed = parse_qs(post)
                        if "doc_id" in parsed and parsed["doc_id"]:
                            preferred = parsed["doc_id"][0]
                            print(f"Found preferred doc_id in preferred POST: {preferred}")
                            break
                    except Exception:
                        pass
                    m_net = re.search(r"doc_id=?(\d{6,30})", post)
                    if m_net:
                        preferred = m_net.group(1)
                        print(f"Found preferred doc_id via regex in preferred POST: {preferred}")
                        break
            if preferred:
                doc_id = preferred
            else:
                # fallback to frequency/scoring approach
                doc_count = {}
                for hit in network_hits:
                    post = hit.get("post", "")
                    # look for explicit doc_id in form-encoded data
                    try:
                        from urllib.parse import parse_qs
                        parsed = parse_qs(post)
                        if "doc_id" in parsed and parsed["doc_id"]:
                            d = parsed["doc_id"][0]
                            doc_count[d] = doc_count.get(d, 0) + 5  # strong signal
                    except Exception:
                        pass
                    # regex search
                    for m in re.finditer(r"doc_id=?(\d{6,30})", post):
                        d = m.group(1)
                        doc_count[d] = doc_count.get(d, 0) + 1
                    # if query name present, boost nearby doc_id choices
                    if "AdLibrarySearchPaginationQuery" in post:
                        for m in re.finditer(r"doc_id=?(\d{6,30})", post):
                            d = m.group(1)
                            doc_count[d] = doc_count.get(d, 0) + 10

                if doc_count:
                    # pick doc_id with highest score
                    chosen = max(doc_count.items(), key=lambda x: x[1])[0]
                    doc_id = chosen
                    print(f"Auto-selected doc_id={doc_id} based on network hits (scores: {doc_count})")

        # Final fallback: use known historical doc_id if none detected
        if not doc_id:
            fallback_doc_id = "25464068859919530"
            doc_id = fallback_doc_id
            print(f"No suitable doc_id detected. Falling back to known doc_id {doc_id}.")

        # If none of the network hits contain the `AdLibrarySearchPaginationQuery` (the main pagination query),
        # prefer the historical fallback doc_id to ensure search pagination works.
        if network_hits and not any("AdLibrarySearchPaginationQuery" in (h.get("post","")) for h in network_hits):
            fallback_doc_id = "25464068859919530"
            if doc_id != fallback_doc_id:
                doc_id = fallback_doc_id
                print(f"No AdLibrarySearchPaginationQuery found in network hits; forcing doc_id to fallback {doc_id} for pagination.")

        data = {
            "cookies": cookies,
            "lsd": lsd,
            "doc_id": doc_id,
            "fetched_at": int(time.time()),
            "network_hits": network_hits
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        # Also save network hits separately for easy inspection
        if network_hits:
            with open("network_log.json", "w", encoding="utf-8") as f2:
                json.dump(network_hits, f2, indent=2)
            print("Saved network hits to network_log.json")

        print(f"Saved tokens to {output_file} (cookies: {len(cookies)} entries, doc_id={doc_id})")

        browser.close()

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch cookies/lsd/doc_id for Meta Ads Library using Playwright")
    parser.add_argument("--output", "-o", default="cookies.json", help="Output JSON file")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Run browser in headed mode")
    parser.add_argument("--url", dest="url", default=None, help="Ads Library URL to visit")
    parser.add_argument("--page-id", dest="page_id", default=None, help="Facebook page ID to fetch cookies for")
    args = parser.parse_args()

    fetch_and_save_cookies(output_file=args.output, headless=args.headless, url=args.url, page_id=args.page_id)

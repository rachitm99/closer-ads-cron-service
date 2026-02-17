from flask import Flask, request, jsonify
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
from google.cloud import tasks_v2
from google.cloud import secretmanager
from google.cloud import firestore
import os
import json
import requests
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# Local development mode - skips authentication
LOCAL_DEV = os.environ.get('LOCAL_DEV', '').lower() in ('true', '1', 'yes')

EXPECT_AUDIENCES = [s.strip() for s in os.environ.get('EXPECT_AUDIENCES', '').split(',') if s.strip()]
PROJECT = os.environ.get('GCP_PROJECT', 'closer-video-similarity')
RAPIDAPI_KEY_SECRET = os.environ.get('RAPIDAPI_KEY_SECRET', 'rapidapi-key')
CLOUD_TASKS_QUEUE = os.environ.get('CLOUD_TASKS_QUEUE', 'face-processing-queue-2')
CLOUD_TASKS_LOCATION = os.environ.get('CLOUD_TASKS_LOCATION', 'us-central1')
WORKER_URL = os.environ.get('WORKER_URL', 'https://face-processor-810614481902.us-central1.run.app/task')
TASKS_INVOKER_SA = os.environ.get('TASKS_INVOKER_SA', 'tasks-invoker@closer-video-similarity.iam.gserviceaccount.com')
API_URL = "https://facebook-ads-library-scraper-api.p.rapidapi.com/company/ads"
API_HOST = "facebook-ads-library-scraper-api.p.rapidapi.com"

# Initialize Google Cloud clients (optional - will be None if not available)
try:
    secret_client = secretmanager.SecretManagerServiceClient()
    print("✓ Secret Manager client initialized")
except Exception as e:
    secret_client = None
    print(f"✗ Secret Manager not available: {e}")

try:
    tasks_client = tasks_v2.CloudTasksClient()
    print("✓ Cloud Tasks client initialized")
except Exception as e:
    tasks_client = None
    print(f"✗ Cloud Tasks not available: {e}")

try:
    db = firestore.Client()
    print("✓ Firestore client initialized")
except Exception as e:
    db = None
    print(f"✗ Firestore not available: {e}")

def verify_id_token(token: str):
    if not token:
        raise ValueError('no-token')

    for aud in EXPECT_AUDIENCES:
        try:
            payload = id_token.verify_oauth2_token(token, google_requests.Request(), aud)
            return payload
        except Exception:
            # try next audience
            pass

    raise ValueError('invalid-token')

def get_rapidapi_key():
    """Fetch RapidAPI key from Secret Manager with environment variable fallback"""
    # Try environment variable first on Render/non-GCP environments
    if secret_client is None:
        app.logger.info("Secret Manager not available, using RAPIDAPI_KEY env var")
        api_key = os.environ.get('RAPIDAPI_KEY', '').strip()
        if not api_key:
            raise RuntimeError("RapidAPI key not found - set RAPIDAPI_KEY env var")
        return api_key
    
    # Try Secret Manager (GCP)
    try:
        secret_name = f"projects/{PROJECT}/secrets/{RAPIDAPI_KEY_SECRET}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        api_key = response.payload.data.decode('UTF-8').strip('\ufeff').strip()
        app.logger.info("RapidAPI key fetched from Secret Manager")
        return api_key
    except Exception as e:
        app.logger.warning(f"Secret Manager fetch failed: {e}, falling back to RAPIDAPI_KEY env var")
        api_key = os.environ.get('RAPIDAPI_KEY', '').strip()
        if not api_key:
            raise RuntimeError("RapidAPI key not found in Secret Manager or RAPIDAPI_KEY env var")
        app.logger.info("RapidAPI key fetched from environment variable")
        return api_key

def fetch_ads_with_date_filter_rapidapi(api_key, page_id, cutoff_timestamp):
    """Fetch ads from RapidAPI with consecutive all-old-pages mechanism.
    
    Continues fetching until 2 consecutive API calls contain ads where ALL ads
    have start_date before the cutoff_timestamp.
    
    Args:
        api_key: RapidAPI key
        page_id: Facebook page ID to fetch ads for
        cutoff_timestamp: Unix timestamp (epoch seconds) - only fetch ads newer than this
    
    Returns:
        List[dict]: list of ad dicts
    """
    headers = {
        "x-rapidapi-host": API_HOST,
        "x-rapidapi-key": api_key,
    }
    
    base_params = {
        "status": "ACTIVE",
        "trim": "false",
        "country": "IN",
        "media_type": "VIDEO",
        "pageId": page_id,
    }
    
    all_ads = []
    seen_ad_ids = set()
    cursor = None
    page_num = 0
    consecutive_all_old_pages = 0
    total_duplicates = 0
    total_old_ads = 0
    
    while True:
        page_num += 1
        params = dict(base_params)
        if cursor:
            params["cursor"] = cursor
        
        app.logger.info(f"[RapidAPI] Fetching page {page_num} with cursor={cursor}")
        
        try:
            resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            app.logger.error(f"[RapidAPI] API request failed: {e}")
            break
        
        results = data.get("results") or []
        app.logger.info(f"[RapidAPI] Page {page_num} returned {len(results)} ads from API")
        
        if not results:
            break
        
        relevant_ads_in_page = []
        old_ads_in_page = 0
        skipped_in_page = 0
        
        for item in results:
            extracted = extract_ad_item(item)
            ad_id = extracted.get("ad_archive_id")
            
            # Skip ads without video URL (neither HD nor SD)
            if not extracted.get("video_hd_url"):
                app.logger.debug(f"[RapidAPI] Skipping ad {ad_id} - no video URL")
                skipped_in_page += 1
                continue
            
            if ad_id and ad_id in seen_ad_ids:
                total_duplicates += 1
                skipped_in_page += 1
                continue
            
            if ad_id:
                seen_ad_ids.add(ad_id)
            
            start_date = extracted.get("start_date")
            
            if start_date and start_date < cutoff_timestamp:
                old_ads_in_page += 1
                total_old_ads += 1
            else:
                relevant_ads_in_page.append(extracted)
        
        non_duplicate_ads_in_page = len(results) - skipped_in_page
        all_ads_old_in_page = (non_duplicate_ads_in_page > 0 and old_ads_in_page == non_duplicate_ads_in_page)
        
        app.logger.info(f"[RapidAPI] Page {page_num}: {len(relevant_ads_in_page)} new ads, {old_ads_in_page} old ads, {skipped_in_page} duplicates, all_old={all_ads_old_in_page}")
        
        all_ads.extend(relevant_ads_in_page)
        
        if all_ads_old_in_page:
            consecutive_all_old_pages += 1
            app.logger.info(f"[RapidAPI] Page {page_num} has ALL old ads. Consecutive count: {consecutive_all_old_pages}/2")
            
            if consecutive_all_old_pages >= 2:
                app.logger.info(f"[RapidAPI] Found 2 consecutive pages with ALL old ads, stopping pagination")
                break
        else:
            if consecutive_all_old_pages > 0:
                app.logger.info(f"[RapidAPI] Found new ads, resetting consecutive all-old-pages counter from {consecutive_all_old_pages} to 0")
            consecutive_all_old_pages = 0
        
        cursor = data.get("cursor")
        if not cursor:
            app.logger.info("[RapidAPI] No more cursor, ending pagination")
            break
    
    app.logger.info(f"[RapidAPI] Final stats: {len(all_ads)} new ads, {total_old_ads} old ads, {total_duplicates} duplicates")
    return all_ads

def extract_ad_item(item):
    """Extract relevant fields from API response item"""
    ad_archive_id = item.get("ad_archive_id")
    page_id = item.get("page_id")
    
    # Extract video URL from snapshot
    video_hd_url = None
    snapshot = item.get("snapshot") or {}
    videos = snapshot.get("videos") or []
    if videos:
        video_hd_url = videos[0].get("video_hd_url") or videos[0].get("video_sd_url")
    
    # Check cards if no video found
    if not video_hd_url:
        cards = snapshot.get("cards") or []
        for c in cards:
            if c.get("video_hd_url"):
                video_hd_url = c.get("video_hd_url")
                break
            if c.get("video_sd_url"):
                video_hd_url = c.get("video_sd_url")
                break
    
    page_name = item.get("page_name") or snapshot.get("page_name")
    start_date = item.get("start_date")
    end_date = item.get("end_date")
    ad_url = item.get("url")
    
    return {
        "ad_archive_id": ad_archive_id,
        "page_id": page_id,
        "video_hd_url": video_hd_url,
        "page_name": page_name,
        "start_date": start_date,
        "end_date": end_date,
        "ad_url": ad_url,
    }

def fetch_ads_with_date_filter(page_id, cutoff_timestamp):
    """
    Delegate ad fetching to an external ad-fetcher service.

    The application expects an importable module named `ad_fetcher` that
    implements:

        fetch_ads_for_page(page_id: str, cutoff_timestamp: int) -> List[dict]

    The function should return a list of ad dicts (same shape as the output
    of `extract_ad_item`). Keeping the fetch logic out of `app.py` keeps this
    service decoupled and easier to test/deploy.

    Args:
        page_id: Facebook page ID to fetch ads for
        cutoff_timestamp: Unix timestamp (epoch seconds) - only fetch ads newer than this

    Returns:
        List[dict]: list of ad dicts
    """
    app.logger.info(f"Delegating ad fetch for page_id={page_id} to external ad_fetcher service")
    try:
        # External dependency (should be provided separately)
        from ad_fetcher import fetch_ads_for_page
    except Exception as exc:
        app.logger.error("ad_fetcher.fetch_ads_for_page not available: %s", exc)
        raise RuntimeError(
            "ad_fetcher service not configured. Provide an 'ad_fetcher' module with 'fetch_ads_for_page(page_id, cutoff_timestamp)'") from exc

    result = fetch_ads_for_page(page_id=page_id, cutoff_timestamp=cutoff_timestamp)
    if isinstance(result, tuple) and len(result) == 3:
        ads, logs, raw_responses = result
    elif isinstance(result, tuple) and len(result) == 2:
        ads, logs = result
        raw_responses = []
    else:
        ads = result
        logs = []
        raw_responses = []
    
    if not isinstance(ads, list):
        raise RuntimeError("ad_fetcher.fetch_ads_for_page must return a list of ad dicts")
    
    # Check if rate limited - if no ads and rate limit detected in logs
    is_rate_limited = len(ads) == 0 and any("rate limit" in log.lower() or "1675004" in log for log in logs)
    
    if is_rate_limited:
        app.logger.info(f"🔄 Internal API rate limited for page {page_id}, falling back to RapidAPI")
        try:
            api_key = get_rapidapi_key()
            ads = fetch_ads_with_date_filter_rapidapi(api_key, page_id, cutoff_timestamp)
            logs.append(f"✅ Fallback to RapidAPI successful - fetched {len(ads)} ads")
            app.logger.info(f"✅ RapidAPI fallback successful - fetched {len(ads)} ads for page {page_id}")
        except Exception as e:
            app.logger.error(f"❌ RapidAPI fallback failed: {e}")
            logs.append(f"❌ RapidAPI fallback failed: {str(e)}")
    
    return ads, logs, raw_responses


def generate_cookies(output_file: str = "cookies.json", headless: bool = True):
    """Use the Playwright helper to generate cookies/lsd/doc_id and save them.

    Raises RuntimeError on failure so callers can decide how to proceed.
    """
    app.logger.info("Generating cookies using fetch_cookies_playwright helper")
    try:
        from fetch_cookies_playwright import fetch_and_save_cookies
    except Exception as exc:
        app.logger.exception("fetch_cookies_playwright is not available: %s", exc)
        raise RuntimeError("fetch_cookies_playwright helper not available") from exc

    try:
        data = fetch_and_save_cookies(output_file=output_file, headless=headless)
        app.logger.info("Cookie generation succeeded; saved tokens to %s", output_file)
        return data
    except Exception as exc:
        app.logger.exception("Cookie generation failed: %s", exc)
        raise RuntimeError("cookie generation failed") from exc

def create_cloud_task(ad_data):
    """Create a Cloud Task for processing an ad (skips if Cloud Tasks not available)"""
    # Skip if Cloud Tasks client not available (e.g., on Render)
    if tasks_client is None:
        app.logger.debug(f"Cloud Tasks not available - skipping task creation for ad {ad_data.get('ad_archive_id')}")
        return None
    
    # Map to worker payload format
    video_url = ad_data.get('video_hd_url')
    ad_id = str(ad_data.get('ad_archive_id')) if ad_data.get('ad_archive_id') else None
    company_name = ad_data.get('page_name')
    page_id = ad_data.get('page_id')
    ad_url = ad_data.get('ad_url')
    
    # Validate required fields
    if not all([video_url, ad_id, company_name, page_id, ad_url]):
        missing_fields = []
        if not video_url: missing_fields.append('video_url')
        if not ad_id: missing_fields.append('ad_id')
        if not company_name: missing_fields.append('company_name')
        if not page_id: missing_fields.append('page_id')
        if not ad_url: missing_fields.append('ad_url')
        app.logger.warning(f"Skipping ad {ad_id or 'UNKNOWN'} - missing required fields: {missing_fields}")
        return None
    
    payload = {
        'video_url': video_url,
        'ad_id': ad_id,
        'company_name': company_name,
        'page_id': page_id,
        'ad_url': ad_url,
        'start_date': ad_data.get('start_date'),
        'end_date': ad_data.get('end_date'),
    }
    
    body_bytes = json.dumps(payload).encode('utf-8')
    parent = tasks_client.queue_path(PROJECT, CLOUD_TASKS_LOCATION, CLOUD_TASKS_QUEUE)
    
    # Use ad_id with timestamp for task name to avoid 409 "existed too recently" errors
    # Cloud Tasks prevents reusing same name within ~1 hour
    import time
    timestamp_ms = int(time.time() * 1000)
    task_name = f"{parent}/tasks/ad-{ad_id}-{timestamp_ms}"
    
    task = {
        'name': task_name,
        'http_request': {
            'http_method': tasks_v2.HttpMethod.POST,
            'url': WORKER_URL,
            'headers': {'Content-Type': 'application/json'},
            'body': body_bytes,
            'oidc_token': {'service_account_email': TASKS_INVOKER_SA}
        }
    }
    
    try:
        response = tasks_client.create_task(parent=parent, task=task)
        app.logger.info(f"✅ Created task for ad {ad_id}: {response.name}")
        return response
    except Exception as e:
        # If task already exists (AlreadyExists error), skip silently
        if 'AlreadyExists' in str(e) or 'already exists' in str(e).lower():
            app.logger.info(f"⏭️ Task for ad {ad_id} already exists, skipping (duplicate)")
            return None
        app.logger.error(f"❌ Failed to create task for ad {ad_id}: {e}")
        return None

@app.route('/_healthz', methods=['GET'])
def healthz():
    """Lightweight, unauthenticated health endpoint for container HEALTHCHECKs."""
    return 'ok', 200


@app.route('/health', methods=['GET'])
def health():
    """Authenticated health endpoint used by external monitors (keeps existing behavior)."""
    if not LOCAL_DEV:
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'missing-token'}), 401

        token = auth_header.split(' ', 1)[1]
        try:
            verify_id_token(token)
        except ValueError as e:
            return jsonify({'error': str(e)}), 401
    
    return 'ok', 200

@app.route('/run', methods=['POST'])
def run_job():
    if not LOCAL_DEV:
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'missing-token'}), 401

        token = auth_header.split(' ', 1)[1]
        try:
            verify_id_token(token)
        except ValueError as e:
            return jsonify({'error': str(e)}), 401

    try:
        # Use external ad fetcher service (no RapidAPI call from this module)
        app.logger.info("Using external ad_fetcher service (no RapidAPI key required)")
        
        # Generate cookies before fetching brands
        app.logger.info("=" * 60)
        app.logger.info("STARTING CRON JOB: Meta Ads Fetcher")
        app.logger.info("=" * 60)
        
        try:
            app.logger.info("Step 1: Generating fresh cookies via Playwright...")
            generate_cookies(output_file=os.environ.get('COOKIES_OUTPUT', 'cookies.json'),
                             headless=(os.environ.get('FETCH_COOKIES_HEADLESS','true').lower() != 'false'))
            app.logger.info("✓ Cookie generation completed successfully")
        except Exception as exc:
            app.logger.error('✗ Failed to generate cookies before brand loop', exc_info=True)
            # Don't return - continue with existing cookies if available
            app.logger.warning('Continuing with existing cookies (if available)...')

        # Fetch all brands from Firestore (single query to minimize operations)
        if db is None:
            app.logger.error("Firestore not available - /run endpoint requires GCP Firestore")
            return jsonify({'error': 'Firestore not configured - this endpoint requires GCP deployment'}), 503
        
        app.logger.info("Fetching brands from Firestore")
        brands_ref = db.collection('brands')
        brands_list = brands_ref.get(timeout=120.0)  # Use get() instead of stream() - fetches all at once
        app.logger.info(f"Successfully fetched {len(brands_list)} brands from Firestore")
        
        total_ads_fetched = 0
        total_tasks_created = 0
        total_tasks_skipped = 0
        brands_processed = 0
        brands_failed = 0
        
        for brand_doc in brands_list:
            try:
                brand_data = brand_doc.to_dict()
                brand_id = brand_doc.id
                page_id = brand_data.get('page_id') or brand_data.get('pageId')
                
                if not page_id:
                    app.logger.warning(f"Brand {brand_id} has no page_id, skipping")
                    continue
                
                brands_processed += 1
                app.logger.info(f"\n{'='*60}")
                app.logger.info(f"Processing brand {brands_processed}: {brand_id} (page_id={page_id})")
                app.logger.info(f"{'='*60}")
                
                # Get lastFetchedEpoch or use 6 months default
                last_fetched_epoch = brand_data.get('lastFetchedEpoch')
                if last_fetched_epoch:
                    cutoff_timestamp = last_fetched_epoch
                    app.logger.info(f"Brand {brand_id} (page_id={page_id}): Using lastFetchedEpoch={last_fetched_epoch}")
                else:
                    # Default to 6 months ago
                    cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)
                    cutoff_timestamp = int(cutoff_date.timestamp())
                    app.logger.info(f"Brand {brand_id} (page_id={page_id}): No lastFetchedEpoch, using 6 months default ({cutoff_timestamp})")
            
                # Fetch ads for this brand via external fetcher
                app.logger.info(f"Fetching ads with cutoff_timestamp={cutoff_timestamp}")
                ads, fetch_logs, _ = fetch_ads_with_date_filter(page_id, cutoff_timestamp)
                
                # Log fetcher output for this brand
                for log_line in fetch_logs:
                    app.logger.info(f"  {log_line}")
                
                app.logger.info(f"✓ Fetched {len(ads)} unique ads")
                total_ads_fetched += len(ads)
                
                # Update lastFetched and lastFetchedEpoch BEFORE creating tasks to prevent re-processing on retry
                try:
                    now = firestore.SERVER_TIMESTAMP
                    now_epoch = int(datetime.now(timezone.utc).timestamp())
                    brand_doc.reference.update({
                        'lastFetched': now,
                        'lastFetchedEpoch': now_epoch
                    })
                    app.logger.info(f"✓ Updated Firestore with lastFetchedEpoch={now_epoch}")
                except Exception as update_exc:
                    app.logger.warning(f"Failed to update Firestore for brand {brand_id}: {update_exc}")
                
                # Create Cloud Tasks for each ad
                brand_tasks_created = 0
                brand_tasks_skipped = 0
                app.logger.info(f"📋 Creating tasks for {len(ads)} ads...")
                for ad in ads:
                    result = create_cloud_task(ad)
                    if result:
                        total_tasks_created += 1
                        brand_tasks_created += 1
                    else:
                        total_tasks_skipped += 1
                        brand_tasks_skipped += 1
                
                app.logger.info(f"✓ Brand {brand_id} complete: {brand_tasks_created} tasks created, {brand_tasks_skipped} skipped")
                
            except Exception as brand_exc:
                brands_failed += 1
                app.logger.error(f"✗ Brand {brand_id} failed: {brand_exc}", exc_info=True)
                continue
        
        # Final summary
        app.logger.info("\n" + "=" * 60)
        app.logger.info("CRON JOB COMPLETED")
        app.logger.info("=" * 60)
        app.logger.info(f"Brands processed: {brands_processed}")
        app.logger.info(f"Brands failed: {brands_failed}")
        app.logger.info(f"Total ads fetched: {total_ads_fetched}")
        app.logger.info(f"Tasks created: {total_tasks_created}")
        app.logger.info(f"Tasks skipped: {total_tasks_skipped}")
        app.logger.info("=" * 60)
        
        return jsonify({
            'status': 'ok',
            'brands_processed': brands_processed,
            'brands_failed': brands_failed,
            'ads_fetched': total_ads_fetched,
            'tasks_created': total_tasks_created,
            'tasks_skipped': total_tasks_skipped
        }), 200
    
    except Exception as exc:
        app.logger.exception('run error')
        return jsonify({'error': str(exc)}), 500

@app.route('/run-page', methods=['POST'])
def run_page():
    if not LOCAL_DEV:
        auth_header = request.headers.get('Authorization', '')
        app.logger.info(f"Authorization header present: {bool(auth_header)}, starts with Bearer: {auth_header.startswith('Bearer ')}")
        
        if not auth_header.startswith('Bearer '):
            app.logger.warning(f"Missing or invalid Authorization header: {auth_header[:50] if auth_header else 'None'}")
            return jsonify({'error': 'missing-token'}), 401

        token = auth_header.split(' ', 1)[1]
        try:
            verify_id_token(token)
            app.logger.info("Token verification successful")
        except ValueError as e:
            app.logger.error(f"Token verification failed: {str(e)}")
            return jsonify({'error': str(e)}), 401

    try:
        # Get page_id from request body
        data = request.get_json()
        if not data or 'page_id' not in data:
            return jsonify({'error': 'page_id required in request body'}), 400
        
        target_page_id = str(data['page_id'])
        app.logger.info(f"Processing single page_id: {target_page_id}")
        
        # Use external ad fetcher service (no RapidAPI key required)
        app.logger.info("Using external ad_fetcher service (no RapidAPI key required)")
        
        # Handle Firestore operations if available, otherwise use defaults
        brand_doc = None
        brand_data = None
        brand_id = None
        cutoff_timestamp = None
        
        if db is not None:
            # Find brand with matching page_id
            app.logger.info(f"Searching for brand with page_id={target_page_id}")
            brands_ref = db.collection('brands')
            
            # Try both field names (page_id and pageId)
            # Query by page_id field
            brands_query = brands_ref.where('page_id', '==', target_page_id).limit(1).stream()
            for doc in brands_query:
                brand_doc = doc
                brand_data = doc.to_dict()
                brand_id = doc.id
                break
            
            # If not found, try pageId field
            if not brand_doc:
                brands_query = brands_ref.where('pageId', '==', target_page_id).limit(1).stream()
                for doc in brands_query:
                    brand_doc = doc
                    brand_data = doc.to_dict()
                    brand_id = doc.id
                    break
            
            if brand_doc and brand_data:
                app.logger.info(f"Found brand {brand_id} with page_id={target_page_id}")
                # Get lastFetchedEpoch or use 6 months default
                last_fetched_epoch = brand_data.get('lastFetchedEpoch')
                if last_fetched_epoch:
                    cutoff_timestamp = last_fetched_epoch
                    app.logger.info(f"Brand {brand_id} (page_id={target_page_id}): Using lastFetchedEpoch={last_fetched_epoch}")
            else:
                app.logger.warning(f"No brand found with page_id={target_page_id} in Firestore")
        else:
            app.logger.info("Firestore not available - using default cutoff")
        
        # Use 6 months default if no cutoff found
        if cutoff_timestamp is None:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)
            cutoff_timestamp = int(cutoff_date.timestamp())
            app.logger.info(f"Using 6 months default cutoff ({cutoff_timestamp})")
        
        # Fetch ads for this brand via external fetcher
        app.logger.info(f"Fetching ads for brand {brand_id} (page_id={target_page_id}) with cutoff_timestamp={cutoff_timestamp}")
        ads, fetch_logs, raw_responses = fetch_ads_with_date_filter(target_page_id, cutoff_timestamp)
        
        app.logger.info(f"Brand {brand_id}: Fetched {len(ads)} unique ads (duplicates removed)")
        
        # Save full raw API responses to JSON file
        if raw_responses:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"raw_api_responses_{target_page_id}_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(raw_responses, f, indent=2, ensure_ascii=False)
            app.logger.info(f"✅ Saved {len(raw_responses)} raw API responses to {filename}")
        
        # Update lastFetched and lastFetchedEpoch BEFORE creating tasks (if Firestore available)
        if db is not None and brand_doc is not None:
            now = firestore.SERVER_TIMESTAMP
            now_epoch = int(datetime.now(timezone.utc).timestamp())
            brand_doc.reference.update({
                'lastFetched': now,
                'lastFetchedEpoch': now_epoch
            })
            app.logger.info(f"Updated brand {brand_id} with lastFetched and lastFetchedEpoch={now_epoch} BEFORE task creation")
        else:
            app.logger.info("Skipping Firestore update (not available or brand not found)")
        
        # Create Cloud Tasks for each ad
        tasks_created = 0
        tasks_skipped = 0
        app.logger.info(f"📋 Creating tasks for {len(ads)} ads...")
        for ad in ads:
            result = create_cloud_task(ad)
            if result:
                tasks_created += 1
            else:
                tasks_skipped += 1
        
        app.logger.info(f"✅ Task creation complete: {tasks_created} created, {tasks_skipped} skipped")
        
        return jsonify({
            'status': 'ok',
            'brand_id': brand_id,
            'page_id': target_page_id,
            'ads_fetched': len(ads),
            'tasks_created': tasks_created,
            'tasks_skipped': tasks_skipped,
            'logs': fetch_logs,
            'raw_responses': raw_responses
        }), 200
    
    except Exception as exc:
        app.logger.exception('run-page error')
        return jsonify({'error': str(exc)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

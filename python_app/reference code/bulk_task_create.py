#!/usr/bin/env python3
"""Batch-create Cloud Tasks from an ads list JSON file.

The input JSON should be a list of objects with keys similar to the example you provided:
- ad_archive_id (maps -> ad_id)
- page_id
- page_name (maps -> company_name)
- video_hd_url (maps -> video_url)
- start_date, end_date (epoch seconds)
- optional ad_url (if not present we try to construct a fallback)

Usage:
  python scripts/bulk_create_tasks.py \
    --project closer-video-similarity --location us-central1 --queue face-processing-queue-2 \
    --url https://face-processor-.../task --service-account tasks-invoker@closer-video-similarity.iam.gserviceaccount.com \
    --ads-file /path/to/ads_list.json

Options:
- --dry-run: print payloads but don't call Cloud Tasks API
- --start/--end: process slice of the list (0-based indices)
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

from google.cloud import tasks_v2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('bulk_create_tasks')

# ---------------------------------------------------------------------------
# Hardcoded configuration — edit to avoid passing CLI args every time
# NOTE: For security, avoid committing sensitive values (service account keys, etc.)
HARDCODED_CONFIG = {
    "PROJECT": "closer-video-similarity",
    "LOCATION": "us-central1",
    "QUEUE": "face-processing-queue-2",
    "URL": "https://face-processor-810614481902.us-central1.run.app/task",
    "SERVICE_ACCOUNT": "tasks-invoker@closer-video-similarity.iam.gserviceaccount.com",
    "ADS_FILE": "ads_list.json",
    "DRY_RUN": False,
    "START": 0,
    "END": None,
}
# ---------------------------------------------------------------------------


def map_item_to_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    # Map source keys to worker payload keys
    video_url = item.get('video_hd_url') or item.get('video_url') or item.get('video')
    ad_id = item.get('ad_archive_id') or item.get('ad_id') or item.get('archive_id')
    company_name = item.get('page_name') or item.get('company_name') or item.get('page_name')
    page_id = item.get('page_id') or item.get('page')
    ad_url = item.get('ad_url')

    # Construct fallback ad_url if none provided
    if not ad_url:
        if page_id and ad_id:
            ad_url = f'https://www.facebook.com/{page_id}/posts/{ad_id}'
        elif company_name and ad_id:
            # sanitize company_name for url
            name_safe = ''.join(c for c in company_name if c.isalnum() or c in ('_', '-')).strip()
            if name_safe:
                ad_url = f'https://www.facebook.com/{name_safe}/posts/{ad_id}'

    payload = {
        'video_url': video_url,
        'ad_id': str(ad_id) if ad_id is not None else None,
        'company_name': company_name,
        'page_id': page_id,
        'ad_url': ad_url,
        'start_date': item.get('start_date'),
        'end_date': item.get('end_date'),
    }

    return payload


def create_task_for_payload(client: tasks_v2.CloudTasksClient, parent: str, url: str, service_account_email: str, payload: Dict[str, Any]):
    body_bytes = json.dumps(payload).encode('utf-8')

    task = {
        'http_request': {
            'http_method': tasks_v2.HttpMethod.POST,
            'url': url,
            'headers': {'Content-Type': 'application/json'},
            'body': body_bytes,
            'oidc_token': {'service_account_email': service_account_email}
        }
    }
    return client.create_task(parent=parent, task=task)


def main():
    p = argparse.ArgumentParser(description='Bulk-create Cloud Tasks from an ads list JSON file')
    p.add_argument('--project', default=HARDCODED_CONFIG['PROJECT'], help='GCP project (default from HARDCODED_CONFIG)')
    p.add_argument('--location', default=HARDCODED_CONFIG['LOCATION'], help='GCP location/region (default from HARDCODED_CONFIG)')
    p.add_argument('--queue', default=HARDCODED_CONFIG['QUEUE'], help='Cloud Tasks queue (default from HARDCODED_CONFIG)')
    p.add_argument('--url', default=HARDCODED_CONFIG['URL'], help='Worker URL to call (default from HARDCODED_CONFIG)')
    p.add_argument('--service-account', default=HARDCODED_CONFIG['SERVICE_ACCOUNT'], help='Service account email for OIDC token (default from HARDCODED_CONFIG)')
    p.add_argument('--ads-file', default=HARDCODED_CONFIG['ADS_FILE'], help='Path to JSON file containing list of ads (default from HARDCODED_CONFIG)')
    p.add_argument('--dry-run', action='store_true', default=HARDCODED_CONFIG['DRY_RUN'])
    p.add_argument('--start', type=int, default=HARDCODED_CONFIG['START'], help='Start index (inclusive)')
    p.add_argument('--end', type=int, default=HARDCODED_CONFIG['END'], help='End index (exclusive)')

    args = p.parse_args()

    ads_path = Path(args.ads_file)
    if not ads_path.exists():
        logger.error('ads file not found: %s', ads_path)
        sys.exit(1)

    with ads_path.open('r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        logger.error('ads file must be a JSON list of objects')
        sys.exit(1)

    subset = data[args.start:args.end]

    client = None
    parent = None
    if not args.dry_run:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(args.project, args.location, args.queue)

    # log when using hardcoded defaults (helpful when running without args)
    logger.info('Using project=%s location=%s queue=%s url=%s ads_file=%s', args.project, args.location, args.queue, args.url, args.ads_file)

    success = 0
    skipped = 0
    failed = 0

    for i, item in enumerate(subset, start=args.start):
        payload = map_item_to_payload(item)

        # Validate required fields for worker
        missing = [k for k in ('video_url', 'ad_id', 'company_name', 'page_id', 'ad_url', 'start_date', 'end_date') if not payload.get(k)]
        if missing:
            logger.warning('Skipping item index=%d missing fields=%s: item=%s', i, missing, item)
            skipped += 1
            continue

        logger.info('Enqueuing item index=%d ad_id=%s', i, payload['ad_id'])

        if args.dry_run:
            print(json.dumps(payload, indent=2))
            success += 1
            continue

        try:
            resp = create_task_for_payload(client, parent, args.url, args.service_account, payload)
            logger.info('Created task: %s', resp.name)
            success += 1
        except Exception as e:
            logger.exception('Failed to create task for ad_id=%s', payload.get('ad_id'))
            failed += 1

    logger.info('Done. created=%d skipped=%d failed=%d', success, skipped, failed)


if __name__ == '__main__':
    main()

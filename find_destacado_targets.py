#!/usr/bin/env python3
"""
Find occurrences of legacy `destacado-websked*` content-sources in PageBuilder pages.

Usage examples:
  # If you already exported pages JSON to pb-export/pages-export.json
  python3 find_destacado_targets.py --pages pb-export/pages-export.json

  # Or let the script download pages via API (replace BASE_URL and TOKEN)
  python3 find_destacado_targets.py --download --base-url "https://your-host" --token "<TOKEN>"

Outputs:
  pb-export/targets.txt            # list of target names read from content/sources/*.js
  pb-export/pages-export.json      # downloaded pages JSON (if --download)
  pb-export/destacado-matches.csv  # CSV with matches: page_id,page_name,block_index,content_source,collection_id,params_json

Notes:
- The script derives target content_source names from filenames in content/sources that start with "destacado-websked".
- It matches block.content_source exactly against those names.
- It tolerantly handles several common shapes of PageBuilder JSON.
"""

import argparse
import csv
import glob
import json
import os
import sys
from urllib.parse import urljoin
from dotenv import load_dotenv

try:
    # preferred for nicer TLS/etc
    import requests  # optional; fallback to urllib if not installed
except Exception:
    requests = None

# Load environment variables from .env (if present)
load_dotenv()


def gather_targets(sources_dir):
    """Find files named destacado-websked*.js under the given sources_dir.

    sources_dir can be the repo root (then we look under content/sources) or an explicit
    directory that already points to content/sources.
    """
    # support passing repo root or the direct sources dir
    if os.path.isdir(os.path.join(sources_dir, 'content', 'sources')):
        base = os.path.join(sources_dir, 'content', 'sources')
    else:
        base = sources_dir
    pattern = os.path.join(base, 'destacado-websked*.js')
    files = sorted(glob.glob(pattern))
    targets = []
    for f in files:
        name = os.path.basename(f)
        target = os.path.splitext(name)[0]
        targets.append(target)
    return targets


def download_pages(base_url, token, endpoint='/pagebuilder/api/pages?limit=1000'):
    url = urljoin(base_url.rstrip('/') + '/', endpoint.lstrip('/'))
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    print(f'Downloading pages from: {url}')
    if requests:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.text
    else:
        # fallback to urllib
        import urllib.request
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as resp:
            return resp.read().decode('utf8')


def load_json_file(path):
    with open(path, 'r', encoding='utf8') as fh:
        return json.load(fh)


def ensure_out_dir(out_dir):
    os.makedirs(out_dir, exist_ok=True)


def extract_pages_container(data):
    # Try common shapes
    if isinstance(data, dict):
        # many exports have {"pages": [...]} or {"data": {"pages": [...]}} or {"data": [...]}
        if 'pages' in data and isinstance(data['pages'], list):
            return data['pages']
        if 'data' in data:
            if isinstance(data['data'], dict) and 'pages' in data['data']:
                return data['data']['pages']
            if isinstance(data['data'], list):
                return data['data']
    if isinstance(data, list):
        return data
    # unknown shape: try to find a list inside
    for v in data.values() if isinstance(data, dict) else []:
        if isinstance(v, list):
            return v
    # fallback
    return []


def flatten_blocks(blocks_obj):
    """Return a flat list of blocks given a page['content'] which could be a list or dict of regions."""
    if blocks_obj is None:
        return []
    if isinstance(blocks_obj, list):
        return blocks_obj
    if isinstance(blocks_obj, dict):
        out = []
        for v in blocks_obj.values():
            if isinstance(v, list):
                out.extend(v)
            elif isinstance(v, dict):
                # sometimes regions has nested objects
                for vv in v.values():
                    if isinstance(vv, list):
                        out.extend(vv)
        return out
    return []


def find_matches(pages, targets):
    targets_set = set(targets)
    rows = []
    for p in pages:
        page_id = p.get('_id') or p.get('id') or ''
        page_name = p.get('name') or p.get('title') or ''
        content = p.get('content') or p.get('regions') or []
        blocks = flatten_blocks(content)
        for idx, b in enumerate(blocks):
            if not isinstance(b, dict):
                continue
            cs = (b.get('content_source') or '')
            if cs in targets_set:
                params = b.get('content_source_params') or {}
                collection_id = b.get('collection_id') or params.get('collection_id') or ''
                rows.append((page_id, page_name, idx, cs, collection_id, params))
    return rows


def write_csv(rows, out_path):
    with open(out_path, 'w', encoding='utf8', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(['page_id', 'page_name', 'block_index', 'content_source', 'collection_id', 'params_json'])
        for (page_id, page_name, idx, cs, coll, params) in rows:
            params_json = json.dumps(params, ensure_ascii=False)
            writer.writerow([page_id, page_name, idx, cs, coll, params_json])


def main():
    parser = argparse.ArgumentParser(description='Find pages using legacy destacado-websked content sources')
    parser.add_argument('--pages', help='Path to pages-export.json (if not provided, use --download)')
    parser.add_argument('--download', action='store_true', help='Download pages from PageBuilder API using --base-url and --token')
    parser.add_argument('--base-url', help='Base URL for PageBuilder API (e.g. https://site.arcpublishing.com)')
    parser.add_argument('--token', help='Bearer token for PageBuilder API')
    parser.add_argument('--endpoint', default='/pagebuilder/api/pages?limit=1000', help='Pages endpoint path (default: /pagebuilder/api/pages?limit=1000)')
    parser.add_argument('--out-dir', default='pb-export', help='Output directory')
    parser.add_argument('--sources-dir', help='Path to content/sources directory (defaults to repo_root/content/sources)')
    args = parser.parse_args()

    repo_root = os.getcwd()
    out_dir = args.out_dir
    sources_dir = args.sources_dir or repo_root
    ensure_out_dir(out_dir)

    # gather targets from content/sources (allow overriding location with --sources-dir)
    targets = gather_targets(sources_dir)
    with open(os.path.join(out_dir, 'targets.txt'), 'w', encoding='utf8') as fh:
        fh.write('\n'.join(targets))
    print(f'Found {len(targets)} target(s) from content/sources:')
    for t in targets:
        print('  -', t)

    # allow providing base url / token via env variables: PAGEBUILDER_BASE_URL and PAGEBUILDER_TOKEN
    base_url = args.base_url or os.getenv('PAGEBUILDER_BASE_URL')
    token = args.token or os.getenv('PAGEBUILDER_TOKEN')

    pages_json_path = args.pages or os.path.join(out_dir, 'pages-export.json')

    if args.download:
        if not base_url or not token:
            print('When using --download you must supply --base-url and --token, or set PAGEBUILDER_BASE_URL and PAGEBUILDER_TOKEN in your .env', file=sys.stderr)
            sys.exit(1)
        text = download_pages(base_url, token, endpoint=args.endpoint)
        with open(pages_json_path, 'w', encoding='utf8') as fh:
            fh.write(text)
        print('Saved downloaded pages to', pages_json_path)

    if not os.path.isfile(pages_json_path):
        print('Pages JSON not found at', pages_json_path, file=sys.stderr)
        print('Either provide --pages or run with --download --base-url --token', file=sys.stderr)
        sys.exit(1)

    print('Loading pages JSON from', pages_json_path)
    data = load_json_file(pages_json_path)
    pages = extract_pages_container(data)
    print('Total pages found in JSON:', len(pages))

    rows = find_matches(pages, targets)
    print('Total matching blocks found:', len(rows))

    out_csv = os.path.join(out_dir, 'destacado-matches.csv')
    write_csv(rows, out_csv)
    print('Wrote matches to', out_csv)


if __name__ == '__main__':
    main()
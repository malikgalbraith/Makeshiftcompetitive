#!/usr/bin/env python3
"""
FCC Public Files document name search.
Searches all stations in a state for documents matching a keyword.

Usage:
    python fcc_doc_search.py --state IA --keyword "Lahn"
    python fcc_doc_search.py --state IA --keyword "Lahn" --output results.csv
"""

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

FIND_URL = "https://publicfiles.fcc.gov/find/{prefix}/page-offset-0/order-call-sign-asc/filter-none/"
SEARCH_URL = "https://publicfiles.fcc.gov/api/manager/search/key/{keyword}.json?entityId={entity_id}"
PREFIXES = ["k", "w"]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FCCSearch/1.0)"})


def fetch(url, retries=3, delay=2):
    """Fetch a URL and return response text."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay * (attempt + 1))


def get_iowa_stations(state):
    """Get all stations in a state by fetching K/W prefix lists and filtering by state."""
    stations = []
    seen_ids = set()

    for prefix in PREFIXES:
        url = FIND_URL.format(prefix=prefix)
        print(f"  Fetching {prefix.upper()}-prefix stations...", flush=True)
        try:
            html = fetch(url)
        except Exception as e:
            print(f"  Warning: could not fetch {prefix}-prefix stations: {e}")
            continue

        match = re.search(r"let results = (\{.*?\});", html, re.DOTALL)
        if not match:
            continue

        data = json.loads(match.group(1))
        entity = data.get("entity", {})

        for list_key in ["tvFacilityList", "fmFacilityList", "amFacilityList"]:
            for s in entity.get(list_key, []):
                if s.get("communityState") == state and s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    stations.append({
                        "id": s["id"],
                        "callSign": s.get("callSign", ""),
                        "service": s.get("service", ""),
                        "serviceCode": s.get("serviceCode", ""),
                        "city": s.get("communityCity", ""),
                        "state": s.get("communityState", ""),
                        "status": s.get("status", ""),
                    })

    return stations


def search_station(station, keyword):
    """Search for keyword in a single station's documents. Returns list of matches."""
    import urllib.parse
    url = SEARCH_URL.format(keyword=urllib.parse.quote(keyword), entity_id=station["id"])
    try:
        text = fetch(url)
        data = json.loads(text)
        result = data.get("searchResult", {})
        matches = []
        for f in result.get("files", []):
            matches.append({
                "entityId": station["id"],
                "callSign": station["callSign"],
                "service": station["service"],
                "city": station["city"],
                "state": station["state"],
                "type": "file",
                "name": f.get("file_name", ""),
                "folderPath": f.get("file_folder_path", ""),
                "folderId": f.get("folder_id", ""),
                "fileId": f.get("file_id", ""),
                "extension": f.get("file_extension", ""),
            })
        for folder in result.get("folders", []):
            matches.append({
                "entityId": station["id"],
                "callSign": station["callSign"],
                "service": station["service"],
                "city": station["city"],
                "state": station["state"],
                "type": "folder",
                "name": folder.get("folder_name", ""),
                "folderPath": "",
                "folderId": folder.get("entity_folder_id", ""),
                "fileId": "",
                "extension": "",
            })
        return matches
    except Exception as e:
        print(f"  Warning: error searching {station['callSign']} ({station['id']}): {e}", flush=True)
        return []


def main():
    parser = argparse.ArgumentParser(description="Search FCC public file document names by state and keyword.")
    parser.add_argument("--state", required=True, help="Two-letter state code (e.g. IA)")
    parser.add_argument("--keyword", required=True, help="Keyword to search in document names")
    parser.add_argument("--output", help="Optional CSV output file")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers (default: 10)")
    args = parser.parse_args()

    state = args.state.upper()
    keyword = args.keyword

    print(f"\nFCC Public Files Search")
    print(f"  State:   {state}")
    print(f"  Keyword: {keyword}")
    print(f"  Workers: {args.workers}")
    print()

    # Step 1: Get all stations in the state
    print(f"Step 1: Loading {state} stations...")
    stations = get_iowa_stations(state)
    print(f"  Found {len(stations)} stations in {state}")
    if not stations:
        print("No stations found. Check your state code.")
        sys.exit(1)

    # Step 2: Search each station for the keyword
    print(f"\nStep 2: Searching {len(stations)} stations for '{keyword}'...")
    all_matches = []
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(search_station, s, keyword): s for s in stations}
        for future in as_completed(futures):
            completed += 1
            matches = future.result()
            all_matches.extend(matches)
            if completed % 25 == 0 or completed == len(stations):
                print(f"  Progress: {completed}/{len(stations)} stations checked, {len(all_matches)} matches so far", flush=True)

    # Step 3: Display results
    print(f"\n{'='*60}")
    print(f"Results: {len(all_matches)} document(s) matching '{keyword}' in {state}")
    print(f"{'='*60}")

    if all_matches:
        for m in sorted(all_matches, key=lambda x: (x["callSign"], x["name"])):
            print(f"  [{m['callSign']}] {m['city']}, {m['state']} ({m['service']})")
            label = "FILE" if m["type"] == "file" else "FOLDER"
            print(f"    {label}: {m['name']}")
            if m.get("folderPath"):
                print(f"    Path: {m['folderPath']}")
            print()
    else:
        print(f"  No documents found matching '{keyword}' in {state}.")

    # Step 4: Write CSV if requested
    if args.output and all_matches:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["callSign", "service", "city", "state", "type", "name", "folderPath", "extension", "entityId", "folderId", "fileId"])
            writer.writeheader()
            writer.writerows(all_matches)
        print(f"Results written to {args.output}")
    elif args.output:
        print(f"No results to write.")


if __name__ == "__main__":
    main()

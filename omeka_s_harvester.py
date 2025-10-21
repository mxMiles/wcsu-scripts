#!/usr/bin/env python3
#connects to Omeka-S and pulls down items created in the last 24 hours (configurable)
#saves those items into a .csv file.
#later on, a different script shoves those items into ArchivesSpace

import requests
import csv
import time
from datetime import datetime as dt, timedelta, timezone

# --- CONFIGURATION ---
#to look further back, change timedelta from 1 to whatever
yesterday = (dt.now() - timedelta(1)).strftime('%Y-%m-%d')

BASE_URL = 'http://ruth.wcsu.edu/omeka-s/api/items'+'?datetime[0][join]=and&datetime[0][field]=created&datetime[0][type]=>&datetime[0][val]='+str(yesterday) 
PUBLIC_BASE_URL = 'https://archives.library.wcsu.edu/omeka-s'     # Base for public URLs (no trailing slash)
PUBLIC_SITE_SLUG = 'digital'  # Replace with your public site slug
PER_PAGE = 50

# --- Calculate cutoff time (24 hours ago) ---
#cutoff_time = dt.now(timezone.utc) - timedelta(days=1)
#print(f"Harvesting items modified or created since: {cutoff_time.isoformat()}")
print(f"Harvesting items modified or created since: {yesterday}")

# --- Request setup ---
params = {
    'per_page': PER_PAGE,
    'page': 1,
    'site_id': 1
}

#endpoint = 'http://ruth.wcsu.edu/omeka/api/items'+'?modified_since='+str(yesterday)

# --- Helper: Flatten item JSON and add public URL ---
def flatten_item(item):
    flat = {}

    # ✅ Add public URL
    item_id = item.get('o:id')
    flat['public_url'] = f"{PUBLIC_BASE_URL}/s/{PUBLIC_SITE_SLUG}/item/{item_id}"

    for key, value in item.items():
        if key.startswith('@'):
            continue
        elif isinstance(value, list):
            texts = []
            for v in value:
                if isinstance(v, dict) and '@value' in v:
                    texts.append(v['@value'])
                elif isinstance(v, dict) and '@id' in v:
                    texts.append(v['@id'])
                else:
                    texts.append(str(v))
            flat[key] = '; '.join(texts)
        elif isinstance(value, dict):
            if '@value' in value:
                flat[key] = value['@value']
            elif '@id' in value:
                flat[key] = value['@id']
            else:
                flat[key] = str(value)
        else:
            flat[key] = str(value)

    return flat

# --- Harvest items ---
all_items = []
page = 1

while True:
    print(f"Requesting page {page}...")
    params['page'] = page
    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print(f"❌ Error: {response.status_code} - {response.reason}")
        break

    try:
        page_items = response.json()
    except requests.exceptions.JSONDecodeError:
        print("❌ Failed to parse JSON:")
        print(response.text)
        break

    if not page_items:
        break

    for item in page_items:
        mod = item.get('o:modified') or item.get('o:created')
        if isinstance(mod, dict) and '@value' in mod:
            mod_str = mod['@value']
            try:
                mod_dt = dt.fromisoformat(mod_str.replace('Z', '+00:00'))
#                if mod_dt > cutoff_time:
                all_items.append(item)
            except ValueError:
                print(f"⚠️ Could not parse date: {mod_str}")
        else:
            print(f"⚠️ Item {item.get('o:id')} missing 'o:modified' or 'o:created'")

    page += 1
    time.sleep(1)

print(f"\n✅ Total items retrieved (filtered): {len(all_items)}")

# --- Flatten and write to CSV ---
flattened = [flatten_item(item) for item in all_items]
fieldnames = sorted(set().union(*(item.keys() for item in flattened)))

output_file = 'omeka_s_items_output.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for row in flattened:
        writer.writerow(row)

print(f"📄 CSV file created: {output_file}")

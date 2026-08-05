#!/bin/bash

tellMeAbout() {
  if [ -z "$1" ]; then
    echo "Usage: tellMeAbout <id>"
    echo "Example: tellMeAbout cthi_amrev1776_aspace_c3ae8757140849b9f17c83b79b6639a8"
    return 1
  fi

  local id="$1"
  local solr_url="http://localhost:8984/solr/arclight_b/select"

  # Query Solr for the given id, return all fields
  local response
  response=$(curl -s "${solr_url}?q=id:${id}&fl=*&wt=json")

  # Check if response is valid JSON
  if ! echo "$response" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "Error: Did not get valid JSON back from Solr"
    echo "  URL: ${solr_url}?q=id:${id}&fl=*&wt=json"
    echo ""
    echo "Raw response:"
    echo "$response"
    return 1
  fi

  # Check if we got any results
  local num_found
  num_found=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['response']['numFound'])" 2>/dev/null)

  if [ "$num_found" = "0" ] || [ -z "$num_found" ]; then
    echo "No document found with id: ${id}"
    return 1
  fi

  # Pretty-print all fields
  echo "$response" | python3 -c "
import sys, json

data = json.load(sys.stdin)
doc = data['response']['docs'][0]

print('=' * 60)
print(f'ID: {doc.get(\"id\", \"N/A\")}')
print('=' * 60)

for key in sorted(doc.keys()):
    value = doc[key]
    if isinstance(value, list):
        print(f'\n{key}:')
        for i, v in enumerate(value, 1):
            print(f'  [{i}] {v}')
    else:
        print(f'\n{key}: {value}')

print('\n' + '=' * 60)
print(f'Total fields: {len(doc)}')
print('=' * 60)
"
}

tellMeAbout "$@"

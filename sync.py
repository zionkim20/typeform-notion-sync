"""
Typeform API -> Notion Sync
Pulls all V2 responses via API, matches to Notion clients, updates records.
Handles both backfill and new submissions.

Can be run manually or as a cron job via GitHub Actions.
"""
import json
import os
import re
import sys
import urllib.request
import time

# === Config (from environment variables) ===
TYPEFORM_TOKEN = os.environ.get("TYPEFORM_TOKEN")
TYPEFORM_FORM_ID = os.environ.get("TYPEFORM_FORM_ID")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = os.environ.get("NOTION_DB_ID")

# Validate required env vars
missing = []
for var_name in ["TYPEFORM_TOKEN", "TYPEFORM_FORM_ID", "NOTION_TOKEN", "NOTION_DB_ID"]:
    if not os.environ.get(var_name):
        missing.append(var_name)

if missing:
    print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)

# Typeform field IDs -> our capability short names
CAPABILITY_FIELDS = {
    "O1WlKCDs4vwd": "Cleaning",
    "Iz52pLSqGiTM": "Laundry",
    "n6qrvtdZyKFn": "Cooking",
    "TkJugpqo075d": "Pet Care",
    "YXiKjLDJvtgV": "Childcare",
    "COYuN2wotIQz": "Grocery",
    "9n7pTi6m4iQB": "Vehicle",
    "Huxr6RqNLXmg": "Organization",
    "TiSEMHP47IV0": "House Mgmt",
}

# Typeform field IDs for identity + preferences
FIELD_FIRST_NAME = "o1y3GX8jj48E"
FIELD_LAST_NAME = "KR7LISBiu7yD"
FIELD_EMAIL = "wPikONTZh8zZ"
FIELD_RELATIONAL = "GrQyr8j5sFPl"
FIELD_AUTONOMY = "l7riGwpkiDZK"

# Relational preference mapping
RELATIONAL_MAP = {
    "reserved": "Reserved / Stealth",
    "stealth": "Reserved / Stealth",
    "relational": "Relational / Engaged",
    "engaged": "Relational / Engaged",
    "between": "Somewhere in Between",
}

# Decision autonomy mapping
AUTONOMY_MAP = {
    "directive": "Directive",
    "judgment": "Judgment-Oriented",
    "between": "Somewhere in Between",
}


def extract_level(label):
    """Extract level number from choice label like 'Level 2: Full Household...'"""
    if not label:
        return None
    lower = label.lower()
    if "don't have" in lower or "don't need" in lower:
        return "N/A"
    match = re.match(r"Level (\d)", label)
    if match:
        return f"L{match.group(1)}"
    return None


def map_select(text, mapping):
    """Map a choice label to a Notion select value using keyword matching."""
    if not text:
        return None
    lower = text.lower()
    for keyword, notion_value in mapping.items():
        if keyword in lower:
            return notion_value
    return None


def fetch_typeform_responses():
    """Fetch all V2 typeform responses via API."""
    all_responses = []
    url = f"https://api.typeform.com/forms/{TYPEFORM_FORM_ID}/responses?page_size=100&completed=true"

    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TYPEFORM_TOKEN}"})
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read().decode())

    total = data.get("total_items", 0)
    items = data.get("items", [])
    all_responses.extend(items)

    print(f"Fetched {len(all_responses)} of {total} typeform responses")
    return all_responses


def parse_response(item):
    """Parse a single typeform response into structured data."""
    answers = item.get("answers", [])
    submitted = item.get("submitted_at", "")
    response_id = item.get("response_id", "")

    # Build lookup by field ID
    answer_map = {}
    for a in answers:
        fid = a.get("field", {}).get("id", "")
        answer_map[fid] = a

    # Extract identity
    first_name = answer_map.get(FIELD_FIRST_NAME, {}).get("text", "").strip()
    last_name = answer_map.get(FIELD_LAST_NAME, {}).get("text", "").strip()
    email = answer_map.get(FIELD_EMAIL, {}).get("email", "").strip()

    if not first_name:
        return None

    # Extract capability levels
    capabilities = {}
    for field_id, short_name in CAPABILITY_FIELDS.items():
        answer = answer_map.get(field_id, {})
        label = answer.get("choice", {}).get("label", "")
        level = extract_level(label)
        if level:
            capabilities[short_name] = level

    # Build capability string (skip N/A for cleaner output)
    cap_parts = []
    for name, level in capabilities.items():
        cap_parts.append(f"{name}: {level}")
    cap_string = ", ".join(cap_parts) if cap_parts else ""

    # Extract preferences
    rel_label = answer_map.get(FIELD_RELATIONAL, {}).get("choice", {}).get("label", "")
    relational = map_select(rel_label, RELATIONAL_MAP)

    aut_label = answer_map.get(FIELD_AUTONOMY, {}).get("choice", {}).get("label", "")
    autonomy = map_select(aut_label, AUTONOMY_MAP)

    return {
        "name": f"{first_name} {last_name}",
        "first": first_name,
        "last": last_name,
        "email": email,
        "capabilities": cap_string,
        "relational": relational,
        "autonomy": autonomy,
        "submitted": submitted[:10],
        "response_id": response_id,
    }


def find_notion_client(last_name, first_name):
    """Search Notion for a client record by name."""
    # Try last name first (more unique)
    for search_term in [last_name, first_name]:
        if not search_term:
            continue

        body = json.dumps({
            "filter": {"property": "Task name", "title": {"contains": search_term}},
            "page_size": 3
        }).encode()

        req = urllib.request.Request(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            data=body, method="POST",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
        )

        try:
            resp = urllib.request.urlopen(req)
            data = json.loads(resp.read().decode())
            results = data.get("results", [])
            if results:
                page = results[0]
                title_parts = page["properties"].get("Task name", {}).get("title", [])
                notion_name = "".join([t.get("plain_text", "") for t in title_parts])
                return page["id"], notion_name
        except Exception as e:
            print(f"  Search error for '{search_term}': {e}")

        time.sleep(0.35)

    return None, None


def update_notion(page_id, cap_string, relational, autonomy):
    """Update Notion client record with typeform data."""
    properties = {}

    if cap_string:
        properties["Capability Requirements"] = {
            "rich_text": [{"text": {"content": cap_string}}]
        }
    if relational:
        properties["Relational Preference"] = {"select": {"name": relational}}
    if autonomy:
        properties["Decision Autonomy"] = {"select": {"name": autonomy}}

    if not properties:
        return False

    body = json.dumps({"properties": properties}).encode()
    req = urllib.request.Request(
        f"https://api.notion.com/v1/pages/{page_id}",
        data=body, method="PATCH",
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
    )

    resp = urllib.request.urlopen(req)
    return resp.status == 200


# === Main ===

def main():
    print("=== Typeform V2 -> Notion Sync ===\n")

    # Step 1: Fetch all typeform responses
    responses = fetch_typeform_responses()

    # Step 2: Parse and match
    parsed = []
    for item in responses:
        record = parse_response(item)
        if record and record["capabilities"]:
            parsed.append(record)

    # Deduplicate: keep latest submission per email
    by_email = {}
    for r in parsed:
        key = r["email"] or r["name"]
        if key not in by_email or r["submitted"] > by_email[key]["submitted"]:
            by_email[key] = r

    records = list(by_email.values())
    print(f"Unique records with capability data: {len(records)}\n")

    # Step 3: Match and update
    updated = 0
    not_found = 0
    skipped = 0

    for r in records:
        print(f"-- {r['name']} ({r['submitted']}) --")
        print(f"  Caps: {r['capabilities'][:70]}...")
        print(f"  Rel: {r['relational']} | Aut: {r['autonomy']}")

        page_id, notion_name = find_notion_client(r["last"], r["first"])
        time.sleep(0.35)

        if not page_id:
            print(f"  NOT FOUND in Notion")
            not_found += 1
            print()
            continue

        print(f"  -> Notion: {notion_name}")

        try:
            update_notion(page_id, r["capabilities"], r["relational"], r["autonomy"])
            print(f"  UPDATED!")
            updated += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            skipped += 1

        time.sleep(0.35)
        print()

    print(f"\n=== DONE ===")
    print(f"Updated: {updated} | Not found: {not_found} | Errors: {skipped}")
    print(f"Total unique responses processed: {len(records)}")

    # Exit with error code if everything failed
    if updated == 0 and len(records) > 0 and skipped == len(records):
        sys.exit(1)


if __name__ == "__main__":
    main()

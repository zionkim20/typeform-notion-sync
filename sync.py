"""
Typeform API -> Notion Sync
Pulls all V2 responses via API, matches to Notion clients, updates records.
Syncs BOTH capability assessments AND contact info (email, phone, address, city).
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

# Contact info field IDs — discovered dynamically from form definition
CONTACT_FIELDS = {}  # populated by fetch_form_fields()

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


def get_answer_value(answer):
    """Extract the text value from any Typeform answer type."""
    return (answer.get("text", "") or
            answer.get("email", "") or
            answer.get("phone_number", "") or
            "").strip()


def discover_contact_fields(responses):
    """Discover contact info field IDs from response data (no form definition needed)."""
    global CONTACT_FIELDS

    known_ids = set(CAPABILITY_FIELDS.keys()) | {
        FIELD_FIRST_NAME, FIELD_LAST_NAME, FIELD_EMAIL,
        FIELD_RELATIONAL, FIELD_AUTONOMY,
    }

    # Collect unknown fields across first 10 responses for reliable discovery
    unknown_fields = {}
    for item in responses[:10]:
        for answer in item.get("answers", []):
            fid = answer.get("field", {}).get("id", "")
            ftype = answer.get("field", {}).get("type", "")
            atype = answer.get("type", "")
            ref = answer.get("field", {}).get("ref", "")
            if fid and fid not in known_ids:
                value = get_answer_value(answer)
                if fid not in unknown_fields:
                    unknown_fields[fid] = {
                        "field_type": ftype, "answer_type": atype,
                        "ref": ref, "samples": [],
                    }
                if value:
                    unknown_fields[fid]["samples"].append(value)

    print(f"  Found {len(unknown_fields)} unknown fields to classify:")
    for fid, info in unknown_fields.items():
        sample = info["samples"][0][:50] if info["samples"] else "(empty)"
        print(f"    {fid} type={info['answer_type']} ref={info['ref']} sample={sample}")

    # Auto-classify by answer type and ref keywords
    for fid, info in unknown_fields.items():
        atype = info["answer_type"]
        ref = info["ref"].lower()

        # Phone: distinct answer type
        if atype == "phone_number":
            CONTACT_FIELDS["phone"] = fid
        # Address fields: check ref for keywords
        elif "street" in ref or "address" in ref and "line" not in ref:
            CONTACT_FIELDS["street"] = fid
        elif "line_2" in ref or "line-2" in ref or "address_line" in ref or "address-line" in ref:
            CONTACT_FIELDS["address_line_2"] = fid
        elif ref == "city" or ("city" in ref and "state" not in ref):
            CONTACT_FIELDS["city"] = fid
        elif ref == "state" or ("state" in ref and "city" not in ref):
            CONTACT_FIELDS["state"] = fid

    # Fallback: if refs didn't help, try classifying by sample values
    # Address-like: contains numbers + street words (St, Ave, Dr, Rd, etc.)
    # City-like: 1-3 words, all letters, no numbers
    if not CONTACT_FIELDS:
        address_pattern = re.compile(r'\d+.*\b(st|ave|dr|rd|blvd|ln|ct|way|pl|cir)\b', re.IGNORECASE)
        for fid, info in unknown_fields.items():
            if fid in CONTACT_FIELDS.values():
                continue
            if info["answer_type"] != "text":
                continue
            for sample in info["samples"]:
                if address_pattern.search(sample) and "street" not in CONTACT_FIELDS:
                    CONTACT_FIELDS["street"] = fid
                    break

    print(f"  Classified contact fields: {CONTACT_FIELDS}")
    return bool(CONTACT_FIELDS)


def fetch_typeform_responses():
    """Fetch all V2 typeform responses via API (both completed and partial)."""
    all_responses = []

    for completed_flag in ["true", "false"]:
        url = f"https://api.typeform.com/forms/{TYPEFORM_FORM_ID}/responses?page_size=100&completed={completed_flag}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TYPEFORM_TOKEN}"})
        try:
            resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            print(f"ERROR: Typeform API returned {e.code} for completed={completed_flag}")
            if e.code == 403:
                print("  Token may be expired or revoked. Regenerate at:")
                print("  https://admin.typeform.com/user/tokens")
                print("  Then update TYPEFORM_TOKEN in GitHub Secrets.")
            raise
        data = json.loads(resp.read().decode())

        items = data.get("items", [])
        for item in items:
            item["_completed"] = (completed_flag == "true")
        all_responses.extend(items)
        time.sleep(0.35)

    count = len(all_responses)
    completed = sum(1 for r in all_responses if r.get("_completed"))
    print(f"Fetched {count} typeform responses ({completed} completed, {count - completed} partial)")
    return all_responses


def parse_response(item):
    """Parse a single typeform response into structured data."""
    answers = item.get("answers", [])
    submitted = item.get("submitted_at", "") or item.get("landed_at", "")
    response_id = item.get("response_id", "")
    completed = item.get("_completed", True)

    # Build lookup by field ID
    answer_map = {}
    for a in answers:
        fid = a.get("field", {}).get("id", "")
        answer_map[fid] = a

    # Extract identity
    first_name = answer_map.get(FIELD_FIRST_NAME, {}).get("text", "").strip()
    last_name = answer_map.get(FIELD_LAST_NAME, {}).get("text", "").strip()
    email = answer_map.get(FIELD_EMAIL, {}).get("email", "").strip()

    # Need at least a name to match against Notion
    if not first_name and not last_name:
        if not completed:
            # Debug: show why partial response was skipped
            field_ids = list(answer_map.keys())
            print(f"  [debug] Partial response skipped (no name): {len(answers)} answers, "
                  f"email={email or '(none)'}, fields={field_ids[:5]}")
        return None

    # Extract contact info (dynamically discovered fields)
    phone = ""
    if "phone" in CONTACT_FIELDS:
        phone = get_answer_value(answer_map.get(CONTACT_FIELDS["phone"], {}))

    street = ""
    if "street" in CONTACT_FIELDS:
        street = get_answer_value(answer_map.get(CONTACT_FIELDS["street"], {}))

    line2 = ""
    if "address_line_2" in CONTACT_FIELDS:
        line2 = get_answer_value(answer_map.get(CONTACT_FIELDS["address_line_2"], {}))

    # Combine address parts
    address_parts = []
    if street:
        address_parts.append(street)
    if line2 and line2.lower() not in ("n/a", ".", "-", "none"):
        address_parts.append(line2)
    address = ", ".join(address_parts)

    city = ""
    if "city" in CONTACT_FIELDS:
        city = get_answer_value(answer_map.get(CONTACT_FIELDS["city"], {}))

    state = ""
    if "state" in CONTACT_FIELDS:
        state = get_answer_value(answer_map.get(CONTACT_FIELDS["state"], {}))

    # Extract capability levels
    capabilities = {}
    for field_id, short_name in CAPABILITY_FIELDS.items():
        answer = answer_map.get(field_id, {})
        label = answer.get("choice", {}).get("label", "")
        level = extract_level(label)
        if level:
            capabilities[short_name] = level

    # Build capability string
    cap_parts = []
    for name, level in capabilities.items():
        cap_parts.append(f"{name}: {level}")
    cap_string = ", ".join(cap_parts) if cap_parts else ""

    # Extract preferences
    rel_label = answer_map.get(FIELD_RELATIONAL, {}).get("choice", {}).get("label", "")
    relational = map_select(rel_label, RELATIONAL_MAP)

    aut_label = answer_map.get(FIELD_AUTONOMY, {}).get("choice", {}).get("label", "")
    autonomy = map_select(aut_label, AUTONOMY_MAP)

    # Accept any response with useful data (contact info OR capabilities)
    has_data = bool(cap_string or relational or autonomy or email or phone or address or city)
    if not has_data:
        if not completed:
            name = f"{first_name} {last_name}".strip()
            print(f"  [debug] Partial '{name}' skipped (no data): email={email}, phone={phone}, "
                  f"caps={bool(cap_string)}, addr={bool(address)}")
        return None

    return {
        "name": f"{first_name} {last_name}".strip(),
        "first": first_name,
        "last": last_name,
        "email": email,
        "phone": phone,
        "address": address,
        "city": city,
        "state": state,
        "capabilities": cap_string,
        "relational": relational,
        "autonomy": autonomy,
        "submitted": submitted[:10] if submitted else "",
        "response_id": response_id,
        "completed": completed,
    }


def find_notion_client(last_name, first_name):
    """Search Notion for a client record by name. Returns page_id, name, and current values."""
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
                props = page.get("properties", {})
                title_parts = props.get("Task name", {}).get("title", [])
                notion_name = "".join([t.get("plain_text", "") for t in title_parts])

                # Extract current field values to avoid overwriting
                current = {
                    "email": props.get("Email", {}).get("email", "") or "",
                    "phone": props.get("Phone", {}).get("phone_number", "") or "",
                    "address": "".join(
                        t.get("plain_text", "")
                        for t in props.get("Client Address", {}).get("rich_text", [])
                    ).strip(),
                    "city": "".join(
                        t.get("plain_text", "")
                        for t in props.get("City", {}).get("rich_text", [])
                    ).strip(),
                    "state": (props.get("State", {}).get("select") or {}).get("name", ""),
                }

                return page["id"], notion_name, current
        except Exception as e:
            print(f"  Search error for '{search_term}': {e}")

        time.sleep(0.35)

    return None, None, {}


def update_notion(page_id, record, current_values):
    """Update Notion client record with typeform data. Only fills empty fields for contact info."""
    properties = {}

    # Contact info — only fill if currently empty in Notion
    if record["email"] and not current_values.get("email"):
        properties["Email"] = {"email": record["email"]}
    if record["phone"] and not current_values.get("phone"):
        properties["Phone"] = {"phone_number": record["phone"]}
    if record["address"] and not current_values.get("address"):
        properties["Client Address"] = {
            "rich_text": [{"text": {"content": record["address"][:2000]}}]
        }
    if record["city"] and not current_values.get("city"):
        properties["City"] = {
            "rich_text": [{"text": {"content": record["city"]}}]
        }
    if record["state"] and not current_values.get("state"):
        properties["State"] = {"select": {"name": record["state"]}}

    # Capabilities — always overwrite (latest assessment wins)
    if record["capabilities"]:
        properties["Capability Requirements"] = {
            "rich_text": [{"text": {"content": record["capabilities"]}}]
        }
    if record["relational"]:
        properties["Relational Preference"] = {"select": {"name": record["relational"]}}
    if record["autonomy"]:
        properties["Decision Autonomy"] = {"select": {"name": record["autonomy"]}}

    # Always write Typeform Status so team knows full vs partial
    status = "Complete" if record["completed"] else "Partial"
    properties["Typeform Status"] = {"select": {"name": status}}

    if len(properties) <= 1:
        # Only has status, no actual data — skip
        return False, []

    # Track what we're writing for logging
    written_fields = [k for k in properties if k != "Typeform Status"]

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
    return resp.status == 200, written_fields


# === Main ===

def main():
    print("=== Typeform V2 -> Notion Sync (Full) ===\n")

    # Step 1: Fetch all typeform responses
    responses = fetch_typeform_responses()

    # Step 1b: Discover contact field IDs from response data
    print("\n[1b] Discovering contact fields from response data...")
    discover_contact_fields(responses)
    print()

    # Step 2: Parse and match
    parsed = []
    skipped_no_data = 0
    for item in responses:
        record = parse_response(item)
        if record:
            parsed.append(record)
        else:
            skipped_no_data += 1

    # Deduplicate: prefer completed over partial, then latest submission
    by_key = {}
    for r in parsed:
        key = r["email"] or r["name"]
        existing = by_key.get(key)
        if not existing:
            by_key[key] = r
        elif r["completed"] and not existing["completed"]:
            # Completed submission beats partial
            by_key[key] = r
        elif r["completed"] == existing["completed"] and r["submitted"] > existing["submitted"]:
            # Same type — keep latest
            by_key[key] = r

    records = list(by_key.values())
    complete_count = sum(1 for r in records if r["completed"])
    partial_count = len(records) - complete_count
    print(f"Unique records: {len(records)} ({complete_count} complete, {partial_count} partial)")
    if skipped_no_data:
        print(f"Skipped {skipped_no_data} responses with no usable data")
    print()

    # Step 3: Match and update
    updated = 0
    not_found = 0
    skipped = 0

    for r in records:
        status_tag = "COMPLETE" if r["completed"] else "PARTIAL"
        print(f"-- {r['name']} ({r['submitted']}) [{status_tag}] --")
        if r["capabilities"]:
            print(f"  Caps: {r['capabilities'][:70]}...")
        else:
            print(f"  Caps: (none)")
        print(f"  Rel: {r['relational']} | Aut: {r['autonomy']}")

        # Show contact info in logs
        contact_parts = []
        if r["email"]:
            contact_parts.append(f"email={r['email']}")
        if r["phone"]:
            contact_parts.append(f"phone={r['phone']}")
        if r["address"]:
            contact_parts.append(f"addr={r['address'][:40]}")
        if r["city"]:
            contact_parts.append(f"city={r['city']}")
        if contact_parts:
            print(f"  Contact: {', '.join(contact_parts)}")

        page_id, notion_name, current = find_notion_client(r["last"], r["first"])
        time.sleep(0.35)

        if not page_id:
            print(f"  NOT FOUND in Notion")
            not_found += 1
            print()
            continue

        print(f"  -> Notion: {notion_name}")

        try:
            success, written = update_notion(page_id, r, current)
            if written:
                print(f"  UPDATED: {', '.join(written)}")
                updated += 1
            else:
                print(f"  SKIPPED (no new data to write)")
                skipped += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            skipped += 1

        time.sleep(0.35)
        print()

    print(f"\n=== DONE ===")
    print(f"Updated: {updated} | Not found: {not_found} | Skipped: {skipped}")
    print(f"Total unique responses processed: {len(records)}")

    # Exit with error code if everything failed
    if updated == 0 and len(records) > 0 and skipped == len(records):
        sys.exit(1)


if __name__ == "__main__":
    main()

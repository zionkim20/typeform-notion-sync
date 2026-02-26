#!/usr/bin/env python3
"""
Full CSV → Notion Sync
======================
Pushes ALL Typeform data into Notion — not just capabilities, but emails,
phones, addresses, and the full household onboarding profile.

Handles both V1 (original onboarding) and V2 (capability levels) CSVs.

Usage:
    export NOTION_TOKEN='ntn_...'
    python3 full_csv_sync.py --v2 <v2-csv-path> --v1 <v1-csv-path>
    python3 full_csv_sync.py --v2 <v2-csv-path>              # V2 only
    python3 full_csv_sync.py --v1 <v1-csv-path>              # V1 only
    python3 full_csv_sync.py --v2 <v2-csv-path> --dry-run    # Preview only
"""
import csv
import json
import os
import re
import sys
import time
import argparse
import urllib.request

# === Config ===
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = os.environ.get("NOTION_DB_ID", "281d928d-6a8c-81d6-b2c2-cbdfb63b7b5b")

if not NOTION_TOKEN:
    print("ERROR: Set NOTION_TOKEN environment variable")
    sys.exit(1)


# ============================================================================
# Notion helpers
# ============================================================================

def notion_request(method, url, body=None):
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode())


def fetch_all_notion_clients():
    """Fetch all clients from Notion with their current field values."""
    all_results = []
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data = notion_request("POST",
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query", body)
        all_results.extend(data.get("results", []))
        if data.get("has_more"):
            cursor = data["next_cursor"]
        else:
            break

    clients = {}
    for page in all_results:
        props = page.get("properties", {})
        title_parts = props.get("Task name", {}).get("title", [])
        name = "".join(t.get("plain_text", "") for t in title_parts).strip()

        # Extract current values
        email_val = props.get("Email", {}).get("email", "") or ""
        phone_val = props.get("Phone", {}).get("phone_number", "") or ""

        addr_rt = props.get("Client Address", {}).get("rich_text", [])
        addr_val = "".join(t.get("plain_text", "") for t in addr_rt).strip()

        city_rt = props.get("City", {}).get("rich_text", [])
        city_val = "".join(t.get("plain_text", "") for t in city_rt).strip()

        state_sel = props.get("State", {}).get("select")
        state_val = state_sel.get("name", "") if state_sel else ""

        sched_val = props.get("Scheduling link", {}).get("url", "") or ""

        cap_rt = props.get("Capability Requirements", {}).get("rich_text", [])
        cap_val = "".join(t.get("plain_text", "") for t in cap_rt).strip()

        onb_rt = props.get("Onboarding Profile", {}).get("rich_text", [])
        onb_val = "".join(t.get("plain_text", "") for t in onb_rt).strip()

        clients[name.lower()] = {
            "page_id": page["id"],
            "name": name,
            "email": email_val,
            "phone": phone_val,
            "address": addr_val,
            "city": city_val,
            "state": state_val,
            "scheduling_link": sched_val,
            "caps": cap_val,
            "onboarding_profile": onb_val,
        }

    return clients


def find_match(notion_clients, first, last):
    """Match a CSV name to a Notion client using word-boundary matching.
    Prevents substring collisions like 'Craft' matching 'Beacraft'."""
    search = f"{first} {last}".strip().lower()
    # Exact
    for k, v in notion_clients.items():
        if search == k:
            return v
    # Last name — must match as a whole word
    if last and len(last) > 1:
        pattern = re.compile(r'\b' + re.escape(last.lower()) + r'\b')
        for k, v in notion_clients.items():
            if pattern.search(k):
                return v
    # First name — must match as a whole word
    if first and len(first) > 2:
        pattern = re.compile(r'\b' + re.escape(first.lower()) + r'\b')
        for k, v in notion_clients.items():
            if pattern.search(k):
                return v
    return None


def create_onboarding_profile_property():
    """Create 'Onboarding Profile' rich_text property in Notion DB if it doesn't exist."""
    try:
        db = notion_request("GET", f"https://api.notion.com/v1/databases/{NOTION_DB_ID}")
        if "Onboarding Profile" in db.get("properties", {}):
            print("  'Onboarding Profile' property already exists\n")
            return True

        print("  Creating 'Onboarding Profile' property in Notion...")
        notion_request("PATCH",
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
            {"properties": {"Onboarding Profile": {"rich_text": {}}}}
        )
        print("  Created!\n")
        return True
    except Exception as e:
        print(f"  Error creating property: {e}")
        return False


def update_notion_client(page_id, updates, dry_run=False):
    """Update a Notion page with the given property updates."""
    if not updates:
        return False

    properties = {}

    if "email" in updates:
        properties["Email"] = {"email": updates["email"]}
    if "phone" in updates:
        properties["Phone"] = {"phone_number": updates["phone"]}
    if "address" in updates:
        properties["Client Address"] = {
            "rich_text": [{"text": {"content": updates["address"][:2000]}}]
        }
    if "city" in updates:
        properties["City"] = {
            "rich_text": [{"text": {"content": updates["city"]}}]
        }
    if "state" in updates:
        properties["State"] = {"select": {"name": updates["state"]}}
    if "scheduling_link" in updates:
        properties["Scheduling link"] = {"url": updates["scheduling_link"]}
    if "caps" in updates:
        properties["Capability Requirements"] = {
            "rich_text": [{"text": {"content": updates["caps"][:2000]}}]
        }
    if "relational" in updates:
        properties["Relational Preference"] = {"select": {"name": updates["relational"]}}
    if "autonomy" in updates:
        properties["Decision Autonomy"] = {"select": {"name": updates["autonomy"]}}
    if "onboarding_profile" in updates:
        # Notion rich_text limit is 2000 chars per block
        text = updates["onboarding_profile"][:2000]
        properties["Onboarding Profile"] = {
            "rich_text": [{"text": {"content": text}}]
        }

    if not properties:
        return False

    if dry_run:
        print(f"    [DRY RUN] Would update: {', '.join(properties.keys())}")
        return True

    notion_request("PATCH",
        f"https://api.notion.com/v1/pages/{page_id}",
        {"properties": properties}
    )
    return True


# ============================================================================
# V2 CSV parsing (capability levels form)
# ============================================================================

V2_CAPABILITY_COLS = {
    "Cleaning": "Cleaning",
    "Laundry Management": "Laundry",
    "Meal Planning & Cooking": "Cooking",
    "Pet Care": "Pet Care",
    "Childcare Support": "Childcare",
    "Grocery Ordering & Restocking": "Grocery",
    "Vehicle Care": "Vehicle",
    "Organization & Decluttering": "Organization",
    "House Management & Projects": "House Mgmt",
}

RELATIONAL_MAP = {
    "reserved": "Reserved / Stealth",
    "stealth": "Reserved / Stealth",
    "relational": "Relational / Engaged",
    "engaged": "Relational / Engaged",
    "between": "Somewhere in Between",
}

AUTONOMY_MAP = {
    "directive": "Directive",
    "judgment": "Judgment-Oriented",
    "between": "Somewhere in Between",
}


def extract_level(label):
    if not label:
        return None
    lower = label.lower()
    if "don't have" in lower or "don't need" in lower:
        return "N/A"
    match = re.match(r"Level (\d)", label)
    return f"L{match.group(1)}" if match else None


def map_select(text, mapping):
    if not text:
        return None
    lower = text.lower()
    for keyword, val in mapping.items():
        if keyword in lower:
            return val
    return None


def parse_v2_row(row):
    """Parse a V2 CSV row into structured data."""
    fn = row.get("What\u2019s your first name?", "") or row.get("What's your first name?", "")
    ln = row.get("What\u2019s your last name?", "") or row.get("What's your last name?", "")
    fn = fn.strip()
    ln = ln.strip()
    if not fn:
        return None

    email = (row.get("What\u2019s your email address?", "") or
             row.get("What's your email address?", "")).strip()
    phone = (row.get("What\u2019s your phone number?", "") or
             row.get("What's your phone number?", "")).strip()

    address_parts = []
    street = row.get("Street Address", "").strip()
    line2 = row.get("Address Line 2", "").strip()
    if street:
        address_parts.append(street)
    if line2 and line2.lower() not in ("n/a", ".", "-", "none"):
        address_parts.append(line2)
    address = ", ".join(address_parts)

    city = row.get("City", "").strip()
    state = row.get("State", "").strip()

    # Capability levels
    caps = {}
    for csv_col, short in V2_CAPABILITY_COLS.items():
        level = extract_level(row.get(csv_col, ""))
        if level and level != "N/A":
            caps[short] = level
    cap_string = ", ".join(f"{k}: {v}" for k, v in caps.items())

    # Preferences
    relational = None
    autonomy = None
    for col, val in row.items():
        if "Relational Presence" in col:
            relational = map_select(val, RELATIONAL_MAP)
        if "Decision Autonomy" in col:
            autonomy = map_select(val, AUTONOMY_MAP)

    # Build onboarding profile from ALL the rich data
    profile_lines = []

    def add(label, value):
        v = value.strip() if value else ""
        if v and v.lower() not in ("n/a", "none", "no", "0", ""):
            profile_lines.append(f"{label}: {v}")

    add("Household", row.get("Tell us about your household members", ""))
    add("Pets", row.get("Do you have pets?", ""))
    pet_type = row.get("What type of pets?", "").strip()
    if pet_type and pet_type.lower() not in ("n/a", "i don't have any pets."):
        add("Pet Details", pet_type)
    add("Bedrooms/Baths", row.get("How many bedrooms and bathrooms?", ""))
    add("Sq Ft", row.get("What's the total square footage of your home?", ""))
    add("Current Support", row.get("Do you currently have any household support?", ""))
    support_desc = row.get("Describe your current support and what they do", "")
    if support_desc:
        add("Support Details", support_desc)
    add("Keep/Transition", row.get("Will you keep this support or transition it to your house manager?", ""))
    add("Pain Points", row.get("What are your top 3 pain points with home management right now?", ""))
    add("Moving", row.get("Are you moving soon?", ""))
    add("Ideal Start", row.get("What's your ideal start date for house assistant support?", ""))
    add("Upcoming Travel", row.get("Any upcoming travel or commitments in the next 2 months we should be aware of?", ""))
    add("Weekday Routine", row.get("What does a typical weekday look like for your household?", ""))
    add("Work Schedules", row.get("Tell us about work schedules for adults in the household", ""))
    add("Kids Schedule", row.get("What's your kids' school schedule?", ""))
    add("After-School", row.get("Are there regular after-school activities or commitments?", ""))
    add("Weekend", row.get("What does a typical weekend look like?", ""))
    add("Trash/Recycling", row.get("When does your trash/recycling service come?", ""))
    add("Regular Vendors", row.get("Do you have any other routine vendors or services that come regularly?", ""))
    add("Preferred Hours", row.get("When would you ideally want house management support to happen?", ""))
    add("Off-Limits Times", row.get("Are there times when you definitely DON'T want support?", ""))
    add("Style Preferences", row.get("Any other preferences about how someone shows up in your home?", ""))
    add("Wellness/Fitness", row.get("Are there any fitness or wellness routines we could support?", ""))
    add("Chaotic Areas", row.get("Are there parts of your home that feel chaotic or out of sync?", ""))
    add("Recurring Friction", row.get("What are the sources of recurring friction that additional support could resolve?", ""))
    add("Restoration", row.get("Is there anything specific that helps you feel restored and relaxed at home?", ""))
    add("Special Considerations", row.get("Are there any special household considerations we should know about?", ""))
    add("Other Notes", row.get("Anything else you want us to know?", ""))

    profile_text = "\n".join(profile_lines)

    return {
        "first": fn, "last": ln,
        "email": email, "phone": phone,
        "address": address, "city": city, "state": state,
        "caps": cap_string,
        "relational": relational, "autonomy": autonomy,
        "onboarding_profile": profile_text,
        "submitted": row.get("Submit Date (UTC)", "")[:10],
    }


# ============================================================================
# V1 CSV parsing (original onboarding form)
# ============================================================================

def parse_v1_row(row):
    """Parse a V1 CSV row into structured data."""
    fn = row.get("First name", "").strip()
    ln = row.get("Last name", "").strip()
    if not fn:
        return None

    tags = row.get("Tags", "")
    if "AI Generated" in tags:
        return None  # skip test data

    email = row.get("Email", "").strip()
    phone = row.get("Phone number", "").strip()

    address_parts = []
    street = row.get("Address", "").strip()
    line2 = row.get("Address line 2", "").strip()
    if street:
        address_parts.append(street)
    if line2 and line2.lower() not in ("n/a", ".", "-", "none", ""):
        address_parts.append(line2)
    address = ", ".join(address_parts)

    city = row.get("City/Town", "").strip()
    state = row.get("State/Region/Province", "").strip()

    # Scheduling link
    sched = row.get("If you have 30 minutes scheduling link copy it below", "").strip()
    if sched and not sched.startswith("http"):
        sched = ""

    # Build onboarding profile
    profile_lines = []

    def add(label, value):
        v = value.strip() if value else ""
        if v and v.lower() not in ("n/a", "none", "no", "0", ""):
            profile_lines.append(f"{label}: {v}")

    add("Household", row.get("Family members (names of partners + ages of kids, if applicable)", ""))
    add("Pets", row.get("Pets (type, breed/size, care notes)", ""))
    pet_details = row.get("Share here more about the breed/size and any useful specifics", "")
    if pet_details:
        add("Pet Details", pet_details)
    add("Bedrooms/Baths", row.get("How many bedrooms & bathrooms?", ""))
    add("Sq Ft", row.get("What is the total square footage?", ""))
    add("Current Support", row.get("Do you currently have any household support (cleaners, nanny, meal prep, etc.)? Please list.", ""))
    add("Moving", row.get("Are you moving soon?", ""))
    move_date = row.get("When is the move in date?", "").strip()
    if move_date and move_date != "0":
        add("Move-In Date", move_date)
    add("Travel Frequency", row.get("How often do you travel (approx. # trips per year, typical duration)?", ""))
    add("Grocery Delivery", row.get("Do you already use grocery delivery (e.g., Instacart, Costco, Whole Foods)?", ""))
    add("Recurring Routines", row.get("Do you have any recurring routines in place already (e.g., cleaners every Friday, laundry pickup)?", ""))
    add("Comm Platform", row.get("What's your preferred communication platform?", ""))
    add("Budget", row.get("What's your hourly budget range for your house assistant? ", ""))

    pref = row.get("Would you prefer your house assistant to be:", "").strip()
    if pref:
        add("Schedule Preference", pref)

    add("Open to Full-Time", row.get("Are you open to having this role full time in the future?", ""))
    add("Open to Male HM", row.get("Are you open to having a male home manager?", ""))
    add("Desired Qualities", row.get("Are there any particular qualities or characteristics you'd love to see in your future Home Manager?", ""))
    add("Ideal Start", row.get("Ideal start date for your house assistant:", ""))
    add("Upcoming Travel", row.get("Any upcoming travel or commitments we should be aware of during the next 2 months?", ""))
    add("Payment Preference", row.get("How do you like to handle payments?", ""))

    profile_text = "\n".join(profile_lines)

    return {
        "first": fn, "last": ln,
        "email": email, "phone": phone,
        "address": address, "city": city, "state": state,
        "scheduling_link": sched,
        "onboarding_profile": profile_text,
        "submitted": row.get("Submit Date (UTC)", "")[:10],
    }


# ============================================================================
# Main sync logic
# ============================================================================

def sync_records(notion_clients, records, source_label, dry_run=False):
    """Sync parsed records to Notion. Only fills empty fields."""
    updated = 0
    skipped = 0
    not_found = 0

    for r in records:
        name = f"{r['first']} {r['last']}"
        match = find_match(notion_clients, r["first"], r["last"])

        if not match:
            print(f"  MISS  {name}")
            not_found += 1
            continue

        # Build updates — only fill empty fields
        updates = {}
        if r.get("email") and not match["email"]:
            updates["email"] = r["email"]
        if r.get("phone") and not match["phone"]:
            updates["phone"] = r["phone"]
        if r.get("address") and not match["address"]:
            updates["address"] = r["address"]
        if r.get("city") and not match["city"]:
            updates["city"] = r["city"]
        if r.get("state") and not match["state"]:
            updates["state"] = r["state"]
        if r.get("scheduling_link") and not match["scheduling_link"]:
            updates["scheduling_link"] = r["scheduling_link"]
        if r.get("caps") and not match["caps"]:
            updates["caps"] = r["caps"]
        if r.get("relational"):
            updates["relational"] = r["relational"]
        if r.get("autonomy"):
            updates["autonomy"] = r["autonomy"]
        if r.get("onboarding_profile") and not match["onboarding_profile"]:
            updates["onboarding_profile"] = r["onboarding_profile"]

        if not updates:
            print(f"  SKIP  {name} -> {match['name']} (all fields already populated)")
            skipped += 1
            continue

        print(f"  SYNC  {name} -> {match['name']} (updating: {', '.join(updates.keys())})")
        try:
            update_notion_client(match["page_id"], updates, dry_run=dry_run)
            updated += 1
        except Exception as e:
            print(f"    ERROR: {e}")

        time.sleep(0.35)

    return updated, skipped, not_found


def main():
    parser = argparse.ArgumentParser(description="Full CSV → Notion Sync")
    parser.add_argument("--v2", help="Path to V2 CSV (capability levels form)")
    parser.add_argument("--v1", help="Path to V1 CSV (original onboarding form)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write to Notion")
    args = parser.parse_args()

    if not args.v2 and not args.v1:
        parser.print_help()
        sys.exit(1)

    print("=== Full CSV → Notion Sync ===\n")

    # Step 1: Create Onboarding Profile property if needed
    print("[1] Ensuring 'Onboarding Profile' property exists...")
    create_onboarding_profile_property()

    # Step 2: Fetch all Notion clients
    print("[2] Fetching all Notion clients...")
    notion_clients = fetch_all_notion_clients()
    print(f"  Found {len(notion_clients)} clients\n")

    total_updated = 0
    total_skipped = 0
    total_missing = 0

    # Step 3: Process V1 CSV first (older data, V2 will overwrite if both exist)
    if args.v1:
        print(f"[3a] Processing V1 CSV: {args.v1}")
        with open(args.v1, newline="") as f:
            rows = list(csv.DictReader(f))
        print(f"  {len(rows)} rows\n")

        records = [r for r in (parse_v1_row(row) for row in rows) if r]
        # Dedup by email
        by_key = {}
        for r in records:
            key = r["email"] or f"{r['first']} {r['last']}"
            if key not in by_key or r["submitted"] > by_key[key]["submitted"]:
                by_key[key] = r
        records = sorted(by_key.values(), key=lambda x: f"{x['first']} {x['last']}")
        print(f"  {len(records)} unique records\n")

        u, s, m = sync_records(notion_clients, records, "V1", dry_run=args.dry_run)
        total_updated += u
        total_skipped += s
        total_missing += m
        print(f"\n  V1: {u} updated, {s} skipped, {m} not found\n")

        # Refresh Notion clients (fields may have been filled by V1)
        if u > 0 and not args.dry_run:
            print("  Refreshing Notion data after V1 sync...")
            notion_clients = fetch_all_notion_clients()

    # Step 4: Process V2 CSV (newer data with capabilities)
    if args.v2:
        print(f"[3b] Processing V2 CSV: {args.v2}")
        with open(args.v2, newline="") as f:
            rows = list(csv.DictReader(f))
        print(f"  {len(rows)} rows\n")

        records = [r for r in (parse_v2_row(row) for row in rows) if r]
        # Dedup by email
        by_key = {}
        for r in records:
            key = r["email"] or f"{r['first']} {r['last']}"
            if key not in by_key or r["submitted"] > by_key[key]["submitted"]:
                by_key[key] = r
        records = sorted(by_key.values(), key=lambda x: f"{x['first']} {x['last']}")
        print(f"  {len(records)} unique records\n")

        u, s, m = sync_records(notion_clients, records, "V2", dry_run=args.dry_run)
        total_updated += u
        total_skipped += s
        total_missing += m
        print(f"\n  V2: {u} updated, {s} skipped, {m} not found\n")

    # Summary
    print(f"\n{'='*60}")
    print(f"  TOTAL: {total_updated} updated, {total_skipped} already complete, {total_missing} not in Notion")
    if args.dry_run:
        print(f"  (DRY RUN — no changes made)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

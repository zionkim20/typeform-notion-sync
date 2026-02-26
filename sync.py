"""
Typeform API -> Notion Sync
Pulls all V2 responses via API, matches to Notion clients, updates records.
Syncs capability assessments, contact info, preferences, AND structured
onboarding profile data into individual Notion properties for downstream
agent consumption (job descriptions, trials, manual builds, endorsement packets).

Can be run manually or as a cron job via GitHub Actions.
Run with --verify to check data integrity across Notion client records.
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

# Validate required env vars (verify mode only needs Notion creds)
VERIFY_MODE = "--verify" in sys.argv
if not VERIFY_MODE:
    missing = []
    for var_name in ["TYPEFORM_TOKEN", "TYPEFORM_FORM_ID", "NOTION_TOKEN", "NOTION_DB_ID"]:
        if not os.environ.get(var_name):
            missing.append(var_name)
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
else:
    if not NOTION_TOKEN or not NOTION_DB_ID:
        missing = [v for v in ["NOTION_TOKEN", "NOTION_DB_ID"] if not os.environ.get(v)]
        print(f"ERROR: Verify mode requires: {', '.join(missing)}")
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

# Contact info field IDs (verified from form gFojEmRj)
CONTACT_FIELDS = {
    "phone": "2ALk2W41peZh",
    "street": "z4khG2SM40Kc",
    "address_line_2": "u9FPkYqHQ1cy",
    "city": "RhPRRCt4uxa4",
    "state": "BndPdkJ1HDpQ",
}

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

# Form field titles (populated at runtime from form definition)
FORM_FIELD_TITLES = {}  # field_id -> question_title

# === Structured Profile Routing ===
# Maps keyword in question title -> (internal_key, sub_label)
# internal_key determines which Notion property the answer goes to
# sub_label is a prefix when multiple questions feed the same property
PROFILE_ROUTING = [
    ("household members", "household_members", None),
    ("do you have pets", "pets", None),
    ("type of pets", "pets", "Details"),
    ("bedrooms and bathrooms", "home_size", None),
    ("square footage", "home_size", "Sq Ft"),
    ("pain points", "pain_points", None),
    ("ideal start date", "ideal_start", None),
    ("when would you ideally want", "preferred_hours", None),
    ("don't want support", "off_limits_times", None),
    ("special household considerations", "special_considerations", None),
    ("typical weekday", "routines", "Weekday"),
    ("typical weekend", "routines", "Weekend"),
    ("work schedules", "routines", "Work"),
    ("school schedule", "kids_activities", "School"),
    ("after-school", "kids_activities", "Activities"),
    # Lower-frequency fields -> household_notes catch-all
    ("household support", "household_notes", "Current Support"),
    ("describe your current support", "household_notes", "Support Details"),
    ("keep this support or transition", "household_notes", "Keep/Transition"),
    ("moving soon", "household_notes", "Moving"),
    ("upcoming travel", "household_notes", "Travel"),
    ("trash", "household_notes", "Trash/Recycling"),
    ("routine vendors", "household_notes", "Vendors"),
    ("shows up in your home", "household_notes", "Style"),
    ("fitness or wellness", "household_notes", "Wellness"),
    ("chaotic", "household_notes", "Chaotic Areas"),
    ("recurring friction", "household_notes", "Friction"),
    ("restored and relaxed", "household_notes", "Restoration"),
    ("anything else", "household_notes", "Other"),
]

# Internal key -> Notion property name
PROFILE_NOTION_PROPERTIES = {
    "household_members": "Household Members",
    "pets": "Pets",
    "home_size": "Home Size",
    "pain_points": "Pain Points",
    "ideal_start": "Ideal Start",
    "preferred_hours": "Preferred Hours",
    "off_limits_times": "Off-Limits Times",
    "special_considerations": "Special Considerations",
    "routines": "Routines",
    "kids_activities": "Kids & Activities",
    "household_notes": "Household Notes",
}


# ============================================================================
# Helper functions
# ============================================================================

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


def extract_answer_text(answer):
    """Extract displayable text from any Typeform answer type (for profile building)."""
    atype = answer.get("type", "")
    if atype == "text":
        return answer.get("text", "").strip()
    elif atype == "email":
        return answer.get("email", "").strip()
    elif atype == "phone_number":
        return answer.get("phone_number", "").strip()
    elif atype == "choice":
        return answer.get("choice", {}).get("label", "").strip()
    elif atype == "choices":
        labels = answer.get("choices", {}).get("labels", [])
        return ", ".join(l for l in labels if l)
    elif atype == "number":
        num = answer.get("number")
        if num is None:
            return ""
        return str(int(num)) if num == int(num) else str(num)
    elif atype == "boolean":
        return "Yes" if answer.get("boolean") else "No"
    elif atype == "date":
        return answer.get("date", "").strip()
    elif atype == "url":
        return answer.get("url", "").strip()
    elif atype == "file_url":
        return answer.get("file_url", "").strip()
    return ""


def route_answer_to_profile(title):
    """Route a Typeform question to the correct profile property via keyword matching.
    Returns (internal_key, sub_label) or (None, None) if no match."""
    if not title:
        return None, None
    lower = title.lower()
    for keyword, internal_key, sub_label in PROFILE_ROUTING:
        if keyword in lower:
            return internal_key, sub_label
    return None, None


# ============================================================================
# Typeform API functions
# ============================================================================

def fetch_form_definition():
    """Fetch the Typeform form definition to get field IDs and question titles."""
    global FORM_FIELD_TITLES
    url = f"https://api.typeform.com/forms/{TYPEFORM_FORM_ID}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TYPEFORM_TOKEN}"})
    resp = urllib.request.urlopen(req)
    form = json.loads(resp.read().decode())

    for field in form.get("fields", []):
        FORM_FIELD_TITLES[field["id"]] = field.get("title", "")
        # Handle nested fields inside groups
        for sub in field.get("properties", {}).get("fields", []):
            FORM_FIELD_TITLES[sub["id"]] = sub.get("title", "")

    print(f"  Loaded {len(FORM_FIELD_TITLES)} field titles from form definition")


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


# ============================================================================
# Notion API functions
# ============================================================================

def ensure_profile_properties():
    """Create all structured profile properties in Notion DB if they don't exist."""
    req = urllib.request.Request(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2022-06-28",
        }
    )
    resp = urllib.request.urlopen(req)
    db = json.loads(resp.read().decode())

    existing = set(db.get("properties", {}).keys())
    to_create = {}

    for notion_prop in PROFILE_NOTION_PROPERTIES.values():
        if notion_prop not in existing:
            to_create[notion_prop] = {"rich_text": {}}

    if not to_create:
        print(f"  All {len(PROFILE_NOTION_PROPERTIES)} profile properties exist")
        return True

    print(f"  Creating {len(to_create)} properties: {', '.join(to_create.keys())}")
    body = json.dumps({"properties": to_create}).encode()
    req = urllib.request.Request(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        data=body, method="PATCH",
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
    )
    urllib.request.urlopen(req)
    print("  Created!")
    return True


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

            # Post-filter: verify the search term matches as a whole word
            # Prevents "Craft" matching "Beacraft" via Notion's substring contains
            word_pattern = re.compile(r'\b' + re.escape(search_term.lower()) + r'\b')
            results = [r for r in results if word_pattern.search(
                "".join(t.get("plain_text", "") for t in
                        r.get("properties", {}).get("Task name", {}).get("title", [])).lower()
            )]

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

                # Read all profile properties
                for internal_key, notion_prop in PROFILE_NOTION_PROPERTIES.items():
                    rt = props.get(notion_prop, {}).get("rich_text", [])
                    current[internal_key] = "".join(
                        t.get("plain_text", "") for t in rt
                    ).strip()

                return page["id"], notion_name, current
        except Exception as e:
            print(f"  Search error for '{search_term}': {e}")

        time.sleep(0.35)

    return None, None, {}


def update_notion(page_id, record, current_values):
    """Update Notion client record with typeform data."""
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

    # Profile fields — only fill if currently empty (preserves manual edits)
    for internal_key, notion_prop in PROFILE_NOTION_PROPERTIES.items():
        value = record.get("profile_fields", {}).get(internal_key, "")
        if value and not current_values.get(internal_key):
            properties[notion_prop] = {
                "rich_text": [{"text": {"content": value[:2000]}}]
            }

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


# ============================================================================
# Response parsing
# ============================================================================

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

    # Build structured profile fields from remaining answers
    known_ids = set(CAPABILITY_FIELDS.keys()) | {
        FIELD_FIRST_NAME, FIELD_LAST_NAME, FIELD_EMAIL,
        FIELD_RELATIONAL, FIELD_AUTONOMY,
    } | set(CONTACT_FIELDS.values())

    profile_data = {}  # internal_key -> list of (sub_label, value)

    for a in answers:
        fid = a.get("field", {}).get("id", "")
        if fid in known_ids or not fid:
            continue

        value = extract_answer_text(a)
        if not value or value.lower() in ("n/a", "none", "0", "", "i don't have any pets."):
            continue

        # Route to the correct profile property
        title = FORM_FIELD_TITLES.get(fid, "")
        internal_key, sub_label = route_answer_to_profile(title)

        if internal_key:
            if internal_key not in profile_data:
                profile_data[internal_key] = []
            profile_data[internal_key].append((sub_label, value))
        elif title and value:
            # Unrecognized field -> household_notes catch-all
            if "household_notes" not in profile_data:
                profile_data["household_notes"] = []
            clean = title.rstrip("?").strip()[:50]
            profile_data["household_notes"].append((clean, value))

    # Compile each profile bucket into a string
    profile_fields = {}
    for internal_key, entries in profile_data.items():
        parts = []
        for sub_label, value in entries:
            if sub_label:
                parts.append(f"{sub_label}: {value}")
            else:
                parts.append(value)
        profile_fields[internal_key] = "\n".join(parts)

    has_profile = any(profile_fields.values())
    has_data = bool(cap_string or relational or autonomy or email or phone or address or city or has_profile)
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
        "profile_fields": profile_fields,
        "submitted": submitted[:10] if submitted else "",
        "response_id": response_id,
        "completed": completed,
    }


# ============================================================================
# Data integrity verification
# ============================================================================

def fetch_all_notion_clients():
    """Fetch all client records from Notion with all field values."""
    all_results = []
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data_body = json.dumps(body).encode()
        req = urllib.request.Request(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            data=data_body, method="POST",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
        )
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode())
        all_results.extend(data.get("results", []))
        if data.get("has_more"):
            cursor = data["next_cursor"]
        else:
            break
        time.sleep(0.35)
    return all_results


def verify_data():
    """Check data integrity across all Notion client records."""
    print("=== Data Integrity Check ===\n")

    # Step 1: Fetch all clients
    print("[1] Fetching all Notion clients...")
    all_results = fetch_all_notion_clients()
    print(f"  Found {len(all_results)} total client records\n")

    # Define all tracked fields
    core_fields = [
        ("Email", "email"),
        ("Phone", "phone_number"),
        ("Client Address", "rich_text"),
        ("City", "rich_text"),
        ("State", "select"),
        ("Capability Requirements", "rich_text"),
        ("Relational Preference", "select"),
        ("Decision Autonomy", "select"),
    ]

    profile_fields = [(prop, "rich_text") for prop in PROFILE_NOTION_PROPERTIES.values()]

    all_fields = core_fields + profile_fields + [("Typeform Status", "select")]

    # Step 2: Analyze each client
    field_stats = {f[0]: {"filled": 0, "empty": 0, "truncated": 0} for f in all_fields}
    complete_clients = []
    partial_clients = []
    no_form_clients = []
    issues = []

    for page in all_results:
        props = page.get("properties", {})
        title_parts = props.get("Task name", {}).get("title", [])
        name = "".join(t.get("plain_text", "") for t in title_parts).strip()

        tf_status = (props.get("Typeform Status", {}).get("select") or {}).get("name", "")

        if tf_status == "Complete":
            complete_clients.append(name)
        elif tf_status == "Partial":
            partial_clients.append(name)
        else:
            no_form_clients.append(name)

        for field_name, field_type in all_fields:
            prop = props.get(field_name, {})
            value = ""

            if field_type == "email":
                value = prop.get("email", "") or ""
            elif field_type == "phone_number":
                value = prop.get("phone_number", "") or ""
            elif field_type == "rich_text":
                rt = prop.get("rich_text", [])
                value = "".join(t.get("plain_text", "") for t in rt).strip()
            elif field_type == "select":
                value = (prop.get("select") or {}).get("name", "")

            if value:
                field_stats[field_name]["filled"] += 1
                if len(value) >= 1990:
                    field_stats[field_name]["truncated"] += 1
            else:
                field_stats[field_name]["empty"] += 1

        # Check complete-form clients for missing critical fields
        if tf_status == "Complete":
            critical = ["Capability Requirements", "Relational Preference", "Decision Autonomy"]
            missing_critical = []
            for cf in critical:
                prop = props.get(cf, {})
                if cf in ("Relational Preference", "Decision Autonomy"):
                    val = (prop.get("select") or {}).get("name", "")
                else:
                    val = "".join(t.get("plain_text", "") for t in prop.get("rich_text", [])).strip()
                if not val:
                    missing_critical.append(cf)
            if missing_critical:
                issues.append((name, missing_critical))

    total = len(all_results)

    # Step 3: Report
    print(f"[2] Typeform Status Distribution:")
    print(f"  Complete:  {len(complete_clients)}")
    print(f"  Partial:   {len(partial_clients)}")
    print(f"  No form:   {len(no_form_clients)}")
    print()

    print(f"[3] Field Fill Rates (across {total} clients):")
    print(f"  {'Field':<28} {'Filled':>7} {'Empty':>7} {'Rate':>6} {'Trunc':>6}")
    print(f"  {'-' * 56}")

    for field_name, _ in all_fields:
        stats = field_stats[field_name]
        filled = stats["filled"]
        empty = stats["empty"]
        rate = (filled / total * 100) if total > 0 else 0
        trunc = stats["truncated"]

        flags = ""
        if trunc > 0:
            flags += " [TRUNCATED]"

        print(f"  {field_name:<28} {filled:>7} {empty:>7} {rate:>5.0f}% {trunc:>5}{flags}")

    # Step 4: Critical field integrity
    print(f"\n[4] Critical Field Integrity (Complete-form clients):")
    if not issues:
        print(f"  All {len(complete_clients)} complete-form clients have critical fields populated")
    else:
        for name, missing_fields in issues:
            print(f"  MISSING  {name}: {', '.join(missing_fields)}")
        print(f"\n  {len(issues)} of {len(complete_clients)} complete-form clients have gaps")

    # Step 5: Capability format check
    print(f"\n[5] Capability Format Consistency:")
    valid_pattern = re.compile(r'^([A-Za-z &]+: (L[1-4]|N/A)(, )?)+$')
    bad_caps = 0
    for page in all_results:
        props = page.get("properties", {})
        cap_rt = props.get("Capability Requirements", {}).get("rich_text", [])
        cap_val = "".join(t.get("plain_text", "") for t in cap_rt).strip()
        if cap_val and not valid_pattern.match(cap_val):
            title_parts = props.get("Task name", {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in title_parts).strip()
            print(f"  NON-STANDARD  {name}: {cap_val[:60]}...")
            bad_caps += 1

    if bad_caps == 0:
        print(f"  All capability strings use standard L1-L4/N/A format")

    # Step 6: Profile completeness for complete-form clients
    print(f"\n[6] Profile Completeness (Complete-form clients):")
    high_value_keys = ["household_members", "pets", "home_size", "pain_points",
                       "preferred_hours", "special_considerations", "routines"]
    high_value_props = [PROFILE_NOTION_PROPERTIES[k] for k in high_value_keys]

    profile_scores = []
    for page in all_results:
        props = page.get("properties", {})
        tf_status = (props.get("Typeform Status", {}).get("select") or {}).get("name", "")
        if tf_status != "Complete":
            continue

        filled_count = 0
        for prop_name in high_value_props:
            rt = props.get(prop_name, {}).get("rich_text", [])
            val = "".join(t.get("plain_text", "") for t in rt).strip()
            if val:
                filled_count += 1

        score = (filled_count / len(high_value_props)) * 100
        title_parts = props.get("Task name", {}).get("title", [])
        name = "".join(t.get("plain_text", "") for t in title_parts).strip()
        profile_scores.append((name, score, filled_count))

    if profile_scores:
        avg_score = sum(s[1] for s in profile_scores) / len(profile_scores)
        full_profiles = sum(1 for s in profile_scores if s[1] == 100)
        empty_profiles = sum(1 for s in profile_scores if s[1] == 0)

        print(f"  Average profile completeness: {avg_score:.0f}%")
        print(f"  Fully populated: {full_profiles}/{len(profile_scores)}")
        print(f"  Empty profiles:  {empty_profiles}/{len(profile_scores)}")

        # Show clients with incomplete profiles
        incomplete = [(n, s, c) for n, s, c in profile_scores if 0 < s < 100]
        if incomplete:
            print(f"\n  Partially complete profiles:")
            for name, score, count in sorted(incomplete, key=lambda x: x[1]):
                print(f"    {name}: {score:.0f}% ({count}/{len(high_value_props)} high-value fields)")

        if empty_profiles > 0:
            print(f"\n  Clients with zero profile data (may need re-sync):")
            for name, score, _ in profile_scores:
                if score == 0:
                    print(f"    {name}")
    else:
        print(f"  No complete-form clients found")

    print(f"\n{'=' * 60}")
    print(f"  SUMMARY: {total} clients, {len(complete_clients)} with complete forms")
    if profile_scores:
        avg = sum(s[1] for s in profile_scores) / len(profile_scores)
        print(f"  Profile data health: {avg:.0f}% average completeness")
    print(f"  Critical field issues: {len(issues)}")
    print(f"{'=' * 60}")


# ============================================================================
# Main sync
# ============================================================================

def main():
    print("=== Typeform V2 -> Notion Sync (Structured) ===\n")

    # Step 0: Fetch form definition for profile field routing
    print("[0] Fetching form definition for profile field mapping...")
    try:
        fetch_form_definition()
    except Exception as e:
        print(f"  WARNING: Could not fetch form definition: {e}")
        print("  Profile fields will route to catch-all")
    print()

    # Step 0b: Ensure all profile properties exist in Notion
    print("[0b] Checking Notion profile properties...")
    try:
        ensure_profile_properties()
    except Exception as e:
        print(f"  WARNING: Could not check/create properties: {e}")
        print("  Profile data may fail to write if properties don't exist")
    print()

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
            by_key[key] = r
        elif r["completed"] == existing["completed"] and r["submitted"] > existing["submitted"]:
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
    match_warnings = 0

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

        # Show profile fields in logs
        pf = r.get("profile_fields", {})
        if pf:
            filled = [PROFILE_NOTION_PROPERTIES.get(k, k) for k in pf if pf[k]]
            print(f"  Profile: {len(filled)} fields ({', '.join(filled[:4])}{'...' if len(filled) > 4 else ''})")

        page_id, notion_name, current = find_notion_client(r["last"], r["first"])
        time.sleep(0.35)

        if not page_id:
            print(f"  NOT FOUND in Notion")
            not_found += 1
            print()
            continue

        # Match quality check: warn if the matched name looks suspicious
        tf_name = r["name"].lower()
        notion_lower = notion_name.lower()
        name_ok = (r["first"].lower() in notion_lower and r["last"].lower() in notion_lower)
        if not name_ok:
            print(f"  MATCH WARNING: '{r['name']}' matched to '{notion_name}' — names don't fully overlap")
            match_warnings += 1

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
    if match_warnings:
        print(f"MATCH WARNINGS: {match_warnings} — review matches above")
    print(f"Total unique responses processed: {len(records)}")

    # Exit with error code if everything failed
    if updated == 0 and len(records) > 0 and skipped == len(records):
        sys.exit(1)


if __name__ == "__main__":
    if VERIFY_MODE:
        verify_data()
    else:
        main()

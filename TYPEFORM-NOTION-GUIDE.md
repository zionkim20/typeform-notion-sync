# Typeform to Notion Sync — How It Works

## What This Does

Every 6 hours, an automated script pulls client onboarding data from our Typeform and writes it into structured fields on each client's Notion record. **Every client who fills out the onboarding form automatically gets their data organized in Notion** — no manual copy-paste needed.

The sync also runs on-demand from the GitHub Actions UI whenever we need a fresh pull.

---

## What Data Gets Synced

The Typeform collects ~40 data points per client. The sync organizes them into **three categories** in Notion:

### 1. Core Fields (always overwritten with latest)

These update every sync — if a client resubmits, we get the freshest data.

| Notion Field | What It Contains | Example |
|---|---|---|
| Capability Requirements | Service levels for 9 areas | Cleaning: L2, Cooking: L3, Pet Care: L1 |
| Relational Preference | How they want HM to interact | Relational / Engaged |
| Decision Autonomy | How much judgment they want | Judgment-Oriented |
| Typeform Status | Complete or Partial | Complete |

### 2. Contact Fields (fill only if empty)

These only write if the Notion field is currently blank — so manual edits or data from other sources are never overwritten.

| Notion Field | Example |
|---|---|
| Email | jane@example.com |
| Phone | +1 (555) 123-4567 |
| Client Address | 123 Main St, Apt 4B |
| City | Austin |
| State | Texas |

### 3. Structured Profile Fields (fill only if empty)

These are the household profile questions — the rich context that feeds into job descriptions, house manuals, trials, and endorsement packets.

| Notion Field | What It Contains | Example |
|---|---|---|
| **Household Members** | Who lives there | 2 adults, 3 kids (ages 4, 7, 10) |
| **Pets** | Animals + breed/size | 2 dogs — Golden Retriever (60lbs), Lab (70lbs) |
| **Home Size** | Beds/baths + sq ft | 4 bed / 3 bath, 3,200 sqft |
| **Pain Points** | Top frustrations | Laundry piling up, grocery runs, no system for activities |
| **Ideal Start** | Target start date | March 2026 |
| **Preferred Hours** | When they want support | 9am-3pm while kids at school |
| **Off-Limits Times** | When they don't | Before 8am, after dinner |
| **Special Considerations** | Allergies, safety notes | Nut allergy (youngest), no shoes in house |
| **Routines** | Weekday + weekend + work schedules | Weekday: both WFH, kids school 8-3. Weekend: usually home |
| **Kids & Activities** | School + extracurriculars | Elementary school, soccer Tue/Thu, piano Wed |
| **Household Notes** | Everything else (vendors, trash, wellness, etc.) | Vendors: lawn service Thursdays. Trash: Monday AM |

---

## Why Structured Fields Instead of One Big Text Block

We originally planned to put all profile data in a single "Onboarding Profile" text blob. We restructured it into individual fields because:

1. **Agents can read specific fields directly** — a JD generator just reads "Pain Points" and "Household Members" instead of parsing a text wall
2. **Notion views can filter by field** — e.g., "show all clients with pets" or "clients wanting morning hours"
3. **House manual builders** can pull routines, vendors, and kids schedules programmatically
4. **Endorsement packets** can pull household context and pain points without guessing
5. **Data quality is measurable** — we can check which fields are empty and fix gaps

---

## How to Run

### Automatic (Default)
Runs every 6 hours via GitHub Actions. No action needed — new form submissions are picked up automatically.

### Manual Sync
1. Go to the repo: **github.com/zionkim20/typeform-notion-sync**
2. Click **Actions** tab
3. Click **Typeform → Notion Sync** in the left sidebar
4. Click **Run workflow** button
5. Select **sync** from the dropdown
6. Click the green **Run workflow** button

### Data Integrity Check
Same steps as above, but select **verify** instead of sync. This runs a **read-only** report that shows:
- How many clients have completed forms
- Fill rate for every field (what % of clients have data)
- Any clients with completed forms but missing critical data
- Profile completeness score per client
- Truncation warnings (fields hitting the 2000-character limit)

---

## What Downstream Workflows Use This Data

| Workflow | Fields It Pulls |
|---|---|
| **Job Descriptions** | Household Members, Pets, Home Size, Pain Points, Capabilities, Preferred Hours |
| **Trial Planning** | Routines, Preferred Hours, Off-Limits Times, Special Considerations, Kids & Activities |
| **House Manual Builds** | Routines, Kids & Activities, Household Notes (vendors, trash, wellness), Pets, Special Considerations |
| **Endorsement Packets** | Household Members, Pain Points, Capabilities, Relational Preference |
| **HM Matching** | Pets, Home Size, City/State, Preferred Hours, Capabilities |

---

## Current Data Health (as of Feb 21, 2026)

- **75 total clients** in Notion
- **11 have completed** the Typeform onboarding form
- **100% profile completeness** for all 11 clients with completed forms
- **0 critical field issues**
- **5 truncation warnings** (some Routines and Household Notes are long — no important data lost)

---

## FAQ

**Q: What if a client resubmits the form?**
Capabilities, preferences, and Typeform Status will update to the latest. Contact info and profile fields will NOT overwrite (to protect manual edits). If you need to force a refresh, clear the specific Notion field and the next sync will fill it.

**Q: What if a client isn't found in Notion?**
The sync logs them as "NOT FOUND" — they need to be added to the Notion database first (by name). The sync matches by last name, then first name.

**Q: Can I see what the sync did?**
Yes — go to GitHub Actions, click on any completed run, and expand the "Run sync" step. It shows every client processed and exactly which fields were written.

**Q: What about clients who only partially filled out the form?**
Partial submissions are processed too — whatever data they provided gets synced. Their Typeform Status shows "Partial" so you know they didn't finish.

**Q: What if new questions are added to the Typeform?**
They'll automatically be picked up and routed to the "Household Notes" catch-all field. No code changes needed. To route them to a dedicated field, we update the routing table in the code.

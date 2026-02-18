# Typeform -> Notion Sync

Automated sync of client capability assessments from Typeform to Notion.

## What it does

1. Fetches all completed responses from the HUM Client Capability Assessment (Typeform V2 API)
2. Parses 9 capability levels (Cleaning, Laundry, Cooking, Pet Care, Childcare, Grocery, Vehicle, Organization, House Mgmt) plus Relational Preference and Decision Autonomy
3. Deduplicates by email (keeps latest submission per client)
4. Fuzzy-matches each response to a Notion client record by last name, then first name
5. Updates Capability Requirements, Relational Preference, and Decision Autonomy fields in Notion

## Schedule

Runs every 6 hours via GitHub Actions. Can also be triggered manually from the Actions tab.

## Setup

### GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `TYPEFORM_TOKEN` | Typeform personal access token |
| `TYPEFORM_FORM_ID` | Typeform form ID for the capability assessment |
| `NOTION_TOKEN` | Notion integration token |
| `NOTION_DB_ID` | Notion database ID for the client tracker |

### Local Development

```bash
export TYPEFORM_TOKEN="your-token"
export TYPEFORM_FORM_ID="your-form-id"
export NOTION_TOKEN="your-notion-token"
export NOTION_DB_ID="your-db-id"
python sync.py
```

## Dependencies

No external dependencies required. Uses only Python standard library (`urllib`, `json`, `re`).

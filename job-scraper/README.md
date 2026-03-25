# Job Search Scraper

Searches job APIs and imports results directly into your `jobs.db` dashboard database.

---

## Setup

### 1. Install dependencies

```bash
cd job-scraper
pip install -r requirements.txt
```

### 2. Get your API key(s)

#### JSearch (recommended — free tier: 200 requests/month)

1. Go to https://rapidapi.com and create a free account
2. Search for **"JSearch"** in the API marketplace
3. Click **Subscribe to Test** → select the **Basic (free)** plan
4. Go to the **Endpoints** tab — your API key is shown on the right under **Header Parameters** as `X-RapidAPI-Key`
5. Copy it into `search_config.json`:

```json
"jsearch": {
  "enabled": true,
  "api_key": "PASTE_YOUR_KEY_HERE"
}
```

#### Adzuna (optional backup — free tier: 250 requests/month)

1. Go to https://developer.adzuna.com and create a free account
2. Create a new application to get an `app_id` and `app_key`
3. Paste both into `search_config.json` and set `"enabled": true`

---

## Usage

```bash
# Run all search profiles
python job_scraper.py

# Run one specific profile
python job_scraper.py --profile "Disaster Recovery / Emergency Management"

# Preview what would be imported — nothing written to DB
python job_scraper.py --dry-run

# Skip deduplication and import everything
python job_scraper.py --force

# Point to a different database
python job_scraper.py --db /path/to/jobs.db

# Import from a CSV file
python job_scraper.py --import-csv my-jobs.csv
```

---

## CSV Import

The `--import-csv` command accepts a CSV with any of these column headers (case-insensitive):

| CSV Column | Maps To |
|---|---|
| `company` or `company_name` | Company name |
| `role`, `title`, `job_title` | Role title |
| `status` | Status (defaults to "Researching") |
| `location` | Location |
| `work_type` | Remote / Hybrid / On-site |
| `source` | Where you found it |
| `url`, `link`, `job_url` | Job posting URL |
| `salary_min`, `salary_max` | Salary range |
| `notes` | Notes |
| `priority` | High / Medium / Low |
| `date_applied` | Date applied (YYYY-MM-DD) |

Only `company` and `role` are required. Everything else is optional.

---

## Customising searches

Edit `search_config.json`:

- **Add queries** — add strings to the `queries` array under any profile
- **Add target companies** — jobs from these companies get boosted to High priority
- **Add a new profile** — copy an existing profile block and change name/queries/targets
- **Adjust filters** — tweak `exclude_keywords`, `min_salary`, `work_types`, `days_posted`

---

## Output

Each run prints a summary and saves a JSON log to `scraper_logs/YYYY-MM-DD_HH-MM.json`:

```
==================================================
  SCRAPER RUN SUMMARY
==================================================
  Profiles run:       2
  Queries executed:   11
  Raw results found:  187
  After filters:      94
  New jobs added:     31
  Duplicates skipped: 58
  Flagged for review: 5
==================================================
```

**Flagged jobs** are near-duplicates (fuzzy match score 70–84%). They are NOT imported automatically — review them manually and use `--force` if you want to import anyway.

---

## Rate limits

| API | Free tier | Notes |
|---|---|---|
| JSearch | 200 req/month | 1 query = 1–2 requests depending on result count |
| Adzuna | 250 req/month | 1 query = 1 request |

The scraper adds a 1-second delay between requests. With default config (11 queries across 2 profiles), one full run uses ~15–20 JSearch requests.

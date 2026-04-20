# reno-agents

Autonomous AI pipeline for identifying undervalued renovation properties across Australia. Runs daily on Railway and sends email alerts for viable deals.

## How it works

```
Apify (Domain listings)
    → Text classification     (keyword filter + Claude Haiku fallback)
    → Photo classification    (heuristic room detection + Claude Haiku vision)
    → Vision scoring          (Claude Haiku scores kitchen/bathroom 1–10 on reno need)
    → Suburb gap filter       (skip if renovated vs unrenovated price gap < 20%)
    → Insights agent          (Claude Sonnet feasibility analysis → GO / WATCH / PASS)
    → Email digest            (Gmail SMTP alert for GO / WATCH verdicts)
    → Supabase logging        (pipeline run stats + listing verdicts)
```

The insights agent receives pre-calculated feasibility numbers (stamp duty, holding costs, reno cost estimate) and returns a structured JSON verdict with best/base/worst profit scenarios.

## Project structure

```
config.py                   # API keys, target suburbs, filters, feasibility constants
jobs/
  daily_run.py              # Main orchestrator — run this daily
  backfill.py               # Pull historical sold listings
sources/
  domain.py                 # Fetch new listings via Apify
classifiers/
  text.py                   # Keyword + Claude Haiku text classification
  photos.py                 # Heuristic + Claude vision room identification
  vision.py                 # Claude Haiku vision scoring (reno need 1–10)
agents/
  insights.py               # Claude Sonnet feasibility agent
analysis/
  suburb_gaps.py            # Compute renovated vs unrenovated price gap per suburb
alerts/
  email.py                  # Gmail SMTP digest sender
db/
  client.py                 # Supabase client
```

## Setup

1. Clone the repo and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file with the following keys:
   ```
   ANTHROPIC_API_KEY=
   SUPABASE_URL=
   SUPABASE_ANON_KEY=
   APIFY_API_TOKEN=
   ALERT_EMAIL=
   ALERT_EMAIL_PASSWORD=
   ```

3. Populate suburb gap data before the first run:
   ```bash
   python analysis/suburb_gaps.py
   ```

4. Run the pipeline:
   ```bash
   python jobs/daily_run.py
   ```

## CLI options

```bash
python jobs/daily_run.py --dry-run          # Skip emails and DB writes
python jobs/daily_run.py --skip-fetch       # Re-check existing DB listings only
python jobs/daily_run.py --skip-insights    # Use cached verdicts (no Claude Sonnet calls)
python jobs/daily_run.py --gap-min 25       # Override minimum suburb gap % (default: 20)
```

## Feasibility model

The insights agent uses the following cost assumptions:

| Cost | Value |
|------|-------|
| Stamp duty | ~4% of purchase price (TAS) |
| Conveyancing | $2,000 flat |
| Holding period | 5 months at 0.5%/month |
| Marketing/selling | $3,000 flat |
| Profit target | 10% margin on total outlay |

Reno cost is estimated from the average vision score:

| Score range | Tier | Estimated cost |
|-------------|------|----------------|
| ≥ 3.5 | Cosmetic | $25,000 |
| 2.0–3.5 | Standard | $55,000 |
| < 2.0 | Full | $80,000 |

## Target markets

Evaluates deals across Australia. Currently configured for Tasmania (Greater Hobart, Greater Launceston, Devonport, Ulverstone), with NSW and QLD support planned.

## Stack

- **Python** — pipeline orchestration
- **Anthropic Claude** — Haiku for classification, Sonnet for feasibility analysis
- **Apify** — Domain.com.au property scraper
- **Supabase** — listings database and run logging
- **Railway** — daily cron deployment
- **Gmail SMTP** — deal alert emails

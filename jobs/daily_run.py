# jobs/daily_run.py
"""
Daily pipeline orchestrator.

Flow:
  1. Fetch new for-sale listings via Apify (all target suburbs)
  2. Text classify each listing (keywords → Claude fallback)
  3. Photo classify each listing (heuristic → Claude fallback)
  4. Vision score kitchen + bathroom
  5. Check suburb gap — skip if < MIN_GAP_PCT
  6. Run insights agent (Claude Sonnet feasibility analysis)
  7. Email alert for GO / WATCH verdicts
  8. Log run summary to Supabase
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import traceback
from datetime import datetime
from typing import Optional

from sources.domain import fetch_new_listings
from classifiers.text import classify_listing_text
from classifiers.photos import process_listing_photos
from classifiers.vision import score_listing_renovation
from agents.insights import analyse_listing, print_analysis
from alerts.email import send_alert
from db.client import supabase

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
MIN_GAP_PCT = 20.0          # Only run insights agent on suburbs with 20%+ gap
ALERT_VERDICTS = {"GO", "WATCH"}
DRY_RUN = False             # Set True to skip emails and DB logging


# ─────────────────────────────────────────
# SUBURB GAP CACHE
# Load all suburb gaps once at the start of each run
# ─────────────────────────────────────────
def load_suburb_gaps() -> dict:
    """
    Load all suburb gap data from Supabase into a dict keyed by suburb name.
    Returns: {"Devonport": {...}, "Launceston": {...}, ...}
    """
    try:
        result = supabase.table("suburb_gaps").select("*").execute()
        gaps = {}
        for row in result.data:
            suburb = row["suburb"].strip().title()
            gaps[suburb] = row
        print(f"✓ Loaded gap data for {len(gaps)} suburbs")
        return gaps
    except Exception as e:
        print(f"✗ Failed to load suburb gaps: {e}")
        return {}


def refresh_sold_data():
    """Pull latest sold listings and update suburb gap data."""
    from jobs.backfill import run_backfill_regions
    from analysis.suburb_gaps import run_gap_analysis

    print("\n[0/3] Refreshing sold data...\n")
    try:
        run_backfill_regions()
        print("\n→ Updating suburb gap analysis...")
        run_gap_analysis(min_sales=5)
        print("✓ Sold data refresh complete\n")
    except Exception as e:
        print(f"✗ Sold data refresh failed: {e} — continuing with existing data\n")


def get_gap_for_suburb(suburb: str, gaps: dict) -> Optional[dict]:
    """Look up suburb gap, trying title case and stripped variants."""
    return gaps.get(suburb.strip().title())


# ─────────────────────────────────────────
# PIPELINE: PROCESS A SINGLE LISTING
# ─────────────────────────────────────────
def process_listing(listing: dict, suburb_gaps: dict) -> Optional[dict]:
    """
    Run the full classification + analysis pipeline on one listing.
    Returns analysis dict if alert-worthy, None otherwise.
    """
    listing_id = listing.get("id")
    address = listing.get("address", "Unknown")
    suburb = listing.get("suburb", "")
    price = listing.get("price", 0)

    print(f"\n  ── {address} ──")
    print(f"     ${price:,}  |  {suburb}")

    # ── Step 1: Check suburb gap before doing any expensive work ──
    gap_data = get_gap_for_suburb(suburb, suburb_gaps)
    if not gap_data:
        print(f"     ⚠ No gap data for {suburb} — skipping")
        return None

    gap_pct = float(gap_data.get("gap_percent", 0))
    if gap_pct < MIN_GAP_PCT:
        print(f"     ⚠ Gap {gap_pct}% < {MIN_GAP_PCT}% threshold — skipping")
        return None

    print(f"     ✓ Suburb gap: {gap_pct}% — proceeding")

    # ── Step 2: Text classification ──
    description = listing.get("description", "")
    try:
        text_result = classify_listing_text(listing_id, description)
        listing["text_renovation_signals"] = {
            "classification": text_result.get("classification"),
            "signals": text_result.get("signals", []),
            "confidence": text_result.get("confidence", 0),
        }
    except Exception as e:
        print(f"     ✗ Text classification error: {e}")
        listing["text_renovation_signals"] = {}

    # ── Step 3: Photo processing ──
    photo_urls = listing.get("_photo_urls", [])
    found_rooms = {}
    try:
        if photo_urls:
            found_rooms = process_listing_photos(listing_id, photo_urls)
        else:
            print(f"     ⚠ No photos available")
    except Exception as e:
        print(f"     ✗ Photo processing error: {e}")

    # ── Step 4: Vision scoring ──
    avg_reno_score = 2.5  # default if vision fails
    try:
        if found_rooms:
            vision_result = score_listing_renovation(listing_id)
            avg_reno_score = vision_result.get("avg_score", 2.5)
            listing["avg_reno_score"] = avg_reno_score
            listing["red_flags"] = vision_result.get("red_flags", [])
        else:
            print(f"     ⚠ No room photos — using default reno score {avg_reno_score}")
            listing["avg_reno_score"] = avg_reno_score
    except Exception as e:
        print(f"     ✗ Vision scoring error: {e}")
        listing["avg_reno_score"] = avg_reno_score

    # ── Step 5: Skip renovated properties ──
    classification = listing.get("classification", "uncertain")
    if classification == "renovated":
        print(f"     ⚠ Property classified as renovated — skipping insights")
        return None

    # ── Step 6: Insights agent ──
    try:
        print(f"     → Running insights agent...")
        analysis = analyse_listing(listing, gap_data={suburb: gap_data})
        verdict = analysis.get("verdict", "PASS")
        margin = analysis.get("feasibility", {}).get("margin_at_asking_pct", 0)
        print(f"     → Verdict: {verdict}  |  Margin: {margin}%")
        return analysis
    except Exception as e:
        print(f"     ✗ Insights agent error: {e}")
        traceback.print_exc()
        return None


# ─────────────────────────────────────────
# LOG RUN TO SUPABASE
# ─────────────────────────────────────────
def log_run(stats: dict):
    """Store run summary in a pipeline_runs table (creates silently if missing)."""
    if DRY_RUN:
        return
    try:
        supabase.table("pipeline_runs").insert({
            "run_at":           datetime.utcnow().isoformat(),
            "listings_fetched": stats["fetched"],
            "listings_analysed": stats["analysed"],
            "go_count":         stats["go"],
            "watch_count":      stats["watch"],
            "pass_count":       stats["pass"],
            "errors":           stats["errors"],
        }).execute()
    except Exception:
        pass  # Table may not exist yet — non-fatal


# ─────────────────────────────────────────
# MAIN RUN
# ─────────────────────────────────────────
def run():
    start = datetime.utcnow()
    print(f"\n{'═'*60}")
    print(f"  DAILY PIPELINE RUN — {start.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═'*60}\n")

    stats = {"fetched": 0, "analysed": 0, "go": 0, "watch": 0, "pass": 0, "errors": 0}
    alerts_to_send = []

    # ── 0. Refresh sold data ──
    if not DRY_RUN:
        refresh_sold_data()

    # ── 1. Load suburb gap cache ──
    suburb_gaps = load_suburb_gaps()
    if not suburb_gaps:
        print("✗ No suburb gap data available — run suburb_gaps.py first")
        return

    # ── 2. Fetch new listings ──
    print("\n[1/3] Fetching new listings via Apify...\n")
    try:
        new_listings = fetch_new_listings()
        stats["fetched"] = len(new_listings)
        print(f"\n→ {len(new_listings)} new listings to process")
    except Exception as e:
        print(f"✗ Failed to fetch listings: {e}")
        return

    if not new_listings:
        print("\n✓ No new listings today — nothing to do")
        log_run(stats)
        return

    # ── 3. Process each listing ──
    print(f"\n[2/3] Running pipeline on {len(new_listings)} listings...\n")

    for i, listing in enumerate(new_listings, 1):
        print(f"[{i}/{len(new_listings)}]", end="")
        try:
            analysis = process_listing(listing, suburb_gaps)

            if analysis:
                stats["analysed"] += 1
                verdict = analysis.get("verdict", "PASS")

                if verdict == "GO":
                    stats["go"] += 1
                elif verdict == "WATCH":
                    stats["watch"] += 1
                else:
                    stats["pass"] += 1

                if verdict in ALERT_VERDICTS:
                    alerts_to_send.append((listing, analysis))
                    print_analysis(analysis)

        except Exception as e:
            stats["errors"] += 1
            print(f"\n     ✗ Unhandled error: {e}")
            traceback.print_exc()

        # Polite delay between listings
        time.sleep(1)

    # ── 4. Send alerts ──
    print(f"\n[3/3] Sending alerts ({len(alerts_to_send)} deals found)...\n")

    if alerts_to_send and not DRY_RUN:
        for listing, analysis in alerts_to_send:
            try:
                pf = analysis["_meta"]["preflight_feasibility"]
                feasibility_for_email = {
                    "verdict":        analysis.get("verdict"),
                    "arv":            analysis.get("arv_estimate", 0),
                    "arv_confidence": analysis.get("arv_confidence", "low"),
                    "arv_method":     "suburb_gap",
                    "reno_cost":      pf.get("reno_cost", 0),
                    "buying_costs":   pf.get("buying_costs", 0),
                    "holding_costs":  pf.get("holding_costs", 0),
                    "selling_costs":  pf.get("selling_costs", 0),
                    "profit_target":  pf.get("target_profit_15pct", 0),
                    "max_offer":      analysis["feasibility"].get("max_offer", 0),
                    "margin_at_list": analysis["feasibility"].get("margin_at_asking_pct", 0) / 100,
                    "scenarios": {
                        k: {"arv": v.get("arv", 0), "reno_cost": v.get("reno_cost", 0),
                            "margin": v.get("margin_pct", 0) / 100}
                        for k, v in analysis.get("scenarios", {}).items()
                    },
                }
                vision_for_email = {"red_flags": analysis.get("red_flags", [])}
                text_for_email = listing.get("text_renovation_signals", {})
                send_alert(listing, feasibility_for_email, vision_for_email, text_for_email)
                print(f"  ✓ Alert sent: {listing.get('address')}")
            except Exception as e:
                print(f"  ✗ Email alert failed for {listing.get('address')}: {e}")
                stats["errors"] += 1
    elif DRY_RUN:
        print(f"  [DRY RUN] Would send alert for {len(alerts_to_send)} listing(s)")
    else:
        print(f"  → No alert-worthy listings today")

    # ── 5. Summary ──
    duration = (datetime.utcnow() - start).seconds
    print(f"\n{'═'*60}")
    print(f"  RUN COMPLETE — {duration}s")
    print(f"{'═'*60}")
    print(f"  Fetched:   {stats['fetched']} new listings")
    print(f"  Analysed:  {stats['analysed']} (passed gap filter)")
    print(f"  ✅  GO:    {stats['go']}")
    print(f"  👀  WATCH: {stats['watch']}")
    print(f"  ❌  PASS:  {stats['pass']}")
    if stats["errors"]:
        print(f"  ⚠️  Errors: {stats['errors']}")
    print(f"{'═'*60}\n")

    log_run(stats)


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the daily property pipeline")
    parser.add_argument("--dry-run",      action="store_true", help="Skip emails and DB logging")
    parser.add_argument("--refresh-sold", action="store_true", help="Refresh sold data and gap analysis only")
    parser.add_argument("--gap-min",      type=float, default=MIN_GAP_PCT,
                        help=f"Min suburb gap %% to trigger analysis (default: {MIN_GAP_PCT})")
    args = parser.parse_args()

    if args.refresh_sold:
        refresh_sold_data()
    else:
        if args.dry_run:
            DRY_RUN = True
            print("⚠ DRY RUN MODE — no emails or DB writes")
        MIN_GAP_PCT = args.gap_min
        run()
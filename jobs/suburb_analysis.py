# jobs/suburb_analysis.py
"""
Suburb gap analysis microservice.

Runs independently of the daily listing pipeline. Configure as a separate cron job
(e.g. weekly on Mondays).

Flow:
  1. Pull latest sold listings via Apify (region-based backfill)
  2. Run gap analysis across all suburbs with enough sold data
  3. Send suburb gap summary email
  4. Log run to Supabase
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import traceback
from datetime import datetime, timezone

from jobs.backfill import run_backfill_regions
from analysis.suburb_gaps import run_gap_analysis, score_unclassified_sold_listings
from alerts.email import send_suburb_gap_email
from db.client import supabase

DRY_RUN   = False
SKIP_VISION = False


def log_run(stats: dict):
    if DRY_RUN:
        return
    try:
        supabase.table("pipeline_runs").insert({
            "run_at":            datetime.now(timezone.utc).isoformat(),
            "listings_fetched":  stats.get("sold_inserted", 0),
            "listings_analysed": stats.get("suburbs_analysed", 0),
            "go_count":          0,
            "watch_count":       0,
            "pass_count":        0,
            "errors":            stats.get("errors", 0),
        }).execute()
    except Exception:
        pass


def run():
    start = datetime.now(timezone.utc)
    print(f"\n{'═'*60}")
    print(f"  SUBURB ANALYSIS RUN — {start.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═'*60}\n")

    stats = {"sold_inserted": 0, "suburbs_analysed": 0, "errors": 0}

    # ── 1. Pull latest sold listings ──
    print("[1/3] Pulling sold listings via Apify...\n")
    try:
        run_backfill_regions()
    except Exception as e:
        print(f"✗ Backfill failed: {e}")
        traceback.print_exc()
        stats["errors"] += 1

    # ── 2. Vision-score unclassified sold listings ──
    print("\n[2/3] Vision-scoring unclassified sold listings...\n")
    try:
        score_unclassified_sold_listings(dry_run=(DRY_RUN or SKIP_VISION))
    except Exception as e:
        print(f"✗ Vision scoring failed: {e}")
        traceback.print_exc()
        stats["errors"] += 1

    # ── 3. Run gap analysis ──
    print("\n[3/4] Running suburb gap analysis...\n")
    results = {}
    try:
        results = run_gap_analysis(min_sales=5)
        stats["suburbs_analysed"] = len(results)
        print(f"\n✓ Gap data updated for {len(results)} suburbs")
    except Exception as e:
        print(f"✗ Gap analysis failed: {e}")
        traceback.print_exc()
        stats["errors"] += 1

    # ── 4. Send gap report email ──
    print("\n[4/4] Sending suburb gap report email...\n")
    if results and not DRY_RUN:
        try:
            send_suburb_gap_email(results)
            print("✓ Gap report email sent")
        except Exception as e:
            print(f"✗ Gap report email failed: {e}")
            traceback.print_exc()
            stats["errors"] += 1
    elif DRY_RUN:
        print(f"  [DRY RUN] Would send gap report for {len(results)} suburbs")
    else:
        print("  → No results to email")

    # ── Summary ──
    duration = (datetime.now(timezone.utc) - start).seconds
    print(f"\n{'═'*60}")
    print(f"  SUBURB ANALYSIS COMPLETE — {duration}s")
    print(f"{'═'*60}")
    print(f"  Suburbs analysed: {stats['suburbs_analysed']}")
    if stats["errors"]:
        print(f"  Errors: {stats['errors']}")
    print(f"{'═'*60}\n")

    log_run(stats)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run suburb gap analysis and email report")
    parser.add_argument("--dry-run",       action="store_true", help="Skip email and DB logging")
    parser.add_argument("--skip-backfill", action="store_true", help="Skip Apify pull, re-run on existing data")
    parser.add_argument("--skip-vision",   action="store_true", help="Skip Claude vision scoring of sold listing photos")
    parser.add_argument("--gap-only",      action="store_true", help="Run gap analysis only (no backfill, no vision, no email)")
    parser.add_argument("--state",         type=str, default=None, help="Limit vision scoring to a single state (e.g. --state VIC)")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        print("⚠ DRY RUN MODE — no emails or DB writes")
    if args.skip_vision or args.dry_run:
        SKIP_VISION = True
        if args.skip_vision:
            print("⚠ SKIP VISION — using cached/price-heuristic classification only")
    if args.state:
        print(f"⚠ STATE FILTER — vision scoring limited to {args.state}")

    if args.gap_only:
        run_gap_analysis(min_sales=5)
    elif args.skip_backfill:
        score_unclassified_sold_listings(dry_run=SKIP_VISION, state=args.state)
        results = run_gap_analysis(min_sales=5)
        if results and not DRY_RUN:
            send_suburb_gap_email(results)
    else:
        run()

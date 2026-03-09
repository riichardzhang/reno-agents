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
from datetime import datetime, timezone, timedelta
from typing import Optional

from sources.domain import fetch_new_listings
from classifiers.text import classify_listing_text
from classifiers.photos import process_listing_photos
from classifiers.vision import score_listing_renovation, classify_property_style
from agents.insights import analyse_listing, print_analysis
from alerts.email import send_digest_email
from db.client import supabase, get_photos_for_listing, update_listing

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
MIN_GAP_PCT = 20.0          # Only run insights agent on suburbs with 20%+ gap
ALERT_VERDICTS = {"GO", "WATCH"}
DRY_RUN = False
SKIP_FETCH = False          # Set True to skip Apify and only re-check existing DB listings
SKIP_INSIGHTS = False       # Set True to use cached verdicts instead of calling Claude Sonnet


# ─────────────────────────────────────────
# SUBURB GAP CACHE
# ─────────────────────────────────────────
def load_suburb_gaps() -> dict:
    try:
        result = supabase.table("suburb_gaps").select("*").eq("property_type", "house").execute()
        gaps = {}
        for row in result.data:
            key = (row["suburb"].strip().title(), row["state"])
            gaps[key] = row
        print(f"✓ Loaded gap data for {len(gaps)} suburbs")
        return gaps
    except Exception as e:
        print(f"✗ Failed to load suburb gaps: {e}")
        return {}


def get_gap_for_suburb(suburb: str, gaps: dict, state: str = "TAS") -> Optional[dict]:
    return gaps.get((suburb.strip().title(), state))


# ─────────────────────────────────────────
# CACHED ANALYSIS (no Claude call)
# ─────────────────────────────────────────
def build_cached_analysis(listing: dict, gap_data: dict) -> Optional[dict]:
    """
    Build a full analysis dict from cached DB values without calling Claude.
    Uses preflight_feasibility() math + cached verdict/margin from DB.
    Only returns a result if the listing has a cached GO or WATCH verdict.
    """
    from agents.insights import preflight_feasibility, estimate_reno_cost

    verdict = listing.get("verdict")
    if verdict not in ALERT_VERDICTS:
        return None

    suburb        = listing.get("suburb", "")
    asking_price  = float(listing.get("price", 0))
    avg_reno_score = float(listing.get("avg_reno_score") or 2.5)
    margin_pct    = float(listing.get("margin_percent") or 0)
    cached_max_offer = listing.get("max_offer_price") or 0

    gap = get_gap_for_suburb(suburb, gap_data, listing.get("state", "TAS"))
    arv = float(gap.get("renovated_median", 0)) if gap else asking_price * 1.25

    reno_cost, reno_tier = estimate_reno_cost(avg_reno_score)
    pf = preflight_feasibility(asking_price, arv, reno_cost)
    pf["reno_tier"] = reno_tier

    max_offer = cached_max_offer or pf["max_offer_price"]

    def make_scenario(arv_factor, reno_factor):
        s_pf = preflight_feasibility(asking_price, int(arv * arv_factor), int(reno_cost * reno_factor))
        return {
            "reno_cost":  int(reno_cost * reno_factor),
            "arv":        int(arv * arv_factor),
            "profit":     s_pf["actual_profit_at_asking"],
            "margin_pct": s_pf["actual_margin_pct"],
        }

    return {
        "verdict":        verdict,
        "arv_estimate":   int(arv),
        "arv_confidence": "medium",
        "red_flags":      [],
        "feasibility": {
            "max_bid_above_asking": int(max_offer - asking_price),
            "profit_at_asking":     pf["actual_profit_at_asking"],
            "margin_pct":           margin_pct,
            "verdict_at_asking":    "viable" if margin_pct >= 10 else "borderline",
        },
        "scenarios": {
            "best":  make_scenario(1.10, 0.80),
            "base":  make_scenario(1.00, 1.00),
            "worst": make_scenario(0.90, 1.20),
        },
        "_meta": {
            "listing_id":            listing.get("id"),
            "address":               listing.get("address"),
            "suburb":                suburb,
            "asking_price":          asking_price,
            "preflight_feasibility": {**pf, "max_offer_price": int(max_offer)},
            "model":                 "cached",
        },
    }


# ─────────────────────────────────────────
# PIPELINE: PROCESS A SINGLE LISTING
# ─────────────────────────────────────────
def process_listing(listing: dict, suburb_gaps: dict, skip_property_style: bool = False) -> Optional[dict]:
    listing_id = listing.get("id")
    address = listing.get("address", "Unknown")
    suburb = listing.get("suburb", "")
    state  = listing.get("state", "TAS")
    price = listing.get("price", 0)

    print(f"\n  ── {address} ──")
    print(f"     ${price:,}  |  {suburb}, {state}")

    # ── Step 1: Check suburb gap ──
    gap_data = get_gap_for_suburb(suburb, suburb_gaps, state)
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
    existing_classification = listing.get("classification")
    if existing_classification and existing_classification not in ("uncertain", None):
        # Already classified — skip the Claude call
        print(f"     → Text: using stored classification ({existing_classification})")
        listing["text_renovation_signals"] = {
            "classification": existing_classification,
            "signals": [],
            "confidence": 0.8,
        }
    else:
        try:
            text_result = classify_listing_text(listing_id, description)
            listing["text_renovation_signals"] = {
                "classification": text_result.get("classification"),
                "signals":        text_result.get("signals", []),
                "confidence":     text_result.get("confidence", 0),
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
    # Use DB value as fallback if scoring fails (handles repeat listings with cached scores)
    avg_reno_score = listing.get("renovation_score") or 2.5
    try:
        vision_result = score_listing_renovation(listing_id)
        if vision_result:
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

    # ── Step 5b: Property style classification (for ARV calibration) ──
    # Skipped for re-check listings — only kitchen/bathroom URLs in DB (interior shots
    # always return uncertain anyway), so we avoid a wasted Haiku vision call.
    property_style = {"style": "uncertain", "confidence": 0.0}
    if not skip_property_style:
        try:
            if photo_urls:
                print(f"     → Classifying property style...")
                property_style = classify_property_style(photo_urls)
                listing["property_style"] = property_style.get("style", "uncertain")
        except Exception as e:
            print(f"     ✗ Property style classification error: {e}")

    # ── Step 6: Insights agent ──
    try:
        if SKIP_INSIGHTS:
            print(f"     → Using cached verdict (--skip-insights)")
            analysis = build_cached_analysis(listing, suburb_gaps)
            if not analysis:
                print(f"     ⚠ No cached GO/WATCH verdict — skipping")
                return None
        else:
            print(f"     → Running insights agent...")
            analysis = analyse_listing(listing, gap_data={suburb: gap_data}, property_style=property_style)
        verdict = analysis.get("verdict", "PASS")
        margin = analysis.get("feasibility", {}).get("margin_pct", 0)
        max_offer = analysis.get("_meta", {}).get("preflight_feasibility", {}).get("max_offer_price", 0)
        print(f"     → Verdict: {verdict}  |  Margin: {margin}%")

        # Persist verdict and margin to DB so re-check runs can use them
        try:
            update_listing(listing_id, {
                "verdict":        verdict,
                "margin_percent": margin,
                "max_offer_price": max_offer,
                "evaluated_at":   datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            pass  # Non-critical — don't fail the pipeline if DB write fails

        return analysis
    except Exception as e:
        print(f"     ✗ Insights agent error: {e}")
        traceback.print_exc()
        return None


# ─────────────────────────────────────────
# LOG RUN TO SUPABASE
# ─────────────────────────────────────────
def log_run(stats: dict):
    if DRY_RUN:
        return
    try:
        supabase.table("pipeline_runs").insert({
            "run_at":            datetime.now(timezone.utc).isoformat(),
            "listings_fetched":  stats["fetched"],
            "listings_analysed": stats["analysed"],
            "go_count":          stats["go"],
            "watch_count":       stats["watch"],
            "pass_count":        stats["pass"],
            "errors":            stats["errors"],
        }).execute()
    except Exception:
        pass


# ─────────────────────────────────────────
# MAIN RUN
# ─────────────────────────────────────────
def run():
    start = datetime.now(timezone.utc)
    print(f"\n{'═'*60}")
    print(f"  DAILY PIPELINE RUN — {start.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═'*60}\n")

    stats = {"fetched": 0, "analysed": 0, "go": 0, "watch": 0, "pass": 0, "errors": 0}
    alerts_to_send = []
    is_monday = datetime.now(timezone.utc).weekday() == 0

    # ── 1. Load suburb gap cache ──
    suburb_gaps = load_suburb_gaps()
    if not suburb_gaps:
        print("✗ No suburb gap data available — run suburb_gaps.py first")
        return

    # ── 2. Fetch new listings ──
    if SKIP_FETCH:
        print("\n[1/3] Skipping Apify fetch (--skip-fetch)\n")
        new_listings = []
    else:
        print("\n[1/3] Fetching new listings via Apify...\n")
        try:
            new_listings = fetch_new_listings()
            stats["fetched"] = len(new_listings)
            print(f"\n→ {len(new_listings)} new listings to process")
        except Exception as e:
            print(f"✗ Failed to fetch listings: {e}")
            return

    if not new_listings:
        print("\n✓ No new listings today — skipping to re-check step")

    # ── 3. Process each listing ──
    if new_listings:
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

                margin = analysis.get("feasibility", {}).get("margin_pct", 0)
                worst_margin = analysis.get("scenarios", {}).get("worst", {}).get("margin_pct", 0)
                if verdict in ALERT_VERDICTS and margin >= 0 and worst_margin >= 0:
                    alerts_to_send.append((listing, analysis))
                    print_analysis(analysis)

        except Exception as e:
            stats["errors"] += 1
            print(f"\n     ✗ Unhandled error: {e}")
            traceback.print_exc()

        time.sleep(1)

    # ── 3b. Re-analyse existing active listings ──
    # Listings already in DB are skipped by fetch_new_listings(), but good deals
    # still on the market need to appear in the digest.
    # Strategy:
    #   - Unalerted listings: always re-run (may have been missed on first pass)
    #   - Alerted listings: re-run weekly (Mondays) to refresh numbers; skip other days
    print(f"\n[2b/3] Re-checking existing active listings...\n")
    new_listing_ids = {l.get("id") for l in new_listings}

    try:
        existing_result = supabase.table("listings") \
            .select("*") \
            .eq("status", "active") \
            .execute()
        existing_listings = [
            l for l in existing_result.data
            if l["id"] not in new_listing_ids
            and l.get("renovation_score") is not None
            and l.get("classification") != "renovated"
        ]
        print(f"  → {len(existing_listings)} existing active listings to re-check")
    except Exception as e:
        print(f"  ✗ Failed to fetch existing listings: {e}")
        existing_listings = []

    for i, listing in enumerate(existing_listings, 1):
        already_alerted = listing.get("alerted", False)

        # Alerted listings: only re-run insights on Mondays to avoid daily Sonnet spend
        if already_alerted and not is_monday:
            print(f"[E{i}/{len(existing_listings)}]  ── {listing.get('address')} — skipping (alerted, not Monday)")
            continue

        print(f"[E{i}/{len(existing_listings)}]", end="")
        # Property style skipped — photos table only has interior shots, always returns uncertain
        listing["_photo_urls"] = []

        try:
            analysis = process_listing(listing, suburb_gaps, skip_property_style=True)
            if analysis:
                stats["analysed"] += 1
                verdict = analysis.get("verdict", "PASS")
                if verdict == "GO":
                    stats["go"] += 1
                elif verdict == "WATCH":
                    stats["watch"] += 1
                else:
                    stats["pass"] += 1

                margin = analysis.get("feasibility", {}).get("margin_pct", 0)
                worst_margin = analysis.get("scenarios", {}).get("worst", {}).get("margin_pct", 0)
                if verdict in ALERT_VERDICTS and margin >= 0 and worst_margin >= 0:
                    alerts_to_send.append((listing, analysis))
                    print_analysis(analysis)

        except Exception as e:
            stats["errors"] += 1
            print(f"\n     ✗ Unhandled error: {e}")
            traceback.print_exc()

        time.sleep(1)

    # ── 4. Send alerts ──
    print(f"\n[3/3] Sending alerts ({len(alerts_to_send)} deals found)...\n")

    if alerts_to_send and not DRY_RUN:
        digest_alerts = []
        for listing, analysis in alerts_to_send:
            try:
                pf = analysis["_meta"]["preflight_feasibility"]
                feasibility_for_email = {
                    "verdict":              analysis.get("verdict"),
                    "arv":                  analysis.get("arv_estimate", 0),
                    "arv_confidence":       analysis.get("arv_confidence", "low"),
                    "arv_method":           "suburb_gap",
                    "reno_cost":            pf.get("reno_cost", 0),
                    "buying_costs":         pf.get("buying_costs", 0),
                    "holding_costs":        pf.get("holding_costs", 0),
                    "selling_costs":        pf.get("selling_costs", 0),
                    "capital_injected":     pf.get("capital_injected", 0),
                    "profit_target":        pf.get("target_profit_10pct", 0),
                    "max_offer_price":      pf.get("max_offer_price", 0),
                    "max_bid_above_asking": analysis["feasibility"].get("max_bid_above_asking", 0),
                    "margin_at_list":       analysis["feasibility"].get("margin_pct", 0) / 100,
                    "scenarios": {
                        k: {"arv": v.get("arv", 0), "reno_cost": v.get("reno_cost", 0),
                            "profit": v.get("profit", 0),
                            "margin": v.get("margin_pct", 0) / 100}
                        for k, v in analysis.get("scenarios", {}).items()
                    },
                }
                digest_alerts.append({
                    "listing":    listing,
                    "feasibility": feasibility_for_email,
                    "vision":     {"red_flags": analysis.get("red_flags", [])},
                    "text":       listing.get("text_renovation_signals", {}),
                })
            except Exception as e:
                print(f"  ✗ Failed to prepare alert for {listing.get('address')}: {e}")
                stats["errors"] += 1

        if digest_alerts:
            try:
                send_digest_email(digest_alerts)
                print(f"  ✓ Digest email sent with {len(digest_alerts)} deal(s)")
            except Exception as e:
                print(f"  ✗ Digest email failed: {e}")
                stats["errors"] += 1
    elif DRY_RUN:
        print(f"  [DRY RUN] Would send alert for {len(alerts_to_send)} listing(s)")
    else:
        print(f"  → No alert-worthy listings today")

    # ── 5. Summary ──
    duration = (datetime.now(timezone.utc) - start).seconds
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
    parser.add_argument("--dry-run",       action="store_true", help="Skip emails and DB logging")
    parser.add_argument("--skip-fetch",    action="store_true", help="Skip Apify fetch, only re-check existing DB listings")
    parser.add_argument("--skip-insights", action="store_true", help="Use cached verdicts instead of calling Claude Sonnet")
    parser.add_argument("--gap-min",       type=float, default=MIN_GAP_PCT,
                        help=f"Min suburb gap %% to trigger analysis (default: {MIN_GAP_PCT})")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        print("⚠ DRY RUN MODE — no emails or DB writes")
    if args.skip_fetch:
        SKIP_FETCH = True
        print("⚠ SKIP FETCH — re-checking existing DB listings only")
    if args.skip_insights:
        SKIP_INSIGHTS = True
        print("⚠ SKIP INSIGHTS — using cached verdicts, no Claude Sonnet calls")
    MIN_GAP_PCT = args.gap_min
    run()
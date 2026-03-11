# analysis/suburb_gaps.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import statistics
from db.client import supabase

# ─────────────────────────────────────────
# FETCH SOLD LISTINGS FOR SUBURB
# ─────────────────────────────────────────
def get_sold_listings(suburb: str, state: str = "TAS", property_type: str = "house") -> list:
    """Fetch sold listings for a suburb filtered by property_type."""
    query = supabase.table("listings") \
        .select("*") \
        .eq("suburb", suburb) \
        .eq("state", state) \
        .eq("status", "sold") \
        .gt("price", 0) \
        .eq("property_type", property_type)
    return query.execute().data or []


# ─────────────────────────────────────────
# CLASSIFY SOLD LISTINGS
# ─────────────────────────────────────────
def classify_sold_listings(listings: list) -> dict:
    """
    Classify sold listings as renovated/unrenovated.
    Uses Claude vision classification from DB where available,
    falls back to price per m² heuristic for unclassified listings.
    """
    VISION_CLASSES = {"renovated", "unrenovated", "partial"}

    renovated_prices   = []
    unrenovated_prices = []
    partial_prices     = []
    unclassified       = []

    vision_count = 0
    for l in listings:
        cls   = l.get("classification")
        price = l.get("price", 0)
        if not price:
            continue
        if cls in VISION_CLASSES:
            vision_count += 1
            if cls == "renovated":
                renovated_prices.append(price)
            elif cls == "unrenovated":
                unrenovated_prices.append(price)
            else:
                partial_prices.append(price)
        else:
            unclassified.append(l)

    if vision_count:
        print(f"     → {vision_count} vision-classified, {len(unclassified)} using price heuristic")

    # Price/m² fallback for unclassified listings
    if unclassified:
        fallback = _classify_by_ppm2(unclassified)
        renovated_prices.extend(fallback["renovated"])
        unrenovated_prices.extend(fallback["unrenovated"])
        partial_prices.extend(fallback["partial"])

    return {
        "renovated":   renovated_prices,
        "unrenovated": unrenovated_prices,
        "partial":     partial_prices,
        "uncertain":   [],
    }


def _classify_by_ppm2(listings: list) -> dict:
    """Classify by price per m² ratio (internal fallback)."""
    valid = [
        l for l in listings
        if l.get("price", 0) > 0 and l.get("land_size", 0) and l["land_size"] > 50
    ]

    if len(valid) < len(listings) * 0.3:
        return classify_by_price_split(listings)

    for l in valid:
        l["_ppm2"] = l["price"] / l["land_size"]

    median_ppm2 = statistics.median([l["_ppm2"] for l in valid])

    renovated_prices   = []
    unrenovated_prices = []
    partial_prices     = []

    for l in valid:
        ppm2  = l["_ppm2"]
        price = l["price"]
        if ppm2 >= median_ppm2 * 1.15:
            renovated_prices.append(price)
        elif ppm2 <= median_ppm2 * 0.85:
            unrenovated_prices.append(price)
        else:
            partial_prices.append(price)

    return {"renovated": renovated_prices, "unrenovated": unrenovated_prices, "partial": partial_prices}


# ─────────────────────────────────────────
# VISION SCORING FOR SOLD LISTINGS
# ─────────────────────────────────────────
def score_unclassified_sold_listings(dry_run: bool = False):
    """
    Download and score photos for sold listings that don't yet have
    a vision-based classification. Updates classification in DB.

    Only processes listings that have photo URLs stored in the photos
    table (from the backfill job). Skips already-classified listings.
    """
    import requests as req
    import base64
    from classifiers.photos import identify_room_from_url, identify_room_via_claude
    from classifiers.vision import score_room, classify_from_scores

    TARGET_ROOMS = {"kitchen", "bathroom"}

    print("  Fetching unclassified sold listings with photos...")

    # Find sold listings with pending photos (room_type IS NULL) and no classification
    try:
        pending = supabase.table("photos") \
            .select("listing_id") \
            .is_("room_type", "null") \
            .execute()

        listing_ids_with_photos = list({r["listing_id"] for r in (pending.data or [])})
        if not listing_ids_with_photos:
            print("  → No pending photos to process")
            return
    except Exception as e:
        print(f"  ✗ Failed to fetch pending photos: {e}")
        return

    # Filter to only unclassified sold listings
    try:
        result = supabase.table("listings") \
            .select("id, address, suburb") \
            .in_("id", listing_ids_with_photos) \
            .eq("status", "sold") \
            .is_("classification", "null") \
            .execute()
        to_score = result.data or []
    except Exception as e:
        print(f"  ✗ Failed to fetch unclassified listings: {e}")
        return

    print(f"  → {len(to_score)} sold listings to vision-score")
    if dry_run:
        print("  [DRY RUN] Skipping vision scoring")
        return

    scored = 0
    for listing in to_score:
        listing_id = listing["id"]
        address    = listing.get("address", listing_id)

        # Fetch pending photo URLs for this listing
        try:
            photos = supabase.table("photos") \
                .select("id, url") \
                .eq("listing_id", listing_id) \
                .is_("room_type", "null") \
                .execute()
            photo_records = photos.data or []
        except Exception:
            continue

        if not photo_records:
            continue

        print(f"  ── {address}")

        # Identify and download kitchen + bathroom photos
        room_photos = {}          # room_type -> (photo_record_id, base64)
        needs_claude  = []        # (record_id, url, b64) for unknown rooms

        for i, rec in enumerate(photo_records):
            if all(r in room_photos for r in TARGET_ROOMS):
                break

            url       = rec["url"]
            record_id = rec["id"]
            room_type = identify_room_from_url(url, i)

            if room_type in TARGET_ROOMS and room_type not in room_photos:
                try:
                    resp = req.get(url, headers={
                        "Referer": "https://www.domain.com.au",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=10)
                    if resp.status_code == 200:
                        b64 = base64.b64encode(resp.content).decode("utf-8")
                        room_photos[room_type] = (record_id, b64)
                except Exception:
                    pass
            elif room_type is None:
                try:
                    resp = req.get(url, headers={
                        "Referer": "https://www.domain.com.au",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=10)
                    if resp.status_code == 200:
                        b64 = base64.b64encode(resp.content).decode("utf-8")
                        needs_claude.append((record_id, url, b64))
                except Exception:
                    pass

        # Use Claude to identify unknown photos if still missing rooms
        missing = TARGET_ROOMS - set(room_photos.keys())
        if missing and needs_claude:
            for record_id, url, b64 in needs_claude:
                if not missing:
                    break
                identified = identify_room_via_claude(b64)
                if identified in missing:
                    room_photos[identified] = (record_id, b64)
                    missing.discard(identified)

        if not room_photos:
            print(f"     ⚠ No target room photos found — skipping")
            continue

        # Score each room
        room_scores = {}
        for room_type, (record_id, b64) in room_photos.items():
            result = score_room(b64, room_type)
            score  = result["score"]
            room_scores[room_type] = score
            print(f"     → {room_type.capitalize()}: {score}/10")

            # Cache score and room_type on the photo record
            try:
                supabase.table("photos").update({
                    "room_type":        room_type,
                    "photo_base64":     b64,
                    "renovation_score": score,
                }).eq("id", record_id).execute()
            except Exception:
                pass

        # Classify and update the listing
        classification = classify_from_scores(room_scores)
        avg_score      = round(sum(room_scores.values()) / len(room_scores), 1)
        print(f"     → {avg_score:.1f}/10 — {classification.upper()}")

        try:
            supabase.table("listings").update({
                "classification":   classification,
                "renovation_score": avg_score,
            }).eq("id", listing_id).execute()
            scored += 1
        except Exception as e:
            print(f"     ✗ DB update failed: {e}")

    print(f"  ✓ Vision-scored {scored} sold listings")


def classify_by_price_split(listings: list) -> dict:
    """
    Fallback: split listings into top/bottom thirds by price.
    Top third = renovated, bottom third = unrenovated.
    """
    prices = sorted([l["price"] for l in listings if l.get("price", 0) > 0])
    if len(prices) < 6:
        return {"renovated": [], "unrenovated": [], "partial": prices, "uncertain": []}

    third = len(prices) // 3
    return {
        "renovated":    prices[third*2:],
        "unrenovated":  prices[:third],
        "partial":      prices[third:third*2],
        "uncertain":    []
    }

# ─────────────────────────────────────────
# SUBURB MARKET VELOCITY (DOM ANALYSIS)
# ─────────────────────────────────────────
def get_suburb_dom_stats(suburb: str, state: str = "TAS") -> dict:
    """
    Calculate average days on market for active listings in a suburb.
    Uses first_seen_at as a DOM proxy.
    Returns velocity rating and stats.
    """
    from datetime import datetime, timezone

    try:
        result = supabase.table("listings") \
            .select("id, first_seen_at, price") \
            .eq("suburb", suburb) \
            .eq("state", state) \
            .eq("status", "active") \
            .not_.is_("first_seen_at", "null") \
            .execute()

        listings = result.data or []

        if len(listings) < 3:
            return {
                "suburb":       suburb,
                "avg_dom":      None,
                "median_dom":   None,
                "sample_size":  len(listings),
                "velocity":     "unknown",
                "signal":       "Insufficient data — check back in 2-3 weeks"
            }

        now = datetime.now(timezone.utc)
        dom_values = []
        for l in listings:
            first_seen = l.get("first_seen_at")
            if not first_seen:
                continue
            # Parse the timestamp
            if isinstance(first_seen, str):
                first_seen = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            dom = (now - first_seen).days
            if dom >= 0:
                dom_values.append(dom)

        if not dom_values:
            return {
                "suburb":       suburb,
                "avg_dom":      None,
                "median_dom":   None,
                "sample_size":  0,
                "velocity":     "unknown",
                "signal":       "No DOM data available yet"
            }

        avg_dom    = round(sum(dom_values) / len(dom_values), 1)
        median_dom = int(statistics.median(dom_values))

        # Velocity rating
        if avg_dom <= 14:
            velocity = "very fast"
            signal   = f"Suburb moving fast — avg {avg_dom} days. Easy exit."
        elif avg_dom <= 30:
            velocity = "fast"
            signal   = f"Good market velocity — avg {avg_dom} days on market."
        elif avg_dom <= 60:
            velocity = "moderate"
            signal   = f"Moderate velocity — avg {avg_dom} days. Allow extra holding time."
        elif avg_dom <= 90:
            velocity = "slow"
            signal   = f"Slow market — avg {avg_dom} days. Factor 6+ months holding costs."
        else:
            velocity = "very slow"
            signal   = f"Very slow market — avg {avg_dom} days. High exit risk."

        return {
            "suburb":       suburb,
            "avg_dom":      avg_dom,
            "median_dom":   median_dom,
            "sample_size":  len(dom_values),
            "velocity":     velocity,
            "signal":       signal
        }

    except Exception as e:
        return {
            "suburb":       suburb,
            "avg_dom":      None,
            "median_dom":   None,
            "sample_size":  0,
            "velocity":     "unknown",
            "signal":       f"Error: {e}"
        }


def get_all_suburb_dom_stats(min_samples: int = 3) -> dict:
    """
    Run DOM analysis across all suburbs with enough active listing data.
    Useful for a weekly suburb velocity report.
    """
    result = supabase.table("listings") \
        .select("suburb, state") \
        .eq("status", "active") \
        .not_.is_("first_seen_at", "null") \
        .execute()

    if not result.data:
        print("No active listings with first_seen_at data yet")
        return {}

    # Count per suburb
    suburb_counts = {}
    for row in result.data:
        key = (row["suburb"], row["state"])
        suburb_counts[key] = suburb_counts.get(key, 0) + 1

    eligible = {k: v for k, v in suburb_counts.items() if v >= min_samples}
    print(f"Suburbs with DOM data ({min_samples}+ listings): {len(eligible)}\n")

    results = {}
    for (suburb, state), count in sorted(eligible.items(), key=lambda x: -x[1]):
        stats = get_suburb_dom_stats(suburb, state)
        results[suburb] = stats

    # Print summary table
    if results:
        print(f"{'Suburb':<20} {'Avg DOM':>8} {'Median':>8} {'Listings':>9} {'Velocity':<12} Signal")
        print(f"{'─'*20} {'─'*8} {'─'*8} {'─'*9} {'─'*12} {'─'*30}")
        for suburb, s in sorted(results.items(), key=lambda x: (x[1]["avg_dom"] or 999)):
            avg = f"{s['avg_dom']:.1f}" if s["avg_dom"] is not None else "—"
            med = str(s["median_dom"]) if s["median_dom"] is not None else "—"
            print(f"{suburb:<20} {avg:>8} {med:>8} {s['sample_size']:>9} {s['velocity']:<12} {s['signal']}")

    return results

# ─────────────────────────────────────────
# CALCULATE MEDIAN
# ─────────────────────────────────────────
def safe_median(prices: list):
    """Calculate median, return None if insufficient data."""
    if len(prices) < 3:
        return None
    return int(statistics.median(prices))


# ─────────────────────────────────────────
# CALCULATE SUBURB GAP
# ─────────────────────────────────────────
def calculate_suburb_gap(suburb: str, state: str = "TAS", property_type: str = "house") -> dict:
    """
    Calculate the renovated vs unrenovated price gap for a suburb.
    Stores result in suburb_gaps table.
    Returns gap data dict.
    """
    listings = get_sold_listings(suburb, state, property_type)

    if not listings:
        print(f"  ✗ No sold listings for {suburb}")
        return None

    print(f"  → {suburb}: {len(listings)} sold listings")

    # Classify listings
    classified = classify_sold_listings(listings)

    renovated_prices    = classified["renovated"]
    unrenovated_prices  = classified["unrenovated"]

    print(f"     Renovated: {len(renovated_prices)} | "
          f"Unrenovated: {len(unrenovated_prices)} | "
          f"Partial: {len(classified['partial'])} | "
          f"Uncertain: {len(classified['uncertain'])}")

    # Calculate medians
    renovated_median    = safe_median(renovated_prices)
    unrenovated_median  = safe_median(unrenovated_prices)

    if not renovated_median or not unrenovated_median:
        print(f"  ⚠ Insufficient classified data for {suburb} — skipping (need 3+ renovated AND 3+ unrenovated)")
        return None

    gap_dollar  = renovated_median - unrenovated_median
    gap_percent = round((gap_dollar / unrenovated_median) * 100, 1)

    print(f"     Unrenovated median: ${unrenovated_median:,}")
    print(f"     Renovated median:   ${renovated_median:,}")
    print(f"     Gap: ${gap_dollar:,} ({gap_percent}%)")

    result = {
        "unrenovated_median":   unrenovated_median,
        "renovated_median":     renovated_median,
        "gap_dollar":           gap_dollar,
        "gap_percent":          gap_percent,
        "sample_size":          len(listings),
    }

    upsert_suburb_gap(suburb, state, property_type, result)
    return result


# ─────────────────────────────────────────
# UPSERT SUBURB GAP
# ─────────────────────────────────────────
def upsert_suburb_gap(suburb: str, state: str, property_type: str, data: dict):
    """Insert or update suburb gap data. Drops suburbs with negative gap."""
    try:
        if data["gap_percent"] < 20:
            supabase.table("suburb_gaps") \
                .delete() \
                .eq("suburb", suburb) \
                .eq("state", state) \
                .eq("property_type", property_type) \
                .execute()
            print(f"     ✗ Gap {data['gap_percent']}% < 20% threshold — dropped from suburb_gaps")
            return

        supabase.table("suburb_gaps").upsert({
            "suburb":               suburb,
            "state":                state,
            "property_type":        property_type,
            "unrenovated_median":   data["unrenovated_median"],
            "renovated_median":     data["renovated_median"],
            "gap_dollar":           data["gap_dollar"],
            "gap_percent":          data["gap_percent"],
            "sample_size":          data["sample_size"],
            "last_updated":         "now()"
        }, on_conflict="suburb,state,property_type").execute()
        print(f"     ✓ Saved to suburb_gaps")
    except Exception as e:
        print(f"     ✗ Save error: {e}")


# ─────────────────────────────────────────
# RUN GAP ANALYSIS FOR ALL SUBURBS
# ─────────────────────────────────────────
# Sydney metro postcode ranges — used to restrict unit gap analysis to relevant market
SYDNEY_METRO_POSTCODE_RANGES = [
    (2000, 2234),   # Inner / eastern / southern / northern Sydney
    (2555, 2574),   # South-west (Campbelltown, Camden)
    (2745, 2778),   # West (Penrith, Hills)
]

def _is_sydney_metro(postcode) -> bool:
    if not postcode:
        return False
    return any(lo <= int(postcode) <= hi for lo, hi in SYDNEY_METRO_POSTCODE_RANGES)


def _count_sold_by_suburb(property_type: str) -> dict:
    """
    Page through listings to count sold records per (suburb, state).
    For NSW units, only counts suburbs in Sydney metro postcode ranges.
    """
    counts = {}
    page_size = 1000
    offset = 0
    while True:
        result = supabase.table("listings") \
            .select("suburb, state, postcode") \
            .eq("status", "sold") \
            .gt("price", 0) \
            .eq("property_type", property_type) \
            .range(offset, offset + page_size - 1) \
            .execute()
        rows = result.data or []
        for row in rows:
            # For NSW units, restrict to Sydney metro postcodes only
            if property_type == "unit" and row["state"] == "NSW":
                if not _is_sydney_metro(row.get("postcode")):
                    continue
            key = (row["suburb"], row["state"])
            counts[key] = counts.get(key, 0) + 1
        if len(rows) < page_size:
            break
        offset += page_size
    return counts


def run_gap_analysis(min_sales: int = 5) -> dict:
    """
    Run gap analysis for all suburbs with enough sold data.
    Runs separately for houses and units.
    Returns dict keyed by (suburb, state, property_type).
    """
    all_results = {}

    for prop_type in ("house", "unit"):
        print(f"\n── {prop_type.upper()} GAP ANALYSIS ──")
        suburb_counts = _count_sold_by_suburb(prop_type)

        if not suburb_counts:
            print(f"No sold {prop_type} listings found")
            continue

        eligible = {k: v for k, v in suburb_counts.items() if v >= min_sales}
        print(f"Found {len(suburb_counts)} suburbs with {prop_type} data")
        print(f"Eligible suburbs ({min_sales}+ sales): {len(eligible)}")
        print()

        for (suburb, state), count in sorted(eligible.items(), key=lambda x: -x[1]):
            print(f"[{suburb}, {state}] ({count} {prop_type} sales)")
            gap = calculate_suburb_gap(suburb, state, prop_type)
            if gap:
                all_results[(suburb, state, prop_type)] = {**gap, "state": state, "property_type": prop_type}
            print()

    print(f"{'='*55}")
    print(f"GAP ANALYSIS COMPLETE")
    print(f"{'='*55}")
    for prop_type in ("house", "unit"):
        subset = {k: v for k, v in all_results.items() if k[2] == prop_type}
        print(f"{prop_type.capitalize()}s with gap data: {len(subset)}")
    print()

    return all_results


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--suburb", type=str, help="Run for a single suburb")
    parser.add_argument("--all",    action="store_true", help="Run for all eligible suburbs")
    parser.add_argument("--min",    type=int, default=5, help="Minimum sales required (default: 5)")
    args = parser.parse_args()

    if args.suburb:
        print(f"Running gap analysis for {args.suburb}...")
        result = calculate_suburb_gap(args.suburb)
        if result:
            print(f"\nResult: ${result['gap_dollar']:,} gap ({result['gap_percent']}%)")
    elif args.all:
        run_gap_analysis(min_sales=args.min)
    else:
        print("Usage:")
        print("  python3 analysis/suburb_gaps.py --suburb Glenorchy")
        print("  python3 analysis/suburb_gaps.py --all")
        print("  python3 analysis/suburb_gaps.py --all --min 10")
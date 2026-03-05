# analysis/suburb_gaps.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import statistics
from db.client import supabase

# ─────────────────────────────────────────
# FETCH SOLD LISTINGS FOR SUBURB
# ─────────────────────────────────────────
def get_sold_listings(suburb: str, state: str = "TAS") -> list:
    """Fetch all sold listings for a suburb."""
    result = supabase.table("listings") \
        .select("*") \
        .eq("suburb", suburb) \
        .eq("state", state) \
        .eq("status", "sold") \
        .gt("price", 0) \
        .execute()
    return result.data or []


# ─────────────────────────────────────────
# CLASSIFY SOLD LISTINGS
# ─────────────────────────────────────────
def classify_sold_listings(listings: list) -> dict:
    """
    Classify sold listings as renovated/unrenovated using price per m².
    Listings >15% above median price/m² = renovated
    Listings >15% below median price/m² = unrenovated
    """
    # Filter to listings with valid land size and price
    valid = [
        l for l in listings
        if l.get("price", 0) > 0 and l.get("land_size", 0) and l["land_size"] > 50
    ]

    # Fall back to price-only split if not enough land size data
    if len(valid) < len(listings) * 0.3:
        print(f"     ⚠ Insufficient land size data — using price split")
        return classify_by_price_split(listings)

    # Calculate price per m²
    for l in valid:
        l["_ppm2"] = l["price"] / l["land_size"]

    median_ppm2 = statistics.median([l["_ppm2"] for l in valid])

    renovated_prices    = []
    unrenovated_prices  = []
    partial_prices      = []

    for l in valid:
        ppm2 = l["_ppm2"]
        price = l["price"]

        if ppm2 >= median_ppm2 * 1.15:
            renovated_prices.append(price)
        elif ppm2 <= median_ppm2 * 0.85:
            unrenovated_prices.append(price)
        else:
            partial_prices.append(price)

    return {
        "renovated":    renovated_prices,
        "unrenovated":  unrenovated_prices,
        "partial":      partial_prices,
        "uncertain":    []
    }


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
def calculate_suburb_gap(suburb: str, state: str = "TAS") -> dict:
    """
    Calculate the renovated vs unrenovated price gap for a suburb.
    Stores result in suburb_gaps table.
    Returns gap data dict.
    """
    listings = get_sold_listings(suburb, state)

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
        print(f"  ⚠ Insufficient classified data for {suburb} — "
              f"need 3+ renovated AND 3+ unrenovated sales")

        # Fall back to overall median if we can't split
        all_prices = [l["price"] for l in listings if l.get("price")]
        overall_median = safe_median(all_prices)

        if overall_median:
            # Store with low confidence
            upsert_suburb_gap(suburb, state, {
                "unrenovated_median":   int(overall_median * 0.85),
                "renovated_median":     int(overall_median * 1.15),
                "gap_dollar":           int(overall_median * 0.30),
                "gap_percent":          30.0,
                "sample_size":          len(listings),
                "confidence":           "very low (fallback)"
            })
            return None
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

    upsert_suburb_gap(suburb, state, result)
    return result


# ─────────────────────────────────────────
# UPSERT SUBURB GAP
# ─────────────────────────────────────────
def upsert_suburb_gap(suburb: str, state: str, data: dict):
    """Insert or update suburb gap data."""
    try:
        supabase.table("suburb_gaps").upsert({
            "suburb":               suburb,
            "state":                state,
            "unrenovated_median":   data["unrenovated_median"],
            "renovated_median":     data["renovated_median"],
            "gap_dollar":           data["gap_dollar"],
            "gap_percent":          data["gap_percent"],
            "sample_size":          data["sample_size"],
            "last_updated":         "now()"
        }, on_conflict="suburb,state").execute()
        print(f"     ✓ Saved to suburb_gaps")
    except Exception as e:
        print(f"     ✗ Save error: {e}")


# ─────────────────────────────────────────
# RUN GAP ANALYSIS FOR ALL SUBURBS
# ─────────────────────────────────────────
def run_gap_analysis(min_sales: int = 5) -> dict:
    """
    Run gap analysis for all suburbs with enough sold data.
    Only processes suburbs with min_sales or more sold listings.
    """
    # Get all suburbs with sold listings
    result = supabase.table("listings") \
        .select("suburb, state") \
        .eq("status", "sold") \
        .gt("price", 0) \
        .execute()

    if not result.data:
        print("No sold listings found")
        return {}

    # Count per suburb
    suburb_counts = {}
    for row in result.data:
        key = (row["suburb"], row["state"])
        suburb_counts[key] = suburb_counts.get(key, 0) + 1

    # Filter to suburbs with enough data
    eligible = {k: v for k, v in suburb_counts.items() if v >= min_sales}

    print(f"Found {len(suburb_counts)} suburbs with sold data")
    print(f"Eligible suburbs ({min_sales}+ sales): {len(eligible)}")
    print()

    results = {}
    for (suburb, state), count in sorted(eligible.items(), key=lambda x: -x[1]):
        print(f"[{suburb}, {state}] ({count} sales)")
        gap = calculate_suburb_gap(suburb, state)
        if gap:
            results[suburb] = gap
        print()

    print(f"{'='*55}")
    print(f"GAP ANALYSIS COMPLETE")
    print(f"{'='*55}")
    print(f"Suburbs with gap data: {len(results)}")
    print()

    # Print summary table
    if results:
        print(f"{'Suburb':<20} {'Unreno':>10} {'Reno':>10} {'Gap $':>10} {'Gap %':>8}")
        print(f"{'─'*20} {'─'*10} {'─'*10} {'─'*10} {'─'*8}")
        for suburb, data in sorted(results.items(), key=lambda x: -x[1]["gap_dollar"]):
            print(f"{suburb:<20} "
                  f"${data['unrenovated_median']:>9,} "
                  f"${data['renovated_median']:>9,} "
                  f"${data['gap_dollar']:>9,} "
                  f"{data['gap_percent']:>7.1f}%")

    return results


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
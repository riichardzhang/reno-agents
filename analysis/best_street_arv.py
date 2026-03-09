"""
analysis/best_street_arv.py

Estimates the renovated ARV of a 3 bed / 1 bath property on Best Street, Devonport.

Method:
  1. Pull all sold 3/1 houses in Devonport with land size data
  2. Classify as renovated using price/m² (>=15% above suburb median = renovated)
  3. Check if any Best Street sales are in the renovated bucket
  4. Report renovated median across Devonport + Best Street reference points
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import statistics
from db.client import supabase


def run():
    print("\n" + "═" * 60)
    print("  ARV ESTIMATE — 3/1 Renovated, Best Street, Devonport")
    print("═" * 60)

    # ── Fetch all sold 3/1 in Devonport ──
    result = supabase.table("listings") \
        .select("address, price, bedrooms, bathrooms, land_size") \
        .ilike("suburb", "%devonport%") \
        .eq("status", "sold") \
        .eq("bedrooms", 3) \
        .eq("bathrooms", 1) \
        .gt("price", 0) \
        .execute()

    all_listings = result.data
    print(f"\nTotal 3/1 sold in Devonport: {len(all_listings)}")

    # ── Separate Best Street ──
    best_street = [r for r in all_listings if "best" in r.get("address", "").lower()]
    print(f"Best Street sales in dataset: {len(best_street)}")

    # ── Classify using price/m² ──
    with_land = [r for r in all_listings if r.get("land_size") and r["land_size"] > 50]
    without_land = len(all_listings) - len(with_land)

    print(f"\nWith land size data: {len(with_land)}")
    if without_land:
        print(f"Without land size (excluded from classification): {without_land}")

    if len(with_land) < 10:
        print("✗ Insufficient data for reliable estimate")
        return

    # Price per m²
    for r in with_land:
        r["_ppm2"] = r["price"] / r["land_size"]

    median_ppm2 = statistics.median([r["_ppm2"] for r in with_land])
    print(f"Suburb median price/m²: ${median_ppm2:.0f}")

    renovated   = [r for r in with_land if r["_ppm2"] >= median_ppm2 * 1.15]
    unrenovated = [r for r in with_land if r["_ppm2"] <= median_ppm2 * 0.85]
    partial     = [r for r in with_land if median_ppm2 * 0.85 < r["_ppm2"] < median_ppm2 * 1.15]

    print(f"\nClassification breakdown:")
    print(f"  Renovated:   {len(renovated)}")
    print(f"  Unrenovated: {len(unrenovated)}")
    print(f"  Partial:     {len(partial)}")

    # ── Renovated price stats ──
    reno_prices = sorted([r["price"] for r in renovated])
    reno_median = statistics.median(reno_prices)
    reno_avg    = sum(reno_prices) / len(reno_prices)
    reno_p25    = reno_prices[int(len(reno_prices) * 0.25)]
    reno_p75    = reno_prices[int(len(reno_prices) * 0.75)]

    print(f"\n{'─'*60}")
    print(f"  RENOVATED 3/1 — DEVONPORT (n={len(reno_prices)})")
    print(f"{'─'*60}")
    print(f"  Median:  ${reno_median:,.0f}")
    print(f"  Average: ${reno_avg:,.0f}")
    print(f"  25th:    ${reno_p25:,.0f}")
    print(f"  75th:    ${reno_p75:,.0f}")
    print(f"  Range:   ${min(reno_prices):,} – ${max(reno_prices):,}")

    # ── Best Street reference sales ──
    print(f"\n{'─'*60}")
    print(f"  BEST STREET REFERENCE SALES (all 3/1, any condition)")
    print(f"{'─'*60}")
    if best_street:
        for r in sorted(best_street, key=lambda x: x["price"], reverse=True):
            land = r.get("land_size")
            ppm2 = r["price"] / land if land else None
            # Classify this sale
            if ppm2:
                if ppm2 >= median_ppm2 * 1.15:
                    bucket = "RENOVATED"
                elif ppm2 <= median_ppm2 * 0.85:
                    bucket = "UNRENOVATED"
                else:
                    bucket = "PARTIAL"
                ppm2_str = f"${ppm2:.0f}/m²"
            else:
                bucket = "unknown"
                ppm2_str = "no land data"

            print(f"  {r['address']}")
            print(f"    ${r['price']:,}  |  {land}m²  |  {ppm2_str}  →  {bucket}")
    else:
        print("  No Best Street sales in the sold dataset")

    # ── ARV recommendation ──
    print(f"\n{'═'*60}")
    print(f"  ARV ESTIMATE FOR RENOVATED 3/1 ON BEST STREET")
    print(f"{'═'*60}")
    print(f"  Base case (suburb renovated median): ${reno_median:,.0f}")
    print(f"  Conservative (25th pct):             ${reno_p25:,.0f}")
    print(f"  Optimistic (75th pct):               ${reno_p75:,.0f}")

    if best_street:
        best_prices = [r["price"] for r in best_street]
        best_avg = sum(best_prices) / len(best_prices)
        print(f"\n  Best Street avg (all conditions): ${best_avg:,.0f}  (n={len(best_street)})")
        print(f"  Note: {len(best_street)} sales is a thin sample — use suburb median as primary estimate.")

    print(f"\n  Recommendation: use ${reno_median:,.0f} as ARV base case.\n")


if __name__ == "__main__":
    run()

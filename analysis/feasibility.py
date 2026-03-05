# analysis/feasibility.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FEASIBILITY, RENO_COSTS, RENO_THRESHOLDS
from db.client import update_listing, get_suburb_gap, supabase

# ─────────────────────────────────────────
# RENO COST ESTIMATOR
# ─────────────────────────────────────────
def estimate_reno_cost(room_scores: dict) -> dict:
    """
    Estimate renovation cost from room scores.
    Returns itemised cost breakdown and total.
    """
    itemised = {}
    total = 0

    for room, costs in RENO_COSTS.items():
        score = room_scores.get(room, None)

        if score is None:
            # Room not assessed — use medium estimate as conservative default
            cost = costs["medium"]
            tier = "medium (assumed)"
        elif score >= 9:
            cost = costs["none"]
            tier = "none"
        elif score >= 7:
            cost = costs["low"]
            tier = "low"
        elif score >= 4:
            cost = costs["medium"]
            tier = "medium"
        else:
            cost = costs["high"]
            tier = "high"

        itemised[room] = {
            "score":    score,
            "tier":     tier,
            "cost":     cost
        }
        total += cost

    return {
        "itemised": itemised,
        "total":    total
    }


# ─────────────────────────────────────────
# ARV ESTIMATOR
# ─────────────────────────────────────────
def estimate_arv(listing: dict, suburb_gap: dict) -> dict:
    """
    Estimate After Renovation Value using suburb gap data.
    Falls back to price + gap if no suburb data available.
    """
    suburb = listing.get("suburb", "")
    state = listing.get("state", "TAS")
    bedrooms = listing.get("bedrooms", 3)
    listed_price = listing.get("price", 0)

    if suburb_gap and suburb_gap.get("renovated_median"):
        renovated_median = suburb_gap["renovated_median"]
        sample_size = suburb_gap.get("sample_size", 0)

        # Adjust for bedrooms vs suburb median (rough adjustment)
        # If we had per-bedroom data we'd use it, for now use median directly
        arv_estimate = renovated_median

        # Confidence based on sample size
        if sample_size >= 20:
            confidence = "high"
            confidence_range = 0.05   # ±5%
        elif sample_size >= 10:
            confidence = "medium"
            confidence_range = 0.10   # ±10%
        else:
            confidence = "low"
            confidence_range = 0.15   # ±15%

        return {
            "arv":              arv_estimate,
            "arv_low":          int(arv_estimate * (1 - confidence_range)),
            "arv_high":         int(arv_estimate * (1 + confidence_range)),
            "confidence":       confidence,
            "sample_size":      sample_size,
            "method":           "suburb_gap",
            "renovated_median": renovated_median
        }

    else:
        # No suburb gap data yet — estimate ARV as listed price + 40% uplift
        # This is a rough fallback until gap data is built up
        arv_estimate = int(listed_price * 1.40)

        return {
            "arv":          arv_estimate,
            "arv_low":      int(arv_estimate * 0.90),
            "arv_high":     int(arv_estimate * 1.10),
            "confidence":   "very low",
            "sample_size":  0,
            "method":       "fallback_uplift",
            "note":         "No suburb gap data available — using 40% uplift estimate"
        }


# ─────────────────────────────────────────
# BUYING COSTS
# ─────────────────────────────────────────
def calculate_buying_costs(purchase_price: int) -> dict:
    """Calculate stamp duty and conveyancing costs."""
    stamp_duty = int(purchase_price * FEASIBILITY["stamp_duty_rate"])
    conveyancing = FEASIBILITY["conveyancing_cost"]
    total = stamp_duty + conveyancing

    return {
        "stamp_duty":   stamp_duty,
        "conveyancing": conveyancing,
        "total":        total
    }


# ─────────────────────────────────────────
# HOLDING COSTS
# ─────────────────────────────────────────
def calculate_holding_costs(purchase_price: int) -> dict:
    """Calculate holding costs over the reno + sale period."""
    monthly_rate = FEASIBILITY["holding_rate"] / 12
    months = FEASIBILITY["holding_months"]
    total = int(purchase_price * monthly_rate * months)

    return {
        "months":       months,
        "monthly_rate": FEASIBILITY["holding_rate"],
        "total":        total
    }


# ─────────────────────────────────────────
# SELLING COSTS
# ─────────────────────────────────────────
def calculate_selling_costs(arv: int) -> dict:
    """Calculate agent commission and marketing costs on sale."""
    marketing = FEASIBILITY["marketing_cost"]
    total = marketing

    return {
        "marketing":    marketing,
        "total":        total
    }


# ─────────────────────────────────────────
# CORE FEASIBILITY FORMULA
# ─────────────────────────────────────────
def calculate_feasibility(listing: dict, room_scores: dict) -> dict:
    """
    Full feasibility calculation.

    Formula:
    ARV - Reno - Buying Costs - Holding Costs - Selling Costs - Profit Target
    = Max Offer Price

    Returns full breakdown + verdict.
    """
    listed_price = listing.get("price", 0)
    suburb = listing.get("suburb", "")
    state = listing.get("state", "TAS")

    # Get suburb gap data
    suburb_gap = get_suburb_gap(suburb, state)

    # Step 1: Estimate ARV
    arv_result = estimate_arv(listing, suburb_gap)
    arv = arv_result["arv"]

    # Step 2: Estimate reno cost
    reno_result = estimate_reno_cost(room_scores)
    reno_cost = reno_result["total"]

    # Step 3: Calculate all cost components at listed price
    buying_costs = calculate_buying_costs(listed_price)
    holding_costs = calculate_holding_costs(listed_price)
    selling_costs = calculate_selling_costs(arv)
    profit_target = int(arv * FEASIBILITY["profit_target"])

    # Step 4: Calculate max offer price
    max_offer = (
        arv
        - reno_cost
        - buying_costs["total"]
        - holding_costs["total"]
        - selling_costs["total"]
        - profit_target
    )

    # Step 5: Calculate margin at listed price
    total_costs_at_list = (
        listed_price
        + reno_cost
        + buying_costs["total"]
        + holding_costs["total"]
        + selling_costs["total"]
    )
    gross_profit_at_list = arv - total_costs_at_list
    margin_at_list = gross_profit_at_list / arv if arv > 0 else 0

    # Step 6: Scenario modelling
    scenarios = {
        "best": {
            "arv":          arv_result["arv_high"],
            "reno_cost":    int(reno_cost * 0.9),    # 10% under budget
            "margin":       round((arv_result["arv_high"] - total_costs_at_list * 0.95) / arv_result["arv_high"], 3)
        },
        "base": {
            "arv":          arv,
            "reno_cost":    reno_cost,
            "margin":       round(margin_at_list, 3)
        },
        "worst": {
            "arv":          arv_result["arv_low"],
            "reno_cost":    int(reno_cost * 1.2),    # 20% over budget
            "margin":       round((arv_result["arv_low"] - total_costs_at_list * 1.1) / arv_result["arv_low"], 3)
        }
    }

    # Step 7: Verdict
    threshold = FEASIBILITY["alert_threshold"]
    if margin_at_list >= threshold:
        verdict = "GO"
    elif max_offer >= listed_price * 0.92:
        # Within 8% negotiation range
        verdict = "WATCH"
    else:
        verdict = "PASS"

    result = {
        "listed_price":     listed_price,
        "arv":              arv,
        "arv_confidence":   arv_result["confidence"],
        "arv_method":       arv_result["method"],
        "reno_cost":        reno_cost,
        "reno_itemised":    reno_result["itemised"],
        "buying_costs":     buying_costs["total"],
        "holding_costs":    holding_costs["total"],
        "selling_costs":    selling_costs["total"],
        "profit_target":    profit_target,
        "max_offer":        max_offer,
        "margin_at_list":   round(margin_at_list, 3),
        "verdict":          verdict,
        "scenarios":        scenarios
    }

    # Update listing in database
    update_listing(listing["id"], {
        "feasibility_score":    round(margin_at_list * 100, 1),
        "max_offer_price":      max_offer,
        "margin_percent":       round(margin_at_list * 100, 1),
        "verdict":              verdict,
        "evaluated_at":         "now()"
    })

    return result


# ─────────────────────────────────────────
# PRINT REPORT
# ─────────────────────────────────────────
def print_feasibility_report(listing: dict, result: dict):
    """Print a formatted feasibility report."""
    print(f"\n{'='*55}")
    print(f"FEASIBILITY REPORT")
    print(f"{'='*55}")
    print(f"Property:     {listing.get('address', 'Unknown')}")
    print(f"Listed price: ${result['listed_price']:,}")
    print(f"ARV estimate: ${result['arv']:,} ({result['arv_confidence']} confidence)")
    print(f"")
    print(f"COST BREAKDOWN")
    print(f"{'─'*55}")
    print(f"Renovation:   ${result['reno_cost']:,}")

    for room, detail in result["reno_itemised"].items():
        score_str = f"{detail['score']}/10" if detail['score'] else "no score"
        print(f"  {room:<14} ${detail['cost']:>8,}  ({score_str} — {detail['tier']})")

    print(f"Buying costs: ${result['buying_costs']:,}  (stamp duty + conveyancing)")
    print(f"Holding:      ${result['holding_costs']:,}  ({FEASIBILITY['holding_months']} months)")
    print(f"Selling:      ${result['selling_costs']:,}  (marketing)")
    print(f"Profit target:${result['profit_target']:,}  (15% of ARV)")
    print(f"{'─'*55}")
    print(f"MAX OFFER:    ${result['max_offer']:,}")
    print(f"")
    print(f"VERDICT")
    print(f"{'─'*55}")
    print(f"Margin at list price: {result['margin_at_list']*100:.1f}%")

    verdict_emoji = {"GO": "🟢", "WATCH": "🟡", "PASS": "🔴"}.get(result['verdict'], "⚪")
    print(f"Verdict: {verdict_emoji}  {result['verdict']}")

    print(f"")
    print(f"SCENARIOS")
    print(f"{'─'*55}")
    for name, s in result["scenarios"].items():
        emoji = "📈" if name == "best" else "📊" if name == "base" else "📉"
        print(f"  {emoji} {name.upper():<6} ARV ${s['arv']:,}  |  "
              f"Reno ${s['reno_cost']:,}  |  "
              f"Margin {s['margin']*100:.1f}%")


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing feasibility calculator...\n")

    # Use test listing from database
    result = supabase.table("listings") \
        .select("*") \
        .eq("domain_id", "test-123") \
        .single() \
        .execute()

    if not result.data:
        print("✗ Test listing not found — run the photos test first")
        sys.exit(1)

    listing = result.data
    print(f"✓ Found listing: {listing['address']}")

    # Use the vision scores we got earlier
    room_scores = {
        "kitchen":  listing.get("renovation_score") or 3,
        "bathroom": listing.get("renovation_score") or 4,
    }

    # Run feasibility
    feasibility = calculate_feasibility(listing, room_scores)
    print_feasibility_report(listing, feasibility)
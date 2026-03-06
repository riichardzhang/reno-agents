"""
agents/insights.py

Claude Sonnet feasibility agent.
Takes a Domain listing + suburb gap data → GO/WATCH/PASS verdict with full analysis.
"""

import json
import os
from typing import Optional, Tuple
from anthropic import Anthropic
from db.client import get_client
from analysis.suburb_gaps import get_suburb_dom_stats

client = Anthropic()

# ---------------------------------------------------------------------------
# Reno cost tiers (based on avg reno score from classifiers)
# ---------------------------------------------------------------------------
RENO_COST_TIERS = {
    "cosmetic":    30_000,   # score 3–4, paint/floors/fixtures
    "standard":    50_000,   # score 2–3, + kitchen & bathroom (default)
    "full":        80_000,   # score 1–2, everything
}

def estimate_reno_cost(avg_reno_score: float) -> Tuple[int, str]:
    """Map average reno score to estimated cost and tier label."""
    if avg_reno_score >= 3.5:
        return RENO_COST_TIERS["cosmetic"], "cosmetic"
    elif avg_reno_score >= 2.0:
        return RENO_COST_TIERS["standard"], "standard"
    else:
        return RENO_COST_TIERS["full"], "full"


# ---------------------------------------------------------------------------
# Suburb gap data loader
# ---------------------------------------------------------------------------
def get_suburb_gap(suburb: str, gap_data: Optional[dict] = None) -> Optional[dict]:
    """
    Returns suburb gap stats dict.
    If gap_data is provided and contains the suburb, uses that.
    Otherwise queries Supabase suburb_gaps table.
    """
    suburb_key = suburb.strip().title()

    if gap_data and suburb_key in gap_data:
        return gap_data[suburb_key]

    # Query Supabase
    supabase = get_client()
    result = (
        supabase.table("suburb_gaps")
        .select("*")
        .ilike("suburb", suburb_key)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]
    return None


# ---------------------------------------------------------------------------
# Feasibility pre-calc (passed to Claude as grounding data)
# ---------------------------------------------------------------------------
def preflight_feasibility(
    asking_price: float,
    arv_estimate: float,
    reno_cost: int,
) -> dict:
    """
    Calculate costs and margins so Claude has concrete numbers to work with.
    Buying costs: stamp duty ~4% + conveyancing $2k
    Holding costs: 5 months at ~0.5%/month of purchase price
    Selling costs: $3k flat fee
    Target profit margin: 10% on capital injected at 80% LVR
    Capital injected = 20% deposit + buying costs + reno + holding + selling
    """
    stamp_duty = asking_price * 0.04
    conveyancing = 2_000
    buying_costs = stamp_duty + conveyancing

    holding_months = 5
    holding_costs = asking_price * 0.005 * holding_months

    selling_costs = 3_000

    total_costs = reno_cost + buying_costs + holding_costs + selling_costs

    # Capital injected at 80% LVR
    deposit = asking_price * 0.20
    capital_injected = deposit + buying_costs + reno_cost + holding_costs + selling_costs

    target_profit = capital_injected * 0.10

    # Max offer for 10% margin on capital injected (algebraic solution):
    # Let FC = reno_cost + conveyancing + selling_costs (fixed non-price costs)
    # profit = ARV - FC - 1.065*P
    # capital = 0.285*P + FC
    # profit = 0.10 * capital
    # => P_max = (ARV - FC * 1.10) / 1.0935
    fc = reno_cost + conveyancing + selling_costs
    max_offer = (arv_estimate - fc * 1.10) / 1.0935
    max_bid_above_asking = round(max_offer - asking_price)

    actual_profit = arv_estimate - asking_price - total_costs
    actual_margin = (actual_profit / capital_injected * 100) if capital_injected > 0 else 0

    return {
        "asking_price":             asking_price,
        "arv_estimate":             arv_estimate,
        "reno_cost":                reno_cost,
        "buying_costs":             round(buying_costs),
        "holding_costs":            round(holding_costs),
        "selling_costs":            round(selling_costs),
        "total_costs":              round(total_costs),
        "capital_injected":         round(capital_injected),
        "target_profit_10pct":      round(target_profit),
        "max_offer_price":          round(max_offer),
        "max_bid_above_asking":     max_bid_above_asking,
        "actual_profit_at_asking":  round(actual_profit),
        "actual_margin_pct":        round(actual_margin, 1),
    }


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
def build_prompt(listing: dict, suburb_gap: Optional[dict], feasibility: dict) -> str:
    suburb = listing.get("suburb", "Unknown")
    address = listing.get("address", "Unknown")
    asking_price = listing.get("price", 0)
    bedrooms = listing.get("bedrooms", "?")
    bathrooms = listing.get("bathrooms", "?")
    land_m2 = listing.get("land_size_m2", "?")
    dom = listing.get("days_on_market", "?")
    avg_reno_score = listing.get("avg_reno_score", "?")
    text_signals = listing.get("text_renovation_signals", {})
    description = listing.get("description", "")[:600]

    # DOM stats for suburb
    dom_stats = get_suburb_dom_stats(suburb)
    dom_section = (
        f"\nSUBURB MARKET VELOCITY ({suburb}):\n"
        f"  Avg days on market: {dom_stats.get('avg_dom') or 'insufficient data'}\n"
        f"  Median DOM:         {dom_stats.get('median_dom') or 'insufficient data'}\n"
        f"  Velocity:           {dom_stats.get('velocity', 'unknown')}\n"
        f"  Signal:             {dom_stats.get('signal', 'N/A')}\n"
        f"  Sample size:        {dom_stats.get('sample_size', 0)} active listings\n"
    )

    gap_section = ""
    if suburb_gap:
        def fmt(v):
            return f"${v:,}" if isinstance(v, (int, float)) else str(v)
        gap_section = (
            f"\nSUBURB GAP DATA ({suburb}):\n"
            f"  Median unrenovated: {fmt(suburb_gap.get('unrenovated_median', 'N/A'))}\n"
            f"  Median renovated:   {fmt(suburb_gap.get('renovated_median', 'N/A'))}\n"
            f"  Gap $:              {fmt(suburb_gap.get('gap_dollar', 'N/A'))}\n"
            f"  Gap %:              {suburb_gap.get('gap_percent', 'N/A')}%\n"
            f"  Sample size:        {suburb_gap.get('sample_size', 'N/A')} sales\n"
        )
    else:
        gap_section = f"\nSUBURB GAP DATA: Not available for {suburb}\n"

    f = feasibility
    prompt = f"""You are a property investment analyst specialising in renovation arbitrage in Tasmania, Australia.

Analyse this listing and provide a structured investment assessment.

=== LISTING ===
Address:        {address}
Suburb:         {suburb}
Asking price:   ${asking_price:,}
Bedrooms:       {bedrooms}
Bathrooms:      {bathrooms}
Land size:      {land_m2} m²
Days on market: {dom}
Avg reno score: {avg_reno_score} / 10 (lower = more unrenovated)
Text signals:   {json.dumps(text_signals, indent=2)}
Description:    {description}

{gap_section}
{dom_section}
=== PRE-CALCULATED FEASIBILITY (at asking price) ===
ARV estimate used:       ${f['arv_estimate']:,}
Reno cost estimate:      ${f['reno_cost']:,}
Buying costs:            ${f['buying_costs']:,}  (stamp duty ~4% + $2k conveyancing)
Holding costs:           ${f['holding_costs']:,}  (5 months at 0.5%/month)
Selling costs:           ${f['selling_costs']:,}  (flat fee)
Total costs:             ${f['total_costs']:,}
Capital injected (80% LVR): ${f['capital_injected']:,}  (20% deposit + all cash costs)
─────────────────────────────────────────────────
Max bid above asking for 10% margin on capital: ${f['max_bid_above_asking']:,}
Actual profit at asking:  ${f['actual_profit_at_asking']:,}
Actual margin on capital: {f['actual_margin_pct']}%

IMPORTANT CONTEXT:
- All margins are calculated on capital injected (not ARV). Target is 10%.
- Properties in this market sell AT or ABOVE asking. Do NOT suggest bidding below asking.
- "max_bid_above_asking" = maximum dollars ABOVE asking you can offer and still hit 10% margin on capital.
  A positive number means you have headroom above asking. A negative number means the deal doesn't work at asking.

=== YOUR TASK ===
Return ONLY a valid JSON object (no markdown, no preamble) with this exact structure:

{{
  "suburb_score": <1-10 integer, overall suburb investment attractiveness>,
  "arv_estimate": <integer dollar amount>,
  "arv_confidence": <"low"|"medium"|"high">,
  "arv_reasoning": "<2-3 sentences explaining ARV estimate>",
  "feasibility": {{
    "max_bid_above_asking": <integer, positive = can go above asking, negative = needs discount>,
    "profit_at_asking": <integer>,
    "margin_on_capital_pct": <float, margin as % of capital injected>,
    "verdict_at_asking": <"viable"|"borderline"|"not_viable">
  }},
  "scenarios": {{
    "best":  {{"reno_cost": <int>, "arv": <int>, "profit": <int>, "margin_on_capital_pct": <float>}},
    "base":  {{"reno_cost": <int>, "arv": <int>, "profit": <int>, "margin_on_capital_pct": <float>}},
    "worst": {{"reno_cost": <int>, "arv": <int>, "profit": <int>, "margin_on_capital_pct": <float>}}
  }},
  "red_flags": ["<flag1>", "<flag2>"],
  "positive_signals": ["<signal1>", "<signal2>"],
  "comparable_sales_notes": "<brief notes on suburb comps and market context>",
  "timing_recommendation": "<buy now|monitor>",
  "verdict": <"GO"|"WATCH"|"PASS">,
  "verdict_reasoning": "<2-3 sentences summarising the investment case>"
}}"""
    return prompt


# ---------------------------------------------------------------------------
# Main agent function
# ---------------------------------------------------------------------------
def analyse_listing(
    listing: dict,
    gap_data: Optional[dict] = None,
    arv_override: Optional[float] = None,
) -> dict:
    """
    Run the full insights agent on a single listing.

    Args:
        listing:      Dict from Domain (must include 'suburb', 'price', 'avg_reno_score')
        gap_data:     Optional pre-fetched suburb gap dict {suburb: {...}}
        arv_override: Optional manual ARV override (otherwise calculated from gap data)

    Returns:
        Dict with full Claude analysis + raw feasibility pre-calc
    """
    suburb = listing.get("suburb", "")
    asking_price = float(listing.get("price", 0))
    avg_reno_score = float(listing.get("avg_reno_score", 2.5))

    # 1. Get suburb gap
    suburb_gap = get_suburb_gap(suburb, gap_data)

    # 2. Estimate ARV
    if arv_override:
        arv = arv_override
    elif suburb_gap and suburb_gap.get("renovated_median"):
        arv = float(suburb_gap["renovated_median"])
    else:
        # Fallback: assume 25% uplift
        arv = asking_price * 1.25

    # 3. Estimate reno cost from score
    reno_cost, reno_tier = estimate_reno_cost(avg_reno_score)

    # 4. Pre-calc feasibility
    feasibility = preflight_feasibility(asking_price, arv, reno_cost)
    feasibility["reno_tier"] = reno_tier

    # 5. Build prompt and call Claude Sonnet
    prompt = build_prompt(listing, suburb_gap, feasibility)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = response.content[0].text.strip()

    # 6. Parse JSON response
    try:
        analysis = json.loads(raw_text)
    except json.JSONDecodeError:
        clean = raw_text.replace("```json", "").replace("```", "").strip()
        analysis = json.loads(clean)

    # 7. Attach metadata
    analysis["_meta"] = {
        "listing_id":           listing.get("id"),
        "address":              listing.get("address"),
        "suburb":               suburb,
        "asking_price":         asking_price,
        "preflight_feasibility": feasibility,
        "model":                "claude-sonnet-4-20250514",
    }

    return analysis


# ---------------------------------------------------------------------------
# Pretty printer for CLI use
# ---------------------------------------------------------------------------
def print_analysis(analysis: dict):
    meta = analysis.get("_meta", {})
    f = meta.get("preflight_feasibility", {})
    verdict = analysis.get("verdict", "?")
    verdict_emoji = {"GO": "✅", "WATCH": "👀", "PASS": "❌"}.get(verdict, "?")

    print(f"\n{'═'*60}")
    print(f"  {verdict_emoji}  {verdict}  —  {meta.get('address', 'Unknown')}")
    print(f"{'═'*60}")
    print(f"  Suburb:         {meta.get('suburb')} (score {analysis.get('suburb_score')}/10)")
    print(f"  Asking:         ${meta.get('asking_price', 0):,.0f}")
    print(f"  ARV estimate:   ${analysis.get('arv_estimate', 0):,}  ({analysis.get('arv_confidence')} confidence)")
    print(f"  Reno cost:      ${f.get('reno_cost', 0):,}  ({f.get('reno_tier')})")
    max_bid = analysis['feasibility']['max_bid_above_asking']
    max_bid_str = f"+${max_bid:,}" if max_bid >= 0 else f"-${abs(max_bid):,}"
    print(f"  Max bid above asking: {max_bid_str}")
    print(f"  Margin on capital:    {analysis['feasibility']['margin_on_capital_pct']}%  ({analysis['feasibility']['verdict_at_asking']})")
    print(f"  Capital injected:     ${f.get('capital_injected', 0):,}")
    print(f"\n  Timing:         {analysis.get('timing_recommendation')}")

    print(f"\n  Scenarios:")
    for label, s in analysis.get("scenarios", {}).items():
        print(f"    {label.capitalize():6s}  reno ${s['reno_cost']:,}  →  profit ${s['profit']:,}  ({s['margin_on_capital_pct']}% on capital)")

    if analysis.get("red_flags"):
        print(f"\n  ⚠️  Red flags:")
        for flag in analysis["red_flags"]:
            print(f"     • {flag}")

    if analysis.get("positive_signals"):
        print(f"\n  💚 Positives:")
        for sig in analysis["positive_signals"]:
            print(f"     • {sig}")

    print(f"\n  Verdict: {analysis.get('verdict_reasoning')}")
    print(f"{'═'*60}\n")


# ---------------------------------------------------------------------------
# CLI test runner
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_listing = {
        "id": "test-001",
        "address": "14 Example St",
        "suburb": "Devonport",
        "price": 430000,
        "bedrooms": 3,
        "bathrooms": 1,
        "land_size_m2": 620,
        "days_on_market": 34,
        "avg_reno_score": 2.4,
        "text_renovation_signals": {
            "unrenovated_keywords": ["original condition", "investors"],
            "renovated_keywords": [],
            "sentiment": "unrenovated",
        },
        "description": "Original condition 3 bed home on 620m2. Ideal for investors or renovators. Close to CBD.",
    }

    print("Running insights agent on test listing...")
    result = analyse_listing(test_listing)
    print_analysis(result)
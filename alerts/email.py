# alerts/email.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import resend
from datetime import datetime
from config import ALERTS, FEASIBILITY
from db.client import mark_listing_alerted

# ─────────────────────────────────────────
# BUILD LISTING CARD HTML (one property)
# ─────────────────────────────────────────
def build_listing_card_html(listing: dict, feasibility: dict, vision: dict = None, text_classification: dict = None) -> str:
    """Build HTML card for a single listing (no outer email structure)."""

    verdict = feasibility.get("verdict", "WATCH")
    verdict_color = {
        "GO":    "#16A34A",
        "WATCH": "#D97706",
        "PASS":  "#DC2626"
    }.get(verdict, "#666666")

    verdict_emoji = {
        "GO":    "🟢",
        "WATCH": "🟡",
        "PASS":  "🔴"
    }.get(verdict, "⚪")

    # Red flags section
    red_flags_html = ""
    if vision and vision.get("red_flags"):
        flags_html = "".join([
            f"<li style='color:#DC2626;margin:4px 0;'>⚠ {flag}</li>"
            for flag in vision["red_flags"]
        ])
        red_flags_html = f"""
        <div style='background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;padding:16px;margin:16px 0;'>
            <strong style='color:#DC2626;'>⚠ Red Flags Detected</strong>
            <ul style='margin:8px 0 0;padding-left:20px;'>
                {flags_html}
            </ul>
        </div>
        """

    # Reno breakdown rows
    reno_rows = ""
    if feasibility.get("reno_itemised"):
        for room, detail in feasibility["reno_itemised"].items():
            score_str = f"{detail['score']}/10" if detail.get('score') else "—"
            reno_rows += f"""
            <tr>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-transform:capitalize;'>{room}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:center;'>{score_str}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:center;'>{detail.get('tier','—')}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${detail.get('cost',0):,}</td>
            </tr>
            """

    # Scenario rows
    scenario_rows = ""
    if feasibility.get("scenarios"):
        scenario_emojis = {"best": "📈", "base": "📊", "worst": "📉"}
        for name, s in feasibility["scenarios"].items():
            margin = s.get("margin", 0) * 100
            profit = s.get("profit", 0)
            margin_color = "#16A34A" if margin >= 10 else "#D97706" if margin >= 0 else "#DC2626"
            scenario_rows += f"""
            <tr>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-transform:capitalize;'>
                    {scenario_emojis.get(name,'')} {name.title()}
                </td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${s.get('arv',0):,}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${s.get('reno_cost',0):,}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;color:{margin_color};font-weight:bold;'>
                    ${profit:,}
                </td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;color:{margin_color};font-weight:bold;'>
                    {margin:.1f}%
                </td>
            </tr>
            """

    # Text classification note
    text_note = ""
    if text_classification:
        text_note = f"""
        <p style='color:#666;font-size:13px;margin:4px 0;'>
            📝 Description analysis: <strong>{(text_classification.get('classification') or 'unknown').title()}</strong>
            ({text_classification.get('confidence', 0)*100:.0f}% confidence)
            — {', '.join(text_classification.get('signals', [])[:4])}
        </p>
        """

    margin_pct = feasibility.get('margin_at_list', 0) * 100
    margin_color = "#16A34A" if margin_pct >= 10 else "#D97706" if margin_pct >= 0 else "#DC2626"

    max_offer_price = feasibility.get('max_offer_price', 0)
    asking_price = listing.get('price', 0)
    max_bid = max_offer_price - asking_price  # always derived from the same source, never Claude's value
    max_bid_color = "#16A34A" if max_bid >= 0 else "#DC2626"
    delta_str = f"(+${max_bid:,} above asking)" if max_bid >= 0 else f"(-${abs(max_bid):,} below asking)"

    return f"""
        <!-- ── LISTING CARD ── -->
        <div style='margin-bottom:40px;border:2px solid {verdict_color};border-radius:12px;overflow:hidden;'>

            <!-- Verdict Banner -->
            <div style='background:{verdict_color};color:white;padding:14px 24px;'>
                <h2 style='margin:0;font-size:20px;'>{verdict_emoji} {verdict} — {listing.get('address','Unknown')}</h2>
                <p style='margin:4px 0 0;font-size:13px;opacity:0.9;'>
                    {listing.get('suburb','')}, {listing.get('state','')}
                    &nbsp;|&nbsp;
                    Margin on total cost: <strong>{margin_pct:.1f}%</strong>
                    &nbsp;|&nbsp;
                    Target: {FEASIBILITY['profit_target']*100:.0f}%
                </p>
            </div>

            <!-- Property Details -->
            <div style='background:#f9f9f9;padding:20px 24px;border-bottom:1px solid #e5e5e5;'>
                <div style='display:flex;gap:24px;flex-wrap:wrap;'>
                    <div>
                        <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>Listed Price</div>
                        <div style='font-size:22px;font-weight:bold;'>${listing.get('price',0):,}</div>
                    </div>
                    <div>
                        <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>Max Offer Price</div>
                        <div style='font-size:22px;font-weight:bold;color:{max_bid_color};'>${max_offer_price:,}</div>
                        <div style='font-size:12px;color:#999;'>{delta_str}</div>
                    </div>
                    <div>
                        <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>ARV Estimate</div>
                        <div style='font-size:22px;font-weight:bold;'>${feasibility.get('arv',0):,}</div>
                    </div>
                    <div>
                        <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>Beds / Baths</div>
                        <div style='font-size:22px;font-weight:bold;'>{listing.get('bedrooms','—')} / {listing.get('bathrooms','—')}</div>
                    </div>
                </div>

                <div style='margin-top:12px;'>
                    {text_note}
                    <p style='color:#666;font-size:13px;margin:4px 0;'>
                        🏗 Renovation classification:
                        <strong>{(listing.get('classification') or 'unknown').title()}</strong>
                        (avg score {listing.get('renovation_score','—')}/10)
                    </p>
                    <p style='color:#666;font-size:13px;margin:4px 0;'>
                        ARV confidence: <strong>{(feasibility.get('arv_confidence') or 'unknown').title()}</strong>
                        (method: {feasibility.get('arv_method','unknown')})
                    </p>
                </div>

                <a href='{listing.get('listing_url','#')}'
                   style='display:inline-block;margin-top:16px;background:#1a1a1a;color:white;
                          padding:10px 20px;border-radius:6px;text-decoration:none;font-size:14px;'>
                    View Listing →
                </a>
            </div>

            {red_flags_html}

            <!-- Cost Breakdown -->
            <div style='padding:20px 24px;border-bottom:1px solid #e5e5e5;'>
                <h3 style='margin:0 0 12px;font-size:15px;text-transform:uppercase;letter-spacing:1px;color:#666;'>
                    Cost Breakdown
                </h3>
                <table style='width:100%;border-collapse:collapse;font-size:14px;'>
                    <thead>
                        <tr style='background:#f5f5f5;'>
                            <th style='padding:8px 12px;text-align:left;font-weight:600;'>Room</th>
                            <th style='padding:8px 12px;text-align:center;font-weight:600;'>Score</th>
                            <th style='padding:8px 12px;text-align:center;font-weight:600;'>Level</th>
                            <th style='padding:8px 12px;text-align:right;font-weight:600;'>Cost</th>
                        </tr>
                    </thead>
                    <tbody>
                        {reno_rows}
                        <tr style='background:#f5f5f5;font-weight:bold;'>
                            <td colspan='3' style='padding:8px 12px;'>Total Renovation</td>
                            <td style='padding:8px 12px;text-align:right;'>${feasibility.get('reno_cost',0):,}</td>
                        </tr>
                    </tbody>
                </table>

                <table style='width:100%;border-collapse:collapse;font-size:14px;margin-top:12px;'>
                    <tr>
                        <td style='padding:4px 12px;color:#666;'>Buying costs (stamp duty + conveyancing)</td>
                        <td style='padding:4px 12px;text-align:right;'>${feasibility.get('buying_costs',0):,}</td>
                    </tr>
                    <tr>
                        <td style='padding:4px 12px;color:#666;'>Holding costs ({FEASIBILITY['holding_months']} months)</td>
                        <td style='padding:4px 12px;text-align:right;'>${feasibility.get('holding_costs',0):,}</td>
                    </tr>
                    <tr>
                        <td style='padding:4px 12px;color:#666;'>Selling costs (flat fee)</td>
                        <td style='padding:4px 12px;text-align:right;'>${feasibility.get('selling_costs',0):,}</td>
                    </tr>
                    <tr>
                        <td style='padding:4px 12px;color:#666;'>Cash equity injected (20% deposit + costs)</td>
                        <td style='padding:4px 12px;text-align:right;'>${feasibility.get('capital_injected',0):,}</td>
                    </tr>
                    <tr>
                        <td style='padding:4px 12px;color:#666;'>Profit target (10% on total cost)</td>
                        <td style='padding:4px 12px;text-align:right;'>${feasibility.get('profit_target',0):,}</td>
                    </tr>
                    <tr style='font-weight:bold;border-top:2px solid #1a1a1a;'>
                        <td style='padding:10px 12px;font-size:16px;'>MAX OFFER PRICE</td>
                        <td style='padding:10px 12px;text-align:right;font-size:16px;color:{max_bid_color};'>
                            ${max_offer_price:,}
                            <div style='font-size:12px;font-weight:normal;color:#999;'>{delta_str}</div>
                        </td>
                    </tr>
                </table>
            </div>

            <!-- Scenarios -->
            <div style='padding:20px 24px;'>
                <h3 style='margin:0 0 12px;font-size:15px;text-transform:uppercase;letter-spacing:1px;color:#666;'>
                    Scenario Modelling (margin on total cost)
                </h3>
                <table style='width:100%;border-collapse:collapse;font-size:14px;'>
                    <thead>
                        <tr style='background:#f5f5f5;'>
                            <th style='padding:8px 12px;text-align:left;font-weight:600;'>Scenario</th>
                            <th style='padding:8px 12px;text-align:right;font-weight:600;'>ARV</th>
                            <th style='padding:8px 12px;text-align:right;font-weight:600;'>Reno</th>
                            <th style='padding:8px 12px;text-align:right;font-weight:600;'>Profit $</th>
                            <th style='padding:8px 12px;text-align:right;font-weight:600;'>Margin %</th>
                        </tr>
                    </thead>
                    <tbody>
                        {scenario_rows}
                    </tbody>
                </table>
            </div>

        </div>
    """


# ─────────────────────────────────────────
# BUILD DIGEST EMAIL HTML (all deals)
# ─────────────────────────────────────────
def build_digest_email_html(alerts: list) -> str:
    """Build a single digest email containing all deal cards."""
    sorted_alerts = sorted(alerts, key=lambda a: 0 if a["feasibility"].get("verdict") == "GO" else 1)

    go_count    = sum(1 for a in alerts if a["feasibility"].get("verdict") == "GO")
    watch_count = sum(1 for a in alerts if a["feasibility"].get("verdict") == "WATCH")

    summary = f"{go_count} GO" + (f", {watch_count} WATCH" if watch_count else "")

    cards_html = "".join([
        build_listing_card_html(a["listing"], a["feasibility"], a.get("vision"), a.get("text"))
        for a in sorted_alerts
    ])

    return f"""
    <!DOCTYPE html>
    <html>
    <body style='font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px;color:#1a1a1a;'>

        <!-- Header -->
        <div style='background:#1a1a1a;color:white;padding:20px 24px;border-radius:12px 12px 0 0;margin-bottom:8px;'>
            <h1 style='margin:0;font-size:20px;'>🏠 Property Pipeline — Daily Digest</h1>
            <p style='margin:4px 0 0;color:#999;font-size:13px;'>
                {datetime.now(__import__('zoneinfo').ZoneInfo('Australia/Sydney')).strftime("%A %d %B %Y, %I:%M %p AEDT")}
                &nbsp;|&nbsp;
                {len(alerts)} deal{'s' if len(alerts) != 1 else ''} found: {summary}
            </p>
        </div>

        {cards_html}

        <!-- Footer -->
        <div style='background:#f5f5f5;padding:16px 24px;border-radius:12px;text-align:center;margin-top:8px;'>
            <p style='margin:0;color:#999;font-size:12px;'>
                Property Pipeline · Auto-generated digest · {datetime.now().strftime("%d/%m/%Y")}
            </p>
        </div>

    </body>
    </html>
    """


# ─────────────────────────────────────────
# SEND DIGEST EMAIL (all deals, one email)
# ─────────────────────────────────────────
def send_digest_email(alerts: list) -> bool:
    """
    Send a single digest email with all deals.
    alerts: list of dicts with keys: listing, feasibility, vision, text
    """
    if not alerts:
        return False

    try:
        resend.api_key = os.getenv("RESEND_API_KEY")

        go_count    = sum(1 for a in alerts if a["feasibility"].get("verdict") == "GO")
        watch_count = sum(1 for a in alerts if a["feasibility"].get("verdict") == "WATCH")
        summary = f"{go_count} GO" + (f", {watch_count} WATCH" if watch_count else "")
        from zoneinfo import ZoneInfo
        sydney_now = datetime.now(ZoneInfo("Australia/Sydney"))
        date_str = sydney_now.strftime("%A %d %B")
        subject = f"🏠 Property Pipeline — {date_str} — {len(alerts)} deal{'s' if len(alerts) != 1 else ''} ({summary})"

        html_content = build_digest_email_html(alerts)

        resend.Emails.send({
            "from":    "onboarding@resend.dev",
            "to":      "richard.zhang37@gmail.com",
            "subject": subject,
            "html":    html_content,
        })

        for a in alerts:
            try:
                mark_listing_alerted(a["listing"]["id"])
            except Exception:
                pass

        return True

    except Exception as e:
        print(f"  ✗ Failed to send digest email: {e}")
        return False


# ─────────────────────────────────────────
# SEND SINGLE ALERT (kept for test runner)
# ─────────────────────────────────────────
def send_alert(listing: dict, feasibility: dict, vision: dict = None, text_classification: dict = None) -> bool:
    """Send a single listing alert (wraps send_digest_email)."""
    return send_digest_email([{
        "listing":    listing,
        "feasibility": feasibility,
        "vision":     vision,
        "text":       text_classification,
    }])


# ─────────────────────────────────────────
# SUBURB GAP REPORT EMAIL
# ─────────────────────────────────────────
def send_suburb_gap_email(results: dict) -> bool:
    """
    Send a suburb gap analysis report email.
    results: dict of suburb -> gap data (from run_gap_analysis)
    """
    if not results:
        return False

    try:
        resend.api_key = os.getenv("RESEND_API_KEY")

        from zoneinfo import ZoneInfo
        sydney_now = datetime.now(ZoneInfo("Australia/Sydney"))
        date_str = sydney_now.strftime("%A %d %B")
        subject = f"📊 Suburb Gap Report — {date_str} — {len(results)} suburbs"

        # Sort by gap % descending
        sorted_results = sorted(results.items(), key=lambda x: -x[1].get("gap_percent", 0))

        rows_html = ""
        for suburb, data in sorted_results:
            gap_pct = data.get("gap_percent", 0)
            gap_color = "#16A34A" if gap_pct >= 30 else "#D97706" if gap_pct >= 20 else "#DC2626"
            rows_html += f"""
            <tr>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;'>{suburb}</td>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${data.get('unrenovated_median', 0):,}</td>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${data.get('renovated_median', 0):,}</td>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${data.get('gap_dollar', 0):,}</td>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:right;font-weight:bold;color:{gap_color};'>{gap_pct:.1f}%</td>
                <td style='padding:8px 12px;border-bottom:1px solid #f0f0f0;text-align:right;color:#999;'>{data.get('sample_size', 0)}</td>
            </tr>
            """

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <body style='font-family:Arial,sans-serif;max-width:800px;margin:0 auto;padding:20px;color:#1a1a1a;'>

            <div style='background:#1a1a1a;color:white;padding:20px 24px;border-radius:12px 12px 0 0;margin-bottom:8px;'>
                <h1 style='margin:0;font-size:20px;'>📊 Suburb Gap Report</h1>
                <p style='margin:4px 0 0;color:#999;font-size:13px;'>
                    {sydney_now.strftime("%A %d %B %Y, %I:%M %p AEDT")}
                    &nbsp;|&nbsp;
                    {len(results)} suburbs analysed
                </p>
            </div>

            <div style='border:1px solid #e5e5e5;border-radius:0 0 12px 12px;overflow:hidden;'>
                <table style='width:100%;border-collapse:collapse;font-size:14px;'>
                    <thead>
                        <tr style='background:#f5f5f5;'>
                            <th style='padding:10px 12px;text-align:left;font-weight:600;'>Suburb</th>
                            <th style='padding:10px 12px;text-align:right;font-weight:600;'>Unreno Median</th>
                            <th style='padding:10px 12px;text-align:right;font-weight:600;'>Reno Median</th>
                            <th style='padding:10px 12px;text-align:right;font-weight:600;'>Gap $</th>
                            <th style='padding:10px 12px;text-align:right;font-weight:600;'>Gap %</th>
                            <th style='padding:10px 12px;text-align:right;font-weight:600;'>Sales</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>

            <div style='background:#f5f5f5;padding:16px 24px;border-radius:12px;text-align:center;margin-top:16px;'>
                <p style='margin:0;color:#999;font-size:12px;'>
                    Property Pipeline · Suburb Gap Report · {datetime.now().strftime("%d/%m/%Y")}
                    &nbsp;|&nbsp; Gap ≥30%: 🟢 &nbsp; 20–30%: 🟡 &nbsp; &lt;20%: 🔴
                </p>
            </div>

        </body>
        </html>
        """

        resend.Emails.send({
            "from":    "onboarding@resend.dev",
            "to":      "richard.zhang37@gmail.com",
            "subject": subject,
            "html":    html_content,
        })

        return True

    except Exception as e:
        print(f"  ✗ Failed to send gap report email: {e}")
        return False


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing Resend email alert...\n")

    from db.client import supabase
    result = supabase.table("listings") \
        .select("*") \
        .eq("status", "active") \
        .limit(1) \
        .execute()

    if not result.data:
        print("✗ No listings found in DB")
        sys.exit(1)

    listing = result.data[0]
    print(f"✓ Found listing: {listing['address']}")

    mock_feasibility = {
        "verdict":        "WATCH",
        "arv":            630000,
        "arv_confidence": "medium",
        "arv_method":     "suburb_gap",
        "reno_cost":      50000,
        "buying_costs":   20000,
        "holding_costs":  11250,
        "selling_costs":  17175,
        "profit_target":  94500,
        "max_offer":      436250,
        "margin_at_list": 0.128,
        "scenarios": {
            "best":  {"arv": 693000, "reno_cost": 40000, "margin": 0.247},
            "base":  {"arv": 630000, "reno_cost": 50000, "margin": 0.128},
            "worst": {"arv": 567000, "reno_cost": 65000, "margin": -0.066},
        }
    }

    mock_vision = {
        "red_flags": [
            "Possible water damage near bathroom window",
            "Original kitchen fixtures — full refit required"
        ]
    }

    mock_text = {
        "classification": "unrenovated",
        "confidence":     0.85,
        "signals":        ["original condition", "period features", "deceased estate"]
    }

    print("Sending test alert email to richard.zhang37@gmail.com...")
    success = send_alert(listing, mock_feasibility, mock_vision, mock_text)

    if success:
        print("✓ Check your inbox!")
    else:
        print("✗ Email failed — check your RESEND_API_KEY in .env")
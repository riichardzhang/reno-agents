# alerts/email.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from config import ALERTS, FEASIBILITY
from db.client import mark_listing_alerted

# ─────────────────────────────────────────
# BUILD EMAIL HTML
# ─────────────────────────────────────────
def build_email_html(listing: dict, feasibility: dict, vision: dict = None, text_classification: dict = None) -> str:
    """Build a formatted HTML email for a listing alert."""

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
            margin_color = "#16A34A" if margin >= 15 else "#D97706" if margin >= 0 else "#DC2626"
            scenario_rows += f"""
            <tr>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-transform:capitalize;'>
                    {scenario_emojis.get(name,'')} {name.title()}
                </td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${s.get('arv',0):,}</td>
                <td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right;'>${s.get('reno_cost',0):,}</td>
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
            📝 Description analysis: <strong>{text_classification.get('classification','unknown').title()}</strong>
            ({text_classification.get('confidence',0)*100:.0f}% confidence)
            — {', '.join(text_classification.get('signals', [])[:4])}
        </p>
        """

    margin_pct = feasibility.get('margin_at_list', 0) * 100
    margin_color = "#16A34A" if margin_pct >= 15 else "#D97706" if margin_pct >= 0 else "#DC2626"

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style='font-family:Arial,sans-serif;max-width:680px;margin:0 auto;padding:20px;color:#1a1a1a;'>

        <!-- Header -->
        <div style='background:#1a1a1a;color:white;padding:20px 24px;border-radius:12px 12px 0 0;'>
            <h1 style='margin:0;font-size:20px;'>🏠 Property Pipeline Alert</h1>
            <p style='margin:4px 0 0;color:#999;font-size:13px;'>{datetime.now().strftime("%A %d %B %Y, %I:%M %p")}</p>
        </div>

        <!-- Verdict Banner -->
        <div style='background:{verdict_color};color:white;padding:16px 24px;'>
            <h2 style='margin:0;font-size:24px;'>{verdict_emoji} {verdict}</h2>
            <p style='margin:4px 0 0;font-size:14px;opacity:0.9;'>
                Margin at list price: <strong>{margin_pct:.1f}%</strong>
                &nbsp;|&nbsp;
                Target: {FEASIBILITY['profit_target']*100:.0f}%
            </p>
        </div>

        <!-- Property Details -->
        <div style='background:#f9f9f9;padding:20px 24px;border:1px solid #e5e5e5;'>
            <h2 style='margin:0 0 4px;font-size:18px;'>{listing.get('address','Unknown')}</h2>
            <p style='margin:0;color:#666;'>{listing.get('suburb','')}, {listing.get('state','')}</p>

            <div style='display:flex;gap:24px;margin-top:16px;flex-wrap:wrap;'>
                <div>
                    <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>Listed Price</div>
                    <div style='font-size:22px;font-weight:bold;'>${listing.get('price',0):,}</div>
                </div>
                <div>
                    <div style='font-size:12px;color:#999;text-transform:uppercase;letter-spacing:1px;'>Max Offer</div>
                    <div style='font-size:22px;font-weight:bold;color:{verdict_color};'>${feasibility.get('max_offer',0):,}</div>
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
                    <strong>{listing.get('classification','unknown').title()}</strong>
                    (avg score {listing.get('renovation_score','—')}/10)
                </p>
                <p style='color:#666;font-size:13px;margin:4px 0;'>
                    ARV confidence: <strong>{feasibility.get('arv_confidence','unknown').title()}</strong>
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
        <div style='padding:20px 24px;border:1px solid #e5e5e5;border-top:none;'>
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
                    <td style='padding:4px 12px;color:#666;'>Selling costs (marketing)</td>
                    <td style='padding:4px 12px;text-align:right;'>${feasibility.get('selling_costs',0):,}</td>
                </tr>
                <tr>
                    <td style='padding:4px 12px;color:#666;'>Profit target (15% of ARV)</td>
                    <td style='padding:4px 12px;text-align:right;'>${feasibility.get('profit_target',0):,}</td>
                </tr>
                <tr style='font-weight:bold;border-top:2px solid #1a1a1a;'>
                    <td style='padding:10px 12px;font-size:16px;'>MAX OFFER PRICE</td>
                    <td style='padding:10px 12px;text-align:right;font-size:16px;color:{verdict_color};'>
                        ${feasibility.get('max_offer',0):,}
                    </td>
                </tr>
            </table>
        </div>

        <!-- Scenarios -->
        <div style='padding:20px 24px;border:1px solid #e5e5e5;border-top:none;'>
            <h3 style='margin:0 0 12px;font-size:15px;text-transform:uppercase;letter-spacing:1px;color:#666;'>
                Scenario Modelling
            </h3>
            <table style='width:100%;border-collapse:collapse;font-size:14px;'>
                <thead>
                    <tr style='background:#f5f5f5;'>
                        <th style='padding:8px 12px;text-align:left;font-weight:600;'>Scenario</th>
                        <th style='padding:8px 12px;text-align:right;font-weight:600;'>ARV</th>
                        <th style='padding:8px 12px;text-align:right;font-weight:600;'>Reno</th>
                        <th style='padding:8px 12px;text-align:right;font-weight:600;'>Margin</th>
                    </tr>
                </thead>
                <tbody>
                    {scenario_rows}
                </tbody>
            </table>
        </div>

        <!-- Footer -->
        <div style='background:#f5f5f5;padding:16px 24px;border-radius:0 0 12px 12px;
                    border:1px solid #e5e5e5;border-top:none;text-align:center;'>
            <p style='margin:0;color:#999;font-size:12px;'>
                Property Pipeline · Auto-generated alert · {datetime.now().strftime("%d/%m/%Y")}
            </p>
        </div>

    </body>
    </html>
    """
    return html


# ─────────────────────────────────────────
# SEND EMAIL
# ─────────────────────────────────────────
def send_alert(listing: dict, feasibility: dict, vision: dict = None, text_classification: dict = None) -> bool:
    """
    Send a formatted HTML email alert for a listing.
    Returns True if sent successfully.
    """
    try:
        verdict = feasibility.get("verdict", "WATCH")
        subject = (
            f"{verdict} 🏠 {listing.get('address','Unknown')} — "
            f"${listing.get('price',0):,} | "
            f"Max offer ${feasibility.get('max_offer',0):,} | "
            f"Margin {feasibility.get('margin_at_list',0)*100:.1f}%"
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = ALERTS["alert_from"]
        msg["To"]      = ALERTS["alert_to"]

        html_content = build_email_html(listing, feasibility, vision, text_classification)
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(ALERTS["smtp_host"], ALERTS["smtp_port"]) as server:
            server.starttls()
            server.login(ALERTS["alert_from"], os.getenv("ALERT_EMAIL_PASSWORD"))
            server.sendmail(ALERTS["alert_from"], ALERTS["alert_to"], msg.as_string())

        print(f"  ✓ Alert sent: {subject}")
        mark_listing_alerted(listing["id"])
        return True

    except Exception as e:
        print(f"  ✗ Failed to send alert: {e}")
        return False


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing email alert...\n")

    # Pull test listing from database
    from db.client import supabase
    result = supabase.table("listings") \
        .select("*") \
        .eq("domain_id", "test-123") \
        .single() \
        .execute()

    if not result.data:
        print("✗ Test listing not found")
        sys.exit(1)

    listing = result.data
    print(f"✓ Found listing: {listing['address']}")

    # Mock feasibility result
    mock_feasibility = {
        "listed_price":     450000,
        "arv":              630000,
        "arv_confidence":   "very low",
        "arv_method":       "fallback_uplift",
        "reno_cost":        65000,
        "reno_itemised": {
            "kitchen":      {"score": 2, "tier": "high",             "cost": 30000},
            "bathroom":     {"score": 3, "tier": "high",             "cost": 22000},
            "floors":       {"score": None, "tier": "medium (assumed)", "cost": 6000},
            "paint":        {"score": None, "tier": "medium (assumed)", "cost": 4000},
            "landscaping":  {"score": None, "tier": "medium (assumed)", "cost": 3000},
        },
        "buying_costs":     20000,
        "holding_costs":    11250,
        "selling_costs":    3000,
        "profit_target":    94500,
        "max_offer":        436250,
        "margin_at_list":   0.128,
        "verdict":          "WATCH",
        "scenarios": {
            "best":  {"arv": 693000, "reno_cost": 58500, "margin": 0.247},
            "base":  {"arv": 630000, "reno_cost": 65000, "margin": 0.128},
            "worst": {"arv": 567000, "reno_cost": 78000, "margin": -0.066},
        }
    }

    mock_vision = {
        "red_flags": [
            "Possible water damage near bathroom window",
            "Original kitchen fixtures — full refit required"
        ]
    }

    mock_text = {
        "classification":   "unrenovated",
        "confidence":       0.85,
        "signals":          ["original condition", "period features", "deceased estate"]
    }

    print("Sending test alert email...")
    success = send_alert(listing, mock_feasibility, mock_vision, mock_text)

    if success:
        print("✓ Check your inbox!")
    else:
        print("✗ Email failed — check your ALERT_EMAIL and ALERT_EMAIL_PASSWORD in .env")
        print("\nNote: For Gmail you need an App Password, not your regular password.")
        print("Go to: Google Account → Security → 2-Step Verification → App Passwords")

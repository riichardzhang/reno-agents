# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# API CREDENTIALS
# ─────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
DOMAIN_CLIENT_ID = os.getenv("DOMAIN_CLIENT_ID")
DOMAIN_CLIENT_SECRET = os.getenv("DOMAIN_CLIENT_SECRET")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")
ALERT_EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD")

# ─────────────────────────────────────────
# TARGET SUBURBS
# ─────────────────────────────────────────
SUBURBS = {
    "TAS": {

        "greater_hobart": [
            {"name": "Hobart",          "postcode": "7000"},
            {"name": "Glebe",           "postcode": "7000"},
            {"name": "Mount Stuart",    "postcode": "7000"},
            {"name": "North Hobart",    "postcode": "7000"},
            {"name": "Queens Domain",   "postcode": "7000"},
            {"name": "West Hobart",     "postcode": "7000"},
            {"name": "Battery Point",   "postcode": "7004"},
            {"name": "South Hobart",    "postcode": "7004"},
            {"name": "Dynnyrne",        "postcode": "7005"},
            {"name": "Lower Sandy Bay", "postcode": "7005"},
            {"name": "Sandy Bay",       "postcode": "7005"},
            {"name": "Mount Nelson",    "postcode": "7007"},
            {"name": "Tolmans Hill",    "postcode": "7007"},
            {"name": "Lenah Valley",    "postcode": "7008"},
            {"name": "New Town",        "postcode": "7008"},
            {"name": "Derwent Park",    "postcode": "7009"},
            {"name": "Lutana",          "postcode": "7009"},
            {"name": "Moonah",          "postcode": "7009"},
            {"name": "West Moonah",     "postcode": "7009"},
            {"name": "Dowsing Point",   "postcode": "7010"},
            {"name": "Glenorchy",       "postcode": "7010"},
            {"name": "Goodwood",        "postcode": "7010"},
            {"name": "Montrose",        "postcode": "7010"},
            {"name": "Rosetta",         "postcode": "7010"},
            {"name": "Austins Ferry",   "postcode": "7011"},
            {"name": "Berriedale",      "postcode": "7011"},
            {"name": "Chigwell",        "postcode": "7011"},
            {"name": "Claremont",       "postcode": "7011"},
            {"name": "Collinsvale",     "postcode": "7012"},
            {"name": "Glenlusk",        "postcode": "7012"},
            {"name": "Geilston Bay",    "postcode": "7015"},
            {"name": "Lindisfarne",     "postcode": "7015"},
            {"name": "Rose Bay",        "postcode": "7015"},
            {"name": "Risdon Vale",     "postcode": "7016"},
            {"name": "Old Beach",       "postcode": "7017"},
            {"name": "Bellerive",       "postcode": "7018"},
            {"name": "Howrah",          "postcode": "7018"},
            {"name": "Montagu Bay",     "postcode": "7018"},
            {"name": "Mornington",      "postcode": "7018"},
            {"name": "Rosny",           "postcode": "7018"},
            {"name": "Rosny Park",      "postcode": "7018"},
            {"name": "Tranmere",        "postcode": "7018"},
            {"name": "Warrane",         "postcode": "7018"},
            {"name": "Acton Park",      "postcode": "7170"},
            {"name": "Rokeby",          "postcode": "7019"},
            {"name": "Clarendon Vale",  "postcode": "7019"},
            {"name": "Lauderdale",      "postcode": "7021"},
            {"name": "Cremorne",        "postcode": "7023"},
            {"name": "Blackmans Bay",   "postcode": "7052"},
            {"name": "Bonnet Hill",     "postcode": "7053"},
            {"name": "Huntingfield",    "postcode": "7055"},
            {"name": "Kingston",        "postcode": "7050"},
            {"name": "Kingston Beach",  "postcode": "7050"},
            {"name": "Margate",         "postcode": "7054"},
            {"name": "Snug",            "postcode": "7054"},
            {"name": "Huonville",       "postcode": "7109"},
            {"name": "Brighton",        "postcode": "7030"},
            {"name": "Bridgewater",     "postcode": "7030"},
            {"name": "Gagebrook",       "postcode": "7030"},
            {"name": "Herdsmans Cove",  "postcode": "7030"},
            {"name": "Granton",         "postcode": "7030"},
        ],

        "greater_launceston": [
            {"name": "Launceston",          "postcode": "7250"},
            {"name": "Blackstone Heights",  "postcode": "7250"},
            {"name": "East Launceston",     "postcode": "7250"},
            {"name": "Elphin",              "postcode": "7250"},
            {"name": "Newstead",            "postcode": "7250"},
            {"name": "Norwood",             "postcode": "7250"},
            {"name": "Prospect",            "postcode": "7250"},
            {"name": "Prospect Vale",       "postcode": "7250"},
            {"name": "Ravenswood",          "postcode": "7250"},
            {"name": "Riverside",           "postcode": "7250"},
            {"name": "St Leonards",         "postcode": "7250"},
            {"name": "Summerhill",          "postcode": "7250"},
            {"name": "Travellers Rest",     "postcode": "7250"},
            {"name": "Trevallyn",           "postcode": "7250"},
            {"name": "Waverley",            "postcode": "7250"},
            {"name": "West Launceston",     "postcode": "7250"},
            {"name": "South Launceston",    "postcode": "7249"},
            {"name": "Youngtown",           "postcode": "7249"},
            {"name": "Kings Meadows",       "postcode": "7249"},
            {"name": "Newnham",             "postcode": "7248"},
            {"name": "Mowbray",             "postcode": "7248"},
            {"name": "Rocherlea",           "postcode": "7248"},
            {"name": "Mayfield",            "postcode": "7248"},
            {"name": "Invermay",            "postcode": "7248"},
            {"name": "Inveresk",            "postcode": "7248"},
            {"name": "Hadspen",             "postcode": "7290"},
            {"name": "Legana",              "postcode": "7277"},
            {"name": "Rosevears",           "postcode": "7277"},
            {"name": "Exeter",              "postcode": "7275"},
            {"name": "Gravelly Beach",      "postcode": "7276"},
            {"name": "Lanena",              "postcode": "7275"},
            {"name": "Relbia",              "postcode": "7258"},
            {"name": "Ravenswood",          "postcode": "7250"},
        ],

        "ulverstone": [
            {"name": "Ulverstone",      "postcode": "7315"},
            {"name": "West Ulverstone", "postcode": "7315"},
            {"name": "Turners Beach",   "postcode": "7315"},
            {"name": "Leith",           "postcode": "7315"},
        ],

        "devonport": [
            {"name": "Devonport",       "postcode": "7310"},
            {"name": "East Devonport",  "postcode": "7310"},
            {"name": "Don",             "postcode": "7310"},
            {"name": "Miandetta",       "postcode": "7310"},
            {"name": "Spreyton",        "postcode": "7310"},
            {"name": "South Spreyton",  "postcode": "7310"},
            {"name": "Stony Rise",      "postcode": "7310"},
            {"name": "Quoiba",          "postcode": "7310"},
            {"name": "Eugenana",        "postcode": "7310"},
            {"name": "Lillico",         "postcode": "7310"},
            {"name": "Tugrah",          "postcode": "7310"},
            {"name": "Tarleton",        "postcode": "7310"},
            {"name": "Forth",           "postcode": "7310"},
            {"name": "Forthside",       "postcode": "7310"},
        ],
    }
    # NSW and QLD to be added later
}

# Flat list for easy iteration across all regions
ALL_SUBURBS = [
    suburb
    for region in SUBURBS["TAS"].values()
    for suburb in region
]

# ─────────────────────────────────────────
# LISTING FILTERS
# ─────────────────────────────────────────
FILTERS = {
    "min_price":        300000,
    "max_price":        650000,
    "min_bedrooms":     3,
    "max_bedrooms":     5,
    "property_types":   ["house"],
    "sort":             "date-desc"
}

# ─────────────────────────────────────────
# FEASIBILITY CONSTANTS
# ─────────────────────────────────────────
FEASIBILITY = {
    "stamp_duty_rate":      0.04,       # 4% of purchase price
    "conveyancing_cost":    2000,       # flat fee
    "holding_months":       5,          # average reno + hold period
    "holding_rate":         0.06,       # annual interest rate on purchase price
    "marketing_cost":       3000,       # flat fee on sale
    "profit_target":        0.10,       # minimum 10% margin on capital injected
    "alert_threshold":      0.10,       # only alert if margin >= this
}

# ─────────────────────────────────────────
# RENOVATION COST RANGES ($AUD)
# ─────────────────────────────────────────
# Score 1-3 = high cost (heavy reno needed)
# Score 4-6 = medium cost
# Score 7-8 = low cost (light refresh only)
# Score 9-10 = no reno needed (skip)
RENO_COSTS = {
    "kitchen": {
        "high":     30000,
        "medium":   18000,
        "low":      8000,
        "none":     0
    },
    "bathroom": {
        "high":     22000,
        "medium":   12000,
        "low":      5000,
        "none":     0
    },
    "floors": {
        "high":     10000,
        "medium":   6000,
        "low":      2000,
        "none":     0
    },
    "paint": {
        "high":     8000,
        "medium":   4000,
        "low":      2000,
        "none":     0
    },
    "landscaping": {
        "high":     8000,
        "medium":   3000,
        "low":      1000,
        "none":     0
    }
}

RENO_THRESHOLDS = {
    "unrenovated":  4,
    "renovated":    7,
}

# ─────────────────────────────────────────
# CLAUDE MODELS
# ─────────────────────────────────────────
MODELS = {
    "classification":   "claude-haiku-4-5-20251001",
    "analysis":         "claude-sonnet-4-6",
}

# ─────────────────────────────────────────
# PHOTO SETTINGS
# ─────────────────────────────────────────
PHOTOS = {
    "max_photos_to_download":   8,
    "target_rooms":             ["kitchen", "bathroom"],
    "position_heuristic": {
        "kitchen":  [2, 3],
        "bathroom": [3, 4],
    },
    "keywords": {
        "kitchen":  ["kitchen", "ktchn", "dining"],
        "bathroom": ["bathroom", "bath", "ensuite", "wc", "toilet"],
    }
}

# ─────────────────────────────────────────
# DATA SOURCES
# ─────────────────────────────────────────
SOURCES = {
    "use_domain_api":   False,
    "use_apify":        True,
    "apify_actor":      "easyapi/domain-com-au-property-scraper",
}

# ─────────────────────────────────────────
# ALERT SETTINGS
# ─────────────────────────────────────────
ALERTS = {
    "smtp_host":        "smtp.gmail.com",
    "smtp_port":        587,
    "alert_to":         os.getenv("ALERT_EMAIL"),
    "alert_from":       os.getenv("ALERT_EMAIL"),
    "send_daily":       True,
    "min_verdict":      "WATCH"
}
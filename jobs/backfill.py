# jobs/backfill.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import time
from config import APIFY_API_TOKEN, SOURCES
from db.client import supabase

# ─────────────────────────────────────────
# KEY SUBURBS TO BACKFILL
# Priority suburbs only — enough for reliable medians
# ─────────────────────────────────────────
BACKFILL_SUBURBS = [
    # Greater Hobart
    {"name": "hobart",          "region": "greater_hobart"},
    {"name": "sandy-bay",       "region": "greater_hobart"},
    {"name": "battery-point",   "region": "greater_hobart"},
    {"name": "west-hobart",     "region": "greater_hobart"},
    {"name": "north-hobart",    "region": "greater_hobart"},
    {"name": "south-hobart",    "region": "greater_hobart"},
    {"name": "moonah",          "region": "greater_hobart"},
    {"name": "glenorchy",       "region": "greater_hobart"},
    {"name": "kingston",        "region": "greater_hobart"},
    {"name": "blackmans-bay",   "region": "greater_hobart"},
    {"name": "howrah",          "region": "greater_hobart"},
    {"name": "lindisfarne",     "region": "greater_hobart"},
    {"name": "bellerive",       "region": "greater_hobart"},
    {"name": "new-town",        "region": "greater_hobart"},
    {"name": "lenah-valley",    "region": "greater_hobart"},

    # Greater Launceston
    {"name": "launceston",      "region": "greater_launceston"},
    {"name": "newstead",        "region": "greater_launceston"},
    {"name": "prospect",        "region": "greater_launceston"},
    {"name": "kings-meadows",   "region": "greater_launceston"},
    {"name": "youngtown",       "region": "greater_launceston"},
    {"name": "newnham",         "region": "greater_launceston"},
    {"name": "mowbray",         "region": "greater_launceston"},
    {"name": "riverside",       "region": "greater_launceston"},
    {"name": "trevallyn",       "region": "greater_launceston"},
    {"name": "hadspen",         "region": "greater_launceston"},

    # Devonport
    {"name": "devonport",       "region": "devonport"},
    {"name": "east-devonport",  "region": "devonport"},
    {"name": "miandetta",       "region": "devonport"},
    {"name": "spreyton",        "region": "devonport"},

    # Ulverstone
    {"name": "ulverstone",      "region": "ulverstone"},
    {"name": "turners-beach",   "region": "ulverstone"},
    {"name": "west-ulverstone",     "region": "ulverstone"},
]

def build_sold_url(suburb: dict) -> str:
    """Build a Domain sold listings URL for a suburb."""
    name = suburb["name"].lower()
    return (
        f"https://www.domain.com.au/sold-listings/{name}-tas/house/"
        f"?bedrooms=3-5&price=300000-750000&excludepricewithheld=1"
    )
# ─────────────────────────────────────────
# NORMALISE SOLD LISTING
# ─────────────────────────────────────────
def normalise_sold(raw: dict, suburb: dict) -> dict:
    """Convert Apify sold listing to our database format."""
    # Extract numeric price
    price_str = raw.get("price", "0")
    price = 0
    if isinstance(price_str, str):
        digits = ''.join(filter(str.isdigit, price_str))
        price = int(digits) if digits else 0
    elif isinstance(price_str, (int, float)):
        price = int(price_str)

    # Address object
    address_obj = raw.get("address", {})
    full_address = f"{address_obj.get('street', '')} {address_obj.get('suburb', '')} {address_obj.get('state', '')} {address_obj.get('postcode', '')}".strip()

    # Features
    features = raw.get("features", {})

    # Use URL as unique ID
    url = raw.get("url", "")
    domain_id = f"sold_{url.split('/')[-1]}" if url else ""

    return {
        "domain_id":    domain_id,
        "address":      full_address,
        "suburb":       address_obj.get("suburb", suburb["name"]).title(),
        "state":        address_obj.get("state", "TAS"),
        "price":        price,
        "bedrooms":     features.get("beds", None),
        "bathrooms":    features.get("baths", None),
        "land_size":    int(features.get("landSize", None) or 0) or None,
        "listing_url":  url,
        "description":  "",
        "listed_date":  None,
        "status":       "sold",
        "_photo_urls":  raw.get("images", [])
    }

# ─────────────────────────────────────────
# FETCH SOLD LISTINGS VIA APIFY
# ─────────────────────────────────────────
def fetch_sold_via_apify(urls: list) -> list:
    """Fetch sold listings for a batch of URLs via Apify."""
    # Step 1: Start the run
    run_response = requests.post(
        "https://api.apify.com/v2/acts/easyapi~domain-com-au-property-scraper/runs",
        json={
            "searchUrls": urls,
            "maxItems": 500
        },
        params={"token": APIFY_API_TOKEN},
        timeout=60
    )

    if run_response.status_code not in [200, 201]:
        print(f"  ✗ Apify run error {run_response.status_code}: {run_response.text}")
        return []

    run_data = run_response.json()
    run_id = run_data.get("data", {}).get("id")
    dataset_id = run_data.get("data", {}).get("defaultDatasetId")
    print(f"  → Run started: {run_id}")

    # Step 2: Poll until run finishes
    print(f"  → Waiting for run to complete...")
    for attempt in range(60):  # max 5 minutes
        time.sleep(15 if attempt == 0 else 5)

        status_response = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_API_TOKEN}
        )
        status = status_response.json().get("data", {}).get("status")
        print(f"    Status: {status} ({attempt+1}/60)")

        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"  ✗ Run {status}")
            return []

    # Step 3: Fetch results from dataset
    results_response = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": 500}
    )

    if results_response.status_code != 200:
        print(f"  ✗ Dataset fetch error {results_response.status_code}")
        return []

    return results_response.json()

# ─────────────────────────────────────────
# INSERT SOLD LISTING
# ─────────────────────────────────────────
def insert_sold_listing(listing: dict, photo_urls: list = None) -> str:
    """
    Insert a sold listing, skip if already exists.
    Returns the new listing's ID if inserted, None if skipped or on error.
    """
    try:
        existing = supabase.table("listings") \
            .select("id") \
            .eq("domain_id", listing["domain_id"]) \
            .execute()

        if existing.data:
            return None

        result = supabase.table("listings").insert(listing).execute()
        listing_id = result.data[0]["id"] if result.data else None

        # Store first 8 photo URLs for later vision scoring
        if listing_id and photo_urls:
            for url in photo_urls[:8]:
                try:
                    supabase.table("photos").insert({
                        "listing_id":       listing_id,
                        "url":              url,
                        "photo_base64":     None,
                        "room_type":        None,
                        "renovation_score": None,
                    }).execute()
                except Exception:
                    pass

        return listing_id

    except Exception as e:
        print(f"    ✗ Insert error: {e}")
        return None

# ─────────────────────────────────────────
# RUN BACKFILL
# ─────────────────────────────────────────
def run_backfill():
    """
    Run the full historical backfill.
    Processes suburbs in batches of 5 to stay within Apify limits.
    """
    print(f"Starting backfill for {len(BACKFILL_SUBURBS)} suburbs...\n")

    total_inserted = 0
    total_skipped = 0
    batch_size = 5  # Process 5 suburbs per Apify call

    # Split into batches
    batches = [
        BACKFILL_SUBURBS[i:i+batch_size]
        for i in range(0, len(BACKFILL_SUBURBS), batch_size)
    ]

    for batch_num, batch in enumerate(batches, 1):
        print(f"[Batch {batch_num}/{len(batches)}] Processing: {', '.join(s['name'] for s in batch)}")

        # Build URLs for this batch
        urls = [build_sold_url(s) for s in batch]
        for url in urls:
            print(f"  → {url}")

        # Fetch from Apify
        print(f"  → Fetching from Apify...")
        raw_results = fetch_sold_via_apify(urls)
        print(f"  → Got {len(raw_results)} raw results")

        # Process and insert
        batch_inserted = 0
        for raw in raw_results:
            # Match to suburb
            address_obj = raw.get("address", {})
            raw_suburb = address_obj.get("suburb", "").lower().replace(" ", "-")
            matched_suburb = next(
                (s for s in batch if s["name"] in raw_suburb or raw_suburb in s["name"]),
                batch[0]
            )

            listing = normalise_sold(raw, matched_suburb)

            # Skip if no price or invalid
            if not listing["domain_id"] or not listing["price"]:
                continue

            photo_urls = listing.pop("_photo_urls", [])

            if insert_sold_listing(listing, photo_urls):
                batch_inserted += 1
            else:
                total_skipped += 1

        total_inserted += batch_inserted
        print(f"  ✓ Inserted {batch_inserted} sold listings\n")

        # Small delay between batches
        time.sleep(2)

    print(f"{'='*50}")
    print(f"BACKFILL COMPLETE")
    print(f"{'='*50}")
    print(f"Total inserted: {total_inserted}")
    print(f"Total skipped:  {total_skipped}")
    print(f"\nNext step: run the suburb gap calculator")


# ─────────────────────────────────────────
# QUICK TEST (single suburb)
# ─────────────────────────────────────────
def test_single_suburb():
    """Test with just one suburb before running full backfill."""
    suburb = BACKFILL_SUBURBS[0]
    url = build_sold_url(suburb)
    print(f"Test URL: {url}\n")

    print(f"Fetching sold listings for {suburb['name']}...")
    results = fetch_sold_via_apify([url])
    print(f"Got {len(results)} results")

    if results:
        sample = normalise_sold(results[0], suburb)
        print(f"\nSample normalised listing:")
        for k, v in sample.items():
            if k != "_photo_urls":
                print(f"  {k}: {v}")
        print(f"  photos: {len(results[0].get('images', []))} available")


def run_backfill_regions():
    """
    Run backfill using broad region URLs to maximise results per Apify call.
    """
    regions = [
        {"name": "hobart-and-southern-region",          "state": "TAS"},
        {"name": "launceston-and-northern-region",      "state": "TAS"},
        {"name": "devonport-and-central-coast-region",  "state": "TAS"},
    ]

    urls = [
        f"https://www.domain.com.au/sold-listings/{r['name']}-tas/house/"
        f"?bedrooms=3-5&price=300000-750000&excludepricewithheld=1"
        for r in regions
    ]

    print("Running region-based backfill...")
    for url in urls:
        print(f"  → {url}")

    print(f"\nFetching from Apify (all 3 regions in one run)...")

    run_response = requests.post(
        "https://api.apify.com/v2/acts/easyapi~domain-com-au-property-scraper/runs",
        json={
            "searchUrls": urls,
            "maxItems": 200  # enough to catch recent sales without burning Apify credits
        },
        params={"token": APIFY_API_TOKEN},
        timeout=60
    )

    run_data = run_response.json()
    run_id = run_data.get("data", {}).get("id")
    dataset_id = run_data.get("data", {}).get("defaultDatasetId")
    print(f"Run started: {run_id}")

    print("Waiting for completion...")
    for attempt in range(240):  # up to 20 minutes
        time.sleep(15 if attempt == 0 else 5)
        status_response = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_API_TOKEN}
        )
        status = status_response.json().get("data", {}).get("status")
        print(f"  Status: {status} ({attempt+1}/120)")
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"✗ Run {status}")
            return

    results_response = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": 1000}
    )
    raw_results = results_response.json()
    print(f"\nGot {len(raw_results)} raw results")

    inserted = 0
    skipped = 0
    no_price = 0

    for raw in raw_results:
        address_obj = raw.get("address", {})
        suburb_name = address_obj.get("suburb", "").title()
        state = address_obj.get("state", "TAS")

        # Skip non-TAS results
        if state != "TAS":
            skipped += 1
            continue

        # Use a dummy suburb dict for normalisation
        suburb = {"name": suburb_name}
        listing = normalise_sold(raw, suburb)

        if not listing["domain_id"] or not listing["price"]:
            no_price += 1
            continue

        photo_urls = listing.pop("_photo_urls", [])

        if insert_sold_listing(listing, photo_urls):
            inserted += 1
        else:
            skipped += 1

    print(f"\n{'='*50}")
    print(f"REGION BACKFILL COMPLETE")
    print(f"{'='*50}")
    print(f"Total inserted: {inserted}")
    print(f"Skipped (dupe/non-TAS): {skipped}")
    print(f"Skipped (no price): {no_price}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test",    action="store_true", help="Test single suburb only")
    parser.add_argument("--run",     action="store_true", help="Run suburb-by-suburb backfill")
    parser.add_argument("--regions", action="store_true", help="Run region-based backfill (recommended)")
    args = parser.parse_args()

    if args.test:
        test_single_suburb()
    elif args.run:
        run_backfill()
    elif args.regions:
        run_backfill_regions()
    else:
        print("Usage:")
        print("  python3 jobs/backfill.py --test      # test single suburb")
        print("  python3 jobs/backfill.py --run       # suburb-by-suburb backfill")
        print("  python3 jobs/backfill.py --regions   # region-based backfill (recommended)")
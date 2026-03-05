# sources/domain.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import time
from datetime import datetime, timedelta
from config import (
    DOMAIN_CLIENT_ID, DOMAIN_CLIENT_SECRET,
    APIFY_API_TOKEN, FILTERS, ALL_SUBURBS, SOURCES
)
from db.client import insert_listing, listing_exists

# ─────────────────────────────────────────
# TOKEN MANAGER (Domain API only)
# ─────────────────────────────────────────
_token_cache = {
    "access_token": None,
    "expires_at": None
}

def get_domain_token() -> str:
    """Get a valid Domain OAuth token, refreshing if expired."""
    now = datetime.utcnow()
    if _token_cache["access_token"] and _token_cache["expires_at"] > now:
        return _token_cache["access_token"]

    response = requests.post(
        "https://auth.domain.com.au/v1/connect/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     DOMAIN_CLIENT_ID,
            "client_secret": DOMAIN_CLIENT_SECRET,
            "scope":         "api_listings_read api_properties_read"
        }
    )
    response.raise_for_status()
    data = response.json()

    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + timedelta(seconds=data["expires_in"] - 60)

    return _token_cache["access_token"]

# ─────────────────────────────────────────
# BUILD SEARCH URL (Apify)
# ─────────────────────────────────────────
def build_search_url(suburb: dict, state: str = "tas") -> str:
    """Build a Domain search URL for a suburb with filters applied."""
    name = suburb["name"].lower().replace(" ", "-")
    postcode = suburb["postcode"]

    base = f"https://www.domain.com.au/sale/{name}-{state}-{postcode}/house/"

    params = [
        f"bedrooms={FILTERS['min_bedrooms']}-{FILTERS['max_bedrooms']}",
        f"price={FILTERS['min_price']}-{FILTERS['max_price']}",
        "excludeunderoffer=1",
    ]

    return f"{base}?{'&'.join(params)}"

# ─────────────────────────────────────────
# FETCH VIA APIFY
# ─────────────────────────────────────────
def fetch_via_apify(suburb: dict) -> list:
    """Fetch listings for a suburb using Apify scraper."""
    search_url = build_search_url(suburb)
    print(f"  → Apify fetching: {suburb['name']} ({search_url})")

    # Step 1: Start the run
    run_response = requests.post(
        "https://api.apify.com/v2/acts/easyapi~domain-com-au-property-scraper/runs",
        json={
            "searchUrls": [search_url],
            "maxItems": 100
        },
        params={"token": APIFY_API_TOKEN},
        timeout=60
    )

    if run_response.status_code not in [200, 201]:
        print(f"  ✗ Apify error {run_response.status_code}: {run_response.text}")
        return []

    run_data = run_response.json()
    run_id = run_data.get("data", {}).get("id")
    dataset_id = run_data.get("data", {}).get("defaultDatasetId")
    print(f"  → Run started: {run_id}")

    # Step 2: Poll until complete
    for attempt in range(60):
        time.sleep(5)
        status_response = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_API_TOKEN}
        )
        status = status_response.json().get("data", {}).get("status")
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"  ✗ Run {status}")
            return []

    # Step 3: Fetch dataset results
    results_response = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": 100}
    )

    return results_response.json() if results_response.status_code == 200 else []

# ─────────────────────────────────────────
# FETCH VIA DOMAIN API
# ─────────────────────────────────────────
def fetch_via_domain_api(suburb: dict) -> list:
    """Fetch listings for a suburb using the official Domain API."""
    token = get_domain_token()
    headers = {"Authorization": f"Bearer {token}"}

    payload = {
        "listingType": "Sale",
        "propertyTypes": FILTERS["property_types"],
        "minBedrooms": FILTERS["min_bedrooms"],
        "maxBedrooms": FILTERS["max_bedrooms"],
        "minPrice": FILTERS["min_price"],
        "maxPrice": FILTERS["max_price"],
        "locations": [{
            "state": "TAS",
            "suburb": suburb["name"],
            "postCode": suburb["postcode"]
        }],
        "sort": {"sortKey": "dateListed", "direction": "Descending"},
        "pageSize": 100
    }

    response = requests.post(
        "https://api.domain.com.au/v1/listings/residential/_search",
        json=payload,
        headers=headers
    )

    if response.status_code != 200:
        print(f"  ✗ Domain API error {response.status_code} for {suburb['name']}")
        return []

    return response.json()

# ─────────────────────────────────────────
# NORMALISE LISTING
# ─────────────────────────────────────────
def normalise_apify(raw: dict, suburb: dict) -> dict:
    """Convert Apify result to our standard listing format."""
    # Extract numeric price from string like "Offers Over $450,000"
    price_str = raw.get("price", "0")
    price = 0
    if isinstance(price_str, str):
        digits = ''.join(filter(str.isdigit, price_str))
        price = int(digits) if digits else 0
    elif isinstance(price_str, (int, float)):
        price = int(price_str)

    # Address is a nested object
    address_obj = raw.get("address", {})
    full_address = f"{address_obj.get('street', '')} {address_obj.get('suburb', '')} {address_obj.get('state', '')} {address_obj.get('postcode', '')}".strip()

    # Features are nested
    features = raw.get("features", {})

    # Use URL as unique ID since there's no id field
    url = raw.get("url", "")
    domain_id = url.split("/")[-1] if url else ""

    return {
        "domain_id":    domain_id,
        "address":      full_address,
        "suburb":       address_obj.get("suburb", suburb["name"]).title(),
        "state":        address_obj.get("state", "TAS"),
        "price":        price,
        "bedrooms":     features.get("beds", None),
        "bathrooms":    features.get("baths", None),
        "land_size":    int(float(features.get("landSize") or 0)) or None,
        "listing_url":  url,
        "description":  "",   # Apify doesn't return description
        "listed_date":  None, # Apify doesn't return listed date
        "status":       "active",
        "_photo_urls":  raw.get("images", [])
    }

def normalise_domain_api(raw: dict, suburb: dict) -> dict:
    """Convert Domain API result to our standard listing format."""
    listing = raw.get("listing", {})
    price_details = listing.get("priceDetails", {})
    property_details = listing.get("propertyDetails", {})

    return {
        "domain_id":    str(listing.get("id", "")),
        "address":      property_details.get("displayableAddress", ""),
        "suburb":       suburb["name"],
        "state":        "TAS",
        "price":        price_details.get("price", 0),
        "bedrooms":     property_details.get("bedrooms", None),
        "bathrooms":    property_details.get("bathrooms", None),
        "land_size":    property_details.get("landArea", None),
        "listing_url":  listing.get("seoUrl", ""),
        "description":  listing.get("description", ""),
        "listed_date":  listing.get("dateListed", None),
        "status":       "active",
        "_photo_urls":  [m.get("url") for m in listing.get("media", []) if m.get("category") == "Image"]
    }

# ─────────────────────────────────────────
# REGIONAL URLS FOR ACTIVE LISTINGS
# ─────────────────────────────────────────
ACTIVE_REGION_URLS = [
    f"https://www.domain.com.au/sale/hobart-and-southern-region-tas/house/?bedrooms={FILTERS['min_bedrooms']}-{FILTERS['max_bedrooms']}&price={FILTERS['min_price']}-{FILTERS['max_price']}&excludeunderoffer=1",
    f"https://www.domain.com.au/sale/launceston-and-northern-region-tas/house/?bedrooms={FILTERS['min_bedrooms']}-{FILTERS['max_bedrooms']}&price={FILTERS['min_price']}-{FILTERS['max_price']}&excludeunderoffer=1",
    f"https://www.domain.com.au/sale/devonport-and-central-coast-region-tas/house/?bedrooms={FILTERS['min_bedrooms']}-{FILTERS['max_bedrooms']}&price={FILTERS['min_price']}-{FILTERS['max_price']}&excludeunderoffer=1",
]

def fetch_all_via_apify() -> list:
    """Fetch all active listings across Tasmania in a single Apify run."""
    print(f"  → Starting single Apify run for all 3 regions...")
    for url in ACTIVE_REGION_URLS:
        print(f"    {url}")

    run_response = requests.post(
        "https://api.apify.com/v2/acts/easyapi~domain-com-au-property-scraper/runs",
        json={
            "searchUrls": ACTIVE_REGION_URLS,
            "maxItems":   500
        },
        params={"token": APIFY_API_TOKEN},
        timeout=60
    )

    if run_response.status_code not in [200, 201]:
        print(f"  ✗ Apify error {run_response.status_code}: {run_response.text}")
        return []

    run_data   = run_response.json()
    run_id     = run_data.get("data", {}).get("id")
    dataset_id = run_data.get("data", {}).get("defaultDatasetId")
    print(f"  → Run started: {run_id}")

    # Poll until complete
    for attempt in range(120):
        time.sleep(5)
        status_response = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_API_TOKEN}
        )
        status = status_response.json().get("data", {}).get("status")
        print(f"    Status: {status} ({attempt+1}/120)")
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"  ✗ Run {status}")
            return []

    results_response = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": 500}
    )
    return results_response.json() if results_response.status_code == 200 else []


def fetch_new_listings() -> list:
    """
    Fetch new listings across all target regions in a single Apify call.
    Skips listings already in the database.
    Returns list of new listings with photo URLs attached.
    """
    all_new = []

    try:
        if SOURCES["use_domain_api"]:
            # Domain API still loops per suburb
            total = len(ALL_SUBURBS)
            for i, suburb in enumerate(ALL_SUBURBS, 1):
                print(f"[{i}/{total}] Fetching {suburb['name']}...")
                raw_listings = fetch_via_domain_api(suburb)
                normalised = [normalise_domain_api(r, suburb) for r in raw_listings]
                for listing in normalised:
                    if listing["price"] and not (FILTERS["min_price"] <= listing["price"] <= FILTERS["max_price"]):
                        continue
                    if listing_exists(listing["domain_id"]):
                        continue
                    photo_urls = listing.pop("_photo_urls", [])
                    stored = insert_listing(listing)
                    if stored:
                        stored_listing = stored[0] if isinstance(stored, list) else stored
                        stored_listing["_photo_urls"] = photo_urls
                        all_new.append(stored_listing)
        else:
            # Single batched Apify call for all regions
            raw_listings = fetch_all_via_apify()
            print(f"\n  → Got {len(raw_listings)} raw results")

            new_count = 0
            skip_count = 0
            for raw in raw_listings:
                # Filter to TAS only
                address_obj = raw.get("address", {})
                if address_obj.get("state", "").upper() != "TAS":
                    skip_count += 1
                    continue

                suburb_name = address_obj.get("suburb", "").title()
                suburb = {"name": suburb_name}
                listing = normalise_apify(raw, suburb)

                if listing["price"] and not (FILTERS["min_price"] <= listing["price"] <= FILTERS["max_price"]):
                    skip_count += 1
                    continue

                if listing_exists(listing["domain_id"]):
                    skip_count += 1
                    continue

                photo_urls = listing.pop("_photo_urls", [])
                stored = insert_listing(listing)
                if stored:
                    stored_listing = stored[0] if isinstance(stored, list) else stored
                    stored_listing["_photo_urls"] = photo_urls
                    all_new.append(stored_listing)
                    new_count += 1

            print(f"  ✓ {new_count} new listings | {skip_count} skipped (existing/filtered)")

    except Exception as e:
        print(f"✗ Error fetching listings: {e}")

    print(f"\n✅ Total new listings: {len(all_new)}")
    return all_new

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing fetch for first suburb only...")
    suburb = ALL_SUBURBS[0]
    url = build_search_url(suburb)
    print(f"Search URL: {url}")

    if SOURCES["use_apify"] and APIFY_API_TOKEN:
        results = fetch_via_apify(suburb)
        print(f"Raw results returned: {len(results)}")
        if results:
            print(f"First result keys: {list(results[0].keys())}")
    else:
        print("No API token set yet — add APIFY_API_TOKEN to .env to test")
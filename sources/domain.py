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
from db.client import insert_listing, listing_exists, supabase

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

    import re
    prop_type = "unit" if re.match(r"^\d+/", full_address) else "house"

    return {
        "domain_id":     domain_id,
        "address":       full_address,
        "suburb":        address_obj.get("suburb", suburb["name"]).title(),
        "state":         address_obj.get("state", "TAS"),
        "price":         price,
        "bedrooms":      features.get("beds", None),
        "bathrooms":     features.get("baths", None),
        "land_size":     int(float(features.get("landSize") or 0)) or None,
        "listing_url":   url,
        "description":   "",   # Apify doesn't return description
        "listed_date":   None, # Apify doesn't return listed date
        "status":        "active",
        "property_type": prop_type,
        "_photo_urls":   raw.get("images", [])
    }

def normalise_domain_api(raw: dict, suburb: dict) -> dict:
    """Convert Domain API result to our standard listing format."""
    listing = raw.get("listing", {})
    price_details = listing.get("priceDetails", {})
    property_details = listing.get("propertyDetails", {})

    address = property_details.get("displayableAddress", "")
    import re
    prop_type = "unit" if re.match(r"^\d+/", address) else "house"

    return {
        "domain_id":     str(listing.get("id", "")),
        "address":       address,
        "suburb":        suburb["name"],
        "state":         "TAS",
        "price":         price_details.get("price", 0),
        "bedrooms":      property_details.get("bedrooms", None),
        "bathrooms":     property_details.get("bathrooms", None),
        "land_size":     property_details.get("landArea", None),
        "listing_url":   listing.get("seoUrl", ""),
        "description":   listing.get("description", ""),
        "listed_date":   listing.get("dateListed", None),
        "status":        "active",
        "property_type": prop_type,
        "_photo_urls":   [m.get("url") for m in listing.get("media", []) if m.get("category") == "Image"]
    }

VIC_PRICE_MIN = 300000
VIC_PRICE_MAX = 900000

# ─────────────────────────────────────────
# NSW SUBURB URLS (top-gap suburbs only)
# ─────────────────────────────────────────
NSW_DOMAIN_TYPES = {
    "house": "house",
    "unit":  "apartment-unit-flat",
}

def get_nsw_active_urls(min_gap_dollar: int = 150000) -> tuple:
    """
    Query suburb_gaps for NSW suburbs (houses and units) with gap_dollar >= min_gap_dollar.
    Returns (urls, suburb_property_type_map) where map is {suburb_name: property_type}.
    Postcodes are looked up from listings table (required by Domain URLs).
    Units use bedrooms=2-5; houses use bedrooms=3-5.
    """
    try:
        result = supabase.table("suburb_gaps") \
            .select("suburb, property_type") \
            .eq("state", "NSW") \
            .gte("gap_dollar", min_gap_dollar) \
            .order("gap_dollar", desc=True) \
            .execute()

        urls = []
        suburb_type_map = {}  # suburb_name -> property_type for filtering after fetch
        houses = 0
        units  = 0
        for row in (result.data or []):
            suburb_name = row["suburb"]
            prop_type   = row["property_type"]
            domain_type = NSW_DOMAIN_TYPES.get(prop_type)
            if not domain_type:
                continue

            # Look up postcode from listings table
            pc_result = supabase.table("listings") \
                .select("postcode") \
                .eq("suburb", suburb_name) \
                .eq("state", "NSW") \
                .not_.is_("postcode", "null") \
                .limit(1) \
                .execute()
            postcode = pc_result.data[0]["postcode"] if pc_result.data else None
            if not postcode:
                continue

            suburb_slug = suburb_name.lower().replace(" ", "-")
            min_beds = FILTERS['min_bedrooms'] if prop_type == "house" else 2
            url = (
                f"https://www.domain.com.au/sale/{suburb_slug}-nsw-{postcode}/{domain_type}/"
                f"?bedrooms={min_beds}-{FILTERS['max_bedrooms']}"
                f"&price={FILTERS['min_price']}-{FILTERS['max_price']}"
                f"&excludeunderoffer=1"
            )
            urls.append(url)
            suburb_type_map[suburb_name] = prop_type
            if prop_type == "house":
                houses += 1
            else:
                units += 1

        print(f"  → {len(urls)} NSW suburb/type combos with gap >= ${min_gap_dollar:,} ({houses} house, {units} unit)")
        return urls, suburb_type_map
    except Exception as e:
        print(f"  ✗ Error fetching NSW suburb gaps: {e}")
        return [], {}


# ─────────────────────────────────────────
# VIC SUBURB URLS (top-gap suburbs only)
# ─────────────────────────────────────────
def get_vic_active_urls(min_gap_dollar: int = 150_000) -> list:
    """
    Query suburb_gaps for VIC house suburbs with gap_dollar >= min_gap_dollar.
    Returns list of Domain sale search URLs using the wider VIC price range ($300k-$900k).
    Postcodes are looked up from the listings table.
    """
    try:
        result = supabase.table("suburb_gaps") \
            .select("suburb") \
            .eq("state", "VIC") \
            .eq("property_type", "house") \
            .gte("gap_dollar", min_gap_dollar) \
            .order("gap_dollar", desc=True) \
            .execute()

        urls = []
        for row in (result.data or []):
            suburb_name = row["suburb"]

            pc_result = supabase.table("listings") \
                .select("postcode") \
                .eq("suburb", suburb_name) \
                .eq("state", "VIC") \
                .not_.is_("postcode", "null") \
                .limit(1) \
                .execute()
            postcode = pc_result.data[0]["postcode"] if pc_result.data else None
            if not postcode:
                continue

            suburb_slug = suburb_name.lower().replace(" ", "-")
            url = (
                f"https://www.domain.com.au/sale/{suburb_slug}-vic-{postcode}/house/"
                f"?bedrooms={FILTERS['min_bedrooms']}-{FILTERS['max_bedrooms']}"
                f"&price={VIC_PRICE_MIN}-{VIC_PRICE_MAX}"
                f"&excludeunderoffer=1"
            )
            urls.append(url)

        print(f"  → {len(urls)} VIC suburbs with gap >= ${min_gap_dollar:,}")
        return urls
    except Exception as e:
        print(f"  ✗ Error fetching VIC suburb gaps: {e}")
        return []


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
    for attempt in range(800):
        time.sleep(15 if attempt == 0 else 5)
        try:
            status_response = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                params={"token": APIFY_API_TOKEN}
            )
            status = status_response.json().get("data", {}).get("status")
        except Exception:
            print(f"    Status poll error — retrying ({attempt+1}/800)")
            continue
        print(f"    Status: {status} ({attempt+1}/800)")
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


def _apify_run(search_urls: list, max_items: int = 100) -> list:
    """Start an Apify run, poll until complete, return dataset items."""
    run_response = requests.post(
        "https://api.apify.com/v2/acts/easyapi~domain-com-au-property-scraper/runs",
        json={"searchUrls": search_urls, "maxItems": max_items},
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

    for attempt in range(800):
        time.sleep(15 if attempt == 0 else 5)
        try:
            status_response = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                params={"token": APIFY_API_TOKEN}
            )
            status = status_response.json().get("data", {}).get("status")
        except Exception:
            print(f"    Status poll error — retrying ({attempt+1}/800)")
            continue
        print(f"    Status: {status} ({attempt+1}/800)")
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"  ✗ Run {status}")
            return []

    results_response = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN, "limit": max_items}
    )
    return results_response.json() if results_response.status_code == 200 else []


def fetch_new_listings(gap_suburbs: set = None) -> list:
    """
    Fetch new listings for suburbs that have gap data.
    If gap_suburbs is provided (set of suburb names with gap > threshold),
    only those suburbs are scraped — avoiding wasted effort on suburbs we'd
    filter out during processing anyway.
    Skips listings already in the database.
    Returns list of new listings with photo URLs attached.
    """
    all_new = []

    try:
        if SOURCES["use_domain_api"]:
            # Domain API: loop only over suburbs with gap data
            target_suburbs = [
                s for s in ALL_SUBURBS
                if gap_suburbs is None or s["name"].title() in gap_suburbs
            ]
            total = len(target_suburbs)
            print(f"  → Domain API: fetching {total} gap suburbs")
            for i, suburb in enumerate(target_suburbs, 1):
                print(f"  [{i}/{total}] {suburb['name']}...")
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
                time.sleep(0.5)  # gentle rate limiting for Domain API
        else:
            # Apify: TAS regional URLs + NSW top-gap suburb URLs
            if gap_suburbs:
                target_suburbs = [
                    s for s in ALL_SUBURBS
                    if s["name"].title() in gap_suburbs
                ]
                tas_urls = [build_search_url(s) for s in target_suburbs]
                print(f"  → Apify: scraping {len(tas_urls)} TAS gap suburbs")
            else:
                tas_urls = ACTIVE_REGION_URLS
                print(f"  → Apify: scraping 3 TAS regional URLs")

            nsw_urls, nsw_suburb_type_map = get_nsw_active_urls(min_gap_dollar=150000)
            vic_urls = get_vic_active_urls(min_gap_dollar=150_000)
            search_urls = tas_urls + nsw_urls + vic_urls
            max_items = max(500, len(search_urls) * 30)
            print(f"  → Total URLs: {len(search_urls)} (TAS: {len(tas_urls)}, NSW: {len(nsw_urls)}, VIC: {len(vic_urls)})")

            raw_listings = _apify_run(search_urls, max_items=max_items)
            print(f"\n  → Got {len(raw_listings)} raw results")

            new_count = 0
            skip_count = 0
            for raw in raw_listings:
                address_obj = raw.get("address", {})
                state = address_obj.get("state", "").upper()
                if state not in ("TAS", "NSW", "VIC"):
                    skip_count += 1
                    continue

                suburb_name = address_obj.get("suburb", "").title()

                suburb = {"name": suburb_name}
                listing = normalise_apify(raw, suburb)

                # Skip price-withheld listings
                if not listing["price"]:
                    skip_count += 1
                    continue

                # VIC has wider price range ($300k-$900k); TAS/NSW use standard filter
                if state == "VIC":
                    if not (VIC_PRICE_MIN <= listing["price"] <= VIC_PRICE_MAX):
                        skip_count += 1
                        continue
                else:
                    if not (FILTERS["min_price"] <= listing["price"] <= FILTERS["max_price"]):
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
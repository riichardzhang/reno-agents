# db/client.py
import os
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_ANON_KEY

# ─────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────
def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

supabase = get_client()

# ─────────────────────────────────────────
# LISTINGS
# ─────────────────────────────────────────
def insert_listing(listing: dict) -> dict:
    """Insert a new listing, ignore if domain_id already exists."""
    result = supabase.table("listings").upsert(
        listing,
        on_conflict="domain_id"
    ).execute()
    return result.data

def get_listing(listing_id: str) -> dict:
    """Fetch a single listing by ID."""
    result = supabase.table("listings") \
        .select("*") \
        .eq("id", listing_id) \
        .single() \
        .execute()
    return result.data

def get_unevaluated_listings() -> list:
    """Fetch all active listings that haven't been evaluated yet."""
    result = supabase.table("listings") \
        .select("*") \
        .is_("evaluated_at", "null") \
        .eq("status", "active") \
        .execute()
    return result.data

def get_unalerted_listings() -> list:
    """Fetch listings that passed feasibility but haven't been alerted yet."""
    result = supabase.table("listings") \
        .select("*") \
        .eq("alerted", False) \
        .in_("verdict", ["GO", "WATCH"]) \
        .execute()
    return result.data

def update_listing(listing_id: str, updates: dict) -> dict:
    """Update fields on an existing listing."""
    result = supabase.table("listings") \
        .update(updates) \
        .eq("id", listing_id) \
        .execute()
    return result.data

def mark_listing_alerted(listing_id: str) -> None:
    """Mark a listing as alerted so we don't send duplicate alerts."""
    supabase.table("listings") \
        .update({"alerted": True}) \
        .eq("id", listing_id) \
        .execute()

def listing_exists(domain_id: str) -> bool:
    """Check if a listing has already been stored."""
    result = supabase.table("listings") \
        .select("id") \
        .eq("domain_id", domain_id) \
        .execute()
    return len(result.data) > 0

def get_sold_listings_for_suburb(suburb: str, state: str) -> list:
    """Fetch all sold listings for a suburb for gap analysis."""
    result = supabase.table("listings") \
        .select("*") \
        .eq("suburb", suburb) \
        .eq("state", state) \
        .eq("status", "sold") \
        .not_.is_("classification", "null") \
        .execute()
    return result.data

# ─────────────────────────────────────────
# PHOTOS
# ─────────────────────────────────────────
def insert_photo(photo: dict) -> dict:
    """Store a downloaded photo."""
    result = supabase.table("photos") \
        .insert(photo) \
        .execute()
    return result.data

def get_photos_for_listing(listing_id: str) -> list:
    """Fetch all photos for a listing."""
    result = supabase.table("photos") \
        .select("*") \
        .eq("listing_id", listing_id) \
        .execute()
    return result.data

def get_target_room_photos(listing_id: str) -> list:
    """Fetch only kitchen and bathroom photos for a listing."""
    result = supabase.table("photos") \
        .select("*") \
        .eq("listing_id", listing_id) \
        .in_("room_type", ["kitchen", "bathroom"]) \
        .execute()
    return result.data

# ─────────────────────────────────────────
# SUBURB GAPS
# ─────────────────────────────────────────
def upsert_suburb_gap(gap: dict) -> dict:
    """Insert or update suburb gap data."""
    result = supabase.table("suburb_gaps") \
        .upsert(gap, on_conflict="suburb,state") \
        .execute()
    return result.data

def get_suburb_gap(suburb: str, state: str) -> dict:
    """Fetch gap data for a specific suburb."""
    try:
        result = supabase.table("suburb_gaps") \
            .select("*") \
            .eq("suburb", suburb) \
            .eq("state", state) \
            .single() \
            .execute()
        return result.data if result.data else None
    except Exception:
        return None

def get_all_suburb_gaps(state: str = None) -> list:
    """Fetch all suburb gaps, optionally filtered by state."""
    query = supabase.table("suburb_gaps").select("*")
    if state:
        query = query.eq("state", state)
    result = query.order("gap_percent", desc=True).execute()
    return result.data

# ─────────────────────────────────────────
# MARKET STATS
# ─────────────────────────────────────────
def upsert_market_stats(stats: dict) -> dict:
    """Insert or update market stats for a suburb."""
    result = supabase.table("market_stats") \
        .upsert(stats, on_conflict="suburb,state") \
        .execute()
    return result.data

def get_market_stats(suburb: str, state: str) -> dict:
    """Fetch market stats for a suburb."""
    result = supabase.table("market_stats") \
        .select("*") \
        .eq("suburb", suburb) \
        .eq("state", state) \
        .single() \
        .execute()
    return result.data if result.data else None
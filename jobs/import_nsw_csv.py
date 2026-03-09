# jobs/import_nsw_csv.py
"""
One-time import of NSW government sold sales CSV into the listings table.

Usage:
    python3 jobs/import_nsw_csv.py <path_to_csv>

The CSV is the filtered output from filter_nsw_sales.py:
    nsw_sales_filtered.csv

Inserts each row as a sold listing with state=NSW.
Skips rows already in the DB (idempotent — safe to re-run).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
import pandas as pd
from datetime import datetime, timezone
from db.client import supabase

BATCH_SIZE = 100  # rows per Supabase insert


def make_domain_id(row) -> str:
    """Generate a stable unique ID from address fields since CSV has no Domain ID."""
    key = f"nsw_csv_{row['Property house number']}_{row['Property street name']}_{row['Property locality']}_{row['Property post code']}_{row['Settlement date']}"
    return "nsw_" + hashlib.md5(key.encode()).hexdigest()[:16]


def normalise_row(row) -> dict:
    unit    = str(row.get("Property unit number", "") or "").strip()
    house   = str(row.get("Property house number", "") or "").strip()
    street  = str(row.get("Property street name", "") or "").strip()
    locality = str(row.get("Property locality", "") or "").strip().title()
    postcode = str(int(row["Property post code"])) if pd.notna(row.get("Property post code")) else ""

    address_parts = [f"{unit}/{house}" if unit else house, street, locality, f"NSW {postcode}"]
    address = " ".join(p for p in address_parts if p).strip()

    area = float(row["Area"]) if pd.notna(row.get("Area")) else None

    settlement = row.get("Settlement date")
    listed_date = str(settlement)[:10] if pd.notna(settlement) else None

    return {
        "domain_id":     make_domain_id(row),
        "address":       address,
        "suburb":        locality,
        "state":         "NSW",
        "price":         int(row["Purchase price"]),
        "bedrooms":      None,
        "bathrooms":     None,
        "land_size":     int(area) if area else None,
        "listing_url":   None,
        "description":   "",
        "listed_date":   listed_date,
        "status":        "sold",
        "property_type": "unit" if unit else "house",
    }


def get_existing_ids(domain_ids: list) -> set:
    """Return set of domain_ids already in the DB."""
    result = supabase.table("listings") \
        .select("domain_id") \
        .in_("domain_id", domain_ids) \
        .execute()
    return {r["domain_id"] for r in (result.data or [])}


def run(csv_path: str):
    print(f"\nLoading {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  {len(df):,} rows loaded")

    # Normalise all rows
    print("  Normalising rows...")
    rows = [normalise_row(row) for _, row in df.iterrows()]

    # Process in batches
    inserted = 0
    skipped  = 0
    errors   = 0

    batches = [rows[i:i+BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    print(f"  Processing {len(batches)} batches of {BATCH_SIZE}...\n")

    for i, batch in enumerate(batches, 1):
        try:
            result = supabase.table("listings").upsert(
                batch, on_conflict="domain_id", ignore_duplicates=True
            ).execute()
            n_inserted = len(result.data) if result.data else 0
            inserted += n_inserted
            skipped  += len(batch) - n_inserted
        except Exception as e:
            print(f"  ✗ Batch {i} error: {e}")
            errors += len(batch)

        if i % 10 == 0 or i == len(batches):
            print(f"  Batch {i}/{len(batches)} — inserted: {inserted:,}  skipped: {skipped:,}  errors: {errors}")

    print(f"\n{'='*50}")
    print(f"IMPORT COMPLETE")
    print(f"{'='*50}")
    print(f"  Inserted: {inserted:,}")
    print(f"  Skipped (already in DB): {skipped:,}")
    print(f"  Errors: {errors}")
    print(f"\nNext step: run gap analysis")
    print(f"  python3 -m analysis.suburb_gaps --all\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 jobs/import_nsw_csv.py <path_to_nsw_sales_filtered.csv>")
        sys.exit(1)
    run(sys.argv[1])

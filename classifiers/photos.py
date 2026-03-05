# classifiers/photos.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import base64
import time
from config import PHOTOS, ANTHROPIC_API_KEY, MODELS
from db.client import insert_photo, get_photos_for_listing
import anthropic

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────
# DOWNLOAD PHOTO
# ─────────────────────────────────────────
def download_photo(url: str):
    """Download a photo from URL and return as base64 string."""
    try:
        response = requests.get(
            url,
            headers={
                "Referer": "https://www.domain.com.au",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            },
            timeout=10
        )
        if response.status_code == 200:
            return base64.b64encode(response.content).decode("utf-8")
        else:
            print(f"    ✗ Failed to download photo: {response.status_code}")
            return None
    except Exception as e:
        print(f"    ✗ Photo download error: {e}")
        return None

# ─────────────────────────────────────────
# IDENTIFY ROOM TYPE
# ─────────────────────────────────────────
def identify_room_from_url(url: str, index: int):
    """
    Try to identify room type from URL keywords or position heuristic.
    Returns room type string or None if can't determine cheaply.
    """
    url_lower = url.lower()

    # Method 1: Check URL keywords
    for room, keywords in PHOTOS["keywords"].items():
        if any(kw in url_lower for kw in keywords):
            return room

    # Method 2: Position heuristic
    for room, positions in PHOTOS["position_heuristic"].items():
        if index in positions:
            return room

    return None

def identify_room_via_claude(photo_base64: str) -> str:
    """
    Use Claude Haiku to identify room type from photo.
    Only called when URL/position heuristics fail.
    """
    try:
        response = client.messages.create(
            model=MODELS["classification"],
            max_tokens=50,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": photo_base64
                        }
                    },
                    {
                        "type": "text",
                        "text": """What room is shown in this property photo?
Reply with exactly one word from this list only: kitchen, bathroom, bedroom, living, dining, laundry, garage, exterior, other
Reply with one word only, nothing else."""
                    }
                ]
            }]
        )
        room = response.content[0].text.strip().lower()
        # Validate response is in our expected list
        valid_rooms = ["kitchen", "bathroom", "bedroom", "living", "dining", "laundry", "garage", "exterior", "other"]
        return room if room in valid_rooms else "other"

    except Exception as e:
        print(f"    ✗ Claude room ID error: {e}")
        return "other"

# ─────────────────────────────────────────
# PROCESS LISTING PHOTOS
# ─────────────────────────────────────────
def process_listing_photos(listing_id: str, photo_urls: list) -> dict:
    """
    Download and classify photos for a listing.
    Returns dict with kitchen and bathroom photo base64 data.

    Strategy:
    1. Try URL keywords (free)
    2. Try position heuristic (free)
    3. Fall back to Claude Haiku identification (~$0.01)
    """
    if not photo_urls:
        print(f"  ✗ No photos available for listing {listing_id}")
        return {}

    # Limit to first N photos
    urls_to_process = photo_urls[:PHOTOS["max_photos_to_download"]]

    found_rooms = {}         # room_type -> {url, base64}
    needs_claude_id = []     # (index, url, base64) that need Claude to identify

    print(f"  → Processing {len(urls_to_process)} photos...")

    for i, url in enumerate(urls_to_process):
        # Skip if we already have both target rooms
        if all(r in found_rooms for r in PHOTOS["target_rooms"]):
            break

        # Try cheap identification first
        room_type = identify_room_from_url(url, i)

        if room_type in PHOTOS["target_rooms"] and room_type not in found_rooms:
            # Download this photo
            photo_b64 = download_photo(url)
            if photo_b64:
                found_rooms[room_type] = {"url": url, "base64": photo_b64}
                print(f"    ✓ Found {room_type} via heuristic (photo {i+1})")

                # Store in database
                insert_photo({
                    "listing_id":       listing_id,
                    "url":              url,
                    "photo_base64":     photo_b64,
                    "room_type":        room_type,
                    "renovation_score": None
                })
        elif room_type is None:
            # Queue for Claude identification if we still need rooms
            photo_b64 = download_photo(url)
            if photo_b64:
                needs_claude_id.append((i, url, photo_b64))

        time.sleep(0.1)  # small delay between downloads

    # Use Claude to identify remaining photos if we still need target rooms
    missing_rooms = [r for r in PHOTOS["target_rooms"] if r not in found_rooms]

    if missing_rooms and needs_claude_id:
        print(f"  → Using Claude to identify {len(needs_claude_id)} unidentified photos...")

        for i, url, photo_b64 in needs_claude_id:
            if not missing_rooms:
                break

            room_type = identify_room_via_claude(photo_b64)
            print(f"    → Claude identified photo {i+1} as: {room_type}")

            if room_type in missing_rooms:
                found_rooms[room_type] = {"url": url, "base64": photo_b64}
                missing_rooms.remove(room_type)

                # Store in database
                insert_photo({
                    "listing_id":       listing_id,
                    "url":              url,
                    "photo_base64":     photo_b64,
                    "room_type":        room_type,
                    "renovation_score": None
                })

            time.sleep(0.2)

    # Report what we found
    for room in PHOTOS["target_rooms"]:
        if room in found_rooms:
            print(f"  ✓ {room.capitalize()} photo ready")
        else:
            print(f"  ⚠ No {room} photo found for listing {listing_id}")

    return found_rooms


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    # Test with a real photo URL from the Apify result we saw
    test_urls = [
        "https://rimh2.domainstatic.com.au/RKmU05Wp1MS6MweuZ0ZtS74slZQ=/660x440/filters:format(jpeg):quality(80)/2020112251_6_1_250707_050823-w1200-h801",
        "https://rimh2.domainstatic.com.au/M6Q7kVXfDdRtN70mdZhGl_G5LOw=/660x440/filters:format(jpeg):quality(80)/2020112251_2_1_250707_050822-w1200-h800",
        "https://rimh2.domainstatic.com.au/JOyE2vtf8YV62unMEFDguQPOJjQ=/660x440/filters:format(jpeg):quality(80)/2020112251_3_1_250707_050822-w1200-h801",
        "https://rimh2.domainstatic.com.au/Yr0m4NaBrRvfeszfFf6MCZDosqQ=/660x440/filters:format(jpeg):quality(80)/2020112251_4_1_250707_050823-w1200-h801",
        "https://rimh2.domainstatic.com.au/7zo5thEVkvWZWgmBjIJnROdBrU8=/660x440/filters:format(jpeg):quality(80)/2020112251_5_1_250707_050823-w1200-h801",
        "https://rimh2.domainstatic.com.au/rzlalYAU8O_kJz0hJZ_RAVkDA7U=/660x440/filters:format(jpeg):quality(80)/2020112251_7_1_250707_050823-w1200-h801",
        "https://rimh2.domainstatic.com.au/l2469HBqkmb1ccNl56Klee4avkY=/660x440/filters:format(jpeg):quality(80)/2020112251_8_1_250707_050823-w1200-h801",
        "https://rimh2.domainstatic.com.au/9w2WcvTuLImYycXdadtGv1oLN_U=/660x440/filters:format(jpeg):quality(80)/2020112251_9_1_250707_050823-w1200-h801",
    ]

    print("Testing photo processor with real Domain photos...")
    print("(No listing_id needed for test — skipping DB storage)\n")

    # Test download
    print("Testing photo download...")
    b64 = download_photo(test_urls[0])
    if b64:
        print(f"✓ Download works — got {len(b64)} base64 chars")
    else:
        print("✗ Download failed")

    # Test Claude room identification
    if b64:
        print("\nTesting Claude room identification...")
        room = identify_room_via_claude(b64)
        print(f"✓ Claude identified room as: {room}")
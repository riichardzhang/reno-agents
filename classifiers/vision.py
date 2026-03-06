# classifiers/vision.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import anthropic
from config import ANTHROPIC_API_KEY, MODELS, RENO_THRESHOLDS
from db.client import get_target_room_photos, update_listing, supabase

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────
# SCORE A SINGLE ROOM
# ─────────────────────────────────────────
def score_room(photo_base64: str, room_type: str) -> dict:
    """
    Score a room photo for renovation condition.
    Returns score 1-10 and flags.
    """
    room_guidance = {
        "kitchen": """
            Look at: benchtops, cabinetry, appliances, flooring, splashback, fixtures
            1-3: Very outdated — laminate benches, old cabinetry, dated appliances, worn floors
            4-6: Mixed — some updates but still partially dated
            7-8: Modern — stone/timber benches, new cabinetry, modern appliances
            9-10: Fully renovated — high-end finishes, nothing to do
        """,
        "bathroom": """
            Look at: tiles, vanity, toilet, shower/bath, tapware, flooring
            1-3: Very outdated — old tiles, dated vanity, worn fixtures, poor condition
            4-6: Mixed — some updates but still partially dated
            7-8: Modern — fresh tiles, new vanity, updated fixtures
            9-10: Fully renovated — high-end finishes, nothing to do
        """
    }

    prompt = f"""You are a property renovation expert assessing a {room_type} photo.

Score this {room_type} on renovation condition from 1 to 10:
{room_guidance.get(room_type, "")}

Also identify any red flags (water damage, mould, structural issues, severe damage).

Respond in JSON only, no other text:
{{
    "score": <integer 1-10>,
    "condition": "<very outdated|outdated|mixed|modern|fully renovated>",
    "key_observations": ["<observation 1>", "<observation 2>"],
    "red_flags": ["<flag 1>"] or []
}}"""

    try:
        response = client.messages.create(
            model=MODELS["classification"],
            max_tokens=300,
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
                        "text": prompt
                    }
                ]
            }]
        )

        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)

        # Validate score is in range
        result["score"] = max(1, min(10, int(result["score"])))
        return result

    except json.JSONDecodeError as e:
        print(f"    ✗ JSON parse error for {room_type}: {e}")
        return {"score": 5, "condition": "unknown", "key_observations": [], "red_flags": []}
    except Exception as e:
        print(f"    ✗ Claude vision error for {room_type}: {e}")
        return {"score": 5, "condition": "unknown", "key_observations": [], "red_flags": []}


# ─────────────────────────────────────────
# CLASSIFY LISTING FROM SCORES
# ─────────────────────────────────────────
def classify_from_scores(scores: dict) -> str:
    """
    Classify overall renovation status from room scores.
    Returns: renovated / unrenovated / partial
    """
    if not scores:
        return "uncertain"

    avg = sum(scores.values()) / len(scores)

    if avg <= RENO_THRESHOLDS["unrenovated"]:
        return "unrenovated"
    elif avg >= RENO_THRESHOLDS["renovated"]:
        return "renovated"
    else:
        return "partial"


# ─────────────────────────────────────────
# SCORE ALL ROOMS FOR A LISTING
# ─────────────────────────────────────────
def score_listing_renovation(listing_id: str) -> dict:
    """
    Score all target room photos for a listing.
    Updates listing classification in database.
    Returns full scoring result.
    """
    photos = get_target_room_photos(listing_id)

    if not photos:
        print(f"  ✗ No target room photos found for listing {listing_id}")
        return {}

    room_scores = {}
    room_details = {}
    all_red_flags = []

    for photo in photos:
        room_type = photo["room_type"]
        photo_b64 = photo["photo_base64"]

        if not photo_b64:
            print(f"  ✗ No base64 data for {room_type} photo")
            continue

        print(f"  → Scoring {room_type}...")
        result = score_room(photo_b64, room_type)

        score = result["score"]
        room_scores[room_type] = score
        room_details[room_type] = result
        all_red_flags.extend(result.get("red_flags", []))

        print(f"    ✓ {room_type.capitalize()}: {score}/10 — {result['condition']}")
        if result.get("key_observations"):
            for obs in result["key_observations"]:
                print(f"      · {obs}")
        if result.get("red_flags"):
            for flag in result["red_flags"]:
                print(f"      ⚠ RED FLAG: {flag}")

        # Update photo score in database
        supabase.table("photos") \
            .update({"renovation_score": score}) \
            .eq("id", photo["id"]) \
            .execute()

    # Calculate overall classification
    classification = classify_from_scores(room_scores)
    avg_score = sum(room_scores.values()) / len(room_scores) if room_scores else 0

    print(f"\n  → Overall: {avg_score:.1f}/10 — {classification.upper()}")

    # Update listing in database
    update_listing(listing_id, {
        "classification":   classification,
        "renovation_score": round(avg_score, 1),
    })

    return {
        "listing_id":       listing_id,
        "room_scores":      room_scores,
        "room_details":     room_details,
        "avg_score":        round(avg_score, 1),
        "classification":   classification,
        "red_flags":        all_red_flags
    }


# ─────────────────────────────────────────
# CLASSIFY PROPERTY STYLE
# ─────────────────────────────────────────
def classify_property_style(photo_urls: list) -> dict:
    """
    Classify property style (character home vs new build) using the first 1-2 photos.
    Used to calibrate ARV — new builds fetch a premium over renovated character homes.
    Returns style classification and confidence.
    """
    import requests
    import base64

    if not photo_urls:
        return {"style": "uncertain", "confidence": 0.0, "reasoning": "No photos available"}

    # Download first photo (usually exterior/hero shot)
    photo_b64 = None
    for url in photo_urls[:3]:
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
                photo_b64 = base64.b64encode(response.content).decode("utf-8")
                break
        except Exception:
            continue

    if not photo_b64:
        return {"style": "uncertain", "confidence": 0.0, "reasoning": "Could not download photos"}

    prompt = """You are a property analyst looking at an Australian residential property photo.

Classify the property style to help determine an accurate ARV (after renovation value).

Categories:
- character_home: older style home, likely pre-1990s — visible age in roofline, windows, brickwork, weatherboard,
  fibro, period features, older construction materials. These sell for less than new builds even when fully renovated.
- new_build: clearly modern/contemporary construction, built post-2000s — clean lines, modern cladding,
  large glazing, new materials throughout. Commands a premium over renovated older homes.
- modern_renovated: older structure but heavily modernised exterior — hard to tell original age
- uncertain: interior shot, or genuinely can't determine age/style from this photo

This classification is used to calibrate ARV estimates, so be conservative — only classify as new_build
if you are confident.

Respond in JSON only, no other text:
{
    "style": "<character_home|new_build|modern_renovated|uncertain>",
    "confidence": <float 0.0-1.0>,
    "reasoning": "<one sentence>"
}"""

    try:
        response = client.messages.create(
            model=MODELS["classification"],
            max_tokens=150,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": photo_b64
                        }
                    },
                    {"type": "text", "text": prompt}
                ]
            }]
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        print(f"  → Property style: {result['style']} ({result['confidence']:.0%}) — {result['reasoning']}")
        return result

    except Exception as e:
        print(f"  ✗ Property style classification error: {e}")
        return {"style": "uncertain", "confidence": 0.0, "reasoning": str(e)}


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    # Use the test listing we created in the photos test
    print("Fetching test listing from database...")

    result = supabase.table("listings") \
        .select("id, address") \
        .eq("domain_id", "test-123") \
        .single() \
        .execute()

    if not result.data:
        print("✗ Test listing not found — run the photos test first")
        sys.exit(1)

    listing_id = result.data["id"]
    print(f"✓ Found test listing: {result.data['address']}")
    print(f"  ID: {listing_id}\n")

    print("Scoring renovation condition...")
    scoring = score_listing_renovation(listing_id)

    print(f"\n{'='*50}")
    print(f"RESULT SUMMARY")
    print(f"{'='*50}")
    print(f"Kitchen score:   {scoring['room_scores'].get('kitchen', 'N/A')}/10")
    print(f"Bathroom score:  {scoring['room_scores'].get('bathroom', 'N/A')}/10")
    print(f"Average:         {scoring['avg_score']}/10")
    print(f"Classification:  {scoring['classification'].upper()}")
    if scoring['red_flags']:
        print(f"Red flags:       {', '.join(scoring['red_flags'])}")
    else:
        print(f"Red flags:       None")
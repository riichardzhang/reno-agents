# classifiers/text.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import anthropic
from config import ANTHROPIC_API_KEY, MODELS
from db.client import update_listing, supabase

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────
# KEYWORD SIGNALS
# ─────────────────────────────────────────
UNRENOVATED_SIGNALS = [
    "original", "period home", "period features", "original features",
    "as is", "as-is", "deceased estate", "estate sale",
    "potential", "opportunity", "handyman", "tlc",
    "priced to sell", "land value", "developers",
    "original condition", "original kitchen", "original bathroom",
    "retro", "vintage", "character home", "needs work",
    "fixer", "project", "sweat equity", "cosmetic",
    "original floorboards", "original tiles"
]

RENOVATED_SIGNALS = [
    "renovated", "fully renovated", "freshly renovated", "newly renovated",
    "updated", "modern", "contemporary", "stylish",
    "stone benchtops", "stone bench", "caesar stone", "granite",
    "new kitchen", "new bathroom", "new flooring", "new carpet",
    "ducted heating", "ducted cooling", "ducted air",
    "move in ready", "move-in ready", "nothing to do", "turn key", "turnkey",
    "high end", "high-end", "luxury", "premium finishes",
    "custom cabinetry", "soft close", "butler's pantry",
    "heated floors", "hydronic", "plantation shutters",
    "alfresco", "outdoor entertaining"
]

# ─────────────────────────────────────────
# KEYWORD PRE-FILTER (FREE)
# ─────────────────────────────────────────
def classify_from_keywords(description: str) -> dict:
    """
    Fast free classification using keyword matching.
    Returns classification and matched signals.
    """
    if not description:
        return {"classification": "uncertain", "confidence": 0.0, "signals": []}

    desc_lower = description.lower()

    unrenovated_matches = [s for s in UNRENOVATED_SIGNALS if s in desc_lower]
    renovated_matches = [s for s in RENOVATED_SIGNALS if s in desc_lower]

    unrenovated_count = len(unrenovated_matches)
    renovated_count = len(renovated_matches)

    # Clear signal in one direction
    if unrenovated_count >= 2 and renovated_count == 0:
        return {
            "classification": "unrenovated",
            "confidence": min(0.9, 0.5 + unrenovated_count * 0.1),
            "signals": unrenovated_matches
        }
    elif renovated_count >= 2 and unrenovated_count == 0:
        return {
            "classification": "renovated",
            "confidence": min(0.9, 0.5 + renovated_count * 0.1),
            "signals": renovated_matches
        }
    elif unrenovated_count >= 1 and renovated_count == 0:
        return {
            "classification": "unrenovated",
            "confidence": 0.6,
            "signals": unrenovated_matches
        }
    elif renovated_count >= 1 and unrenovated_count == 0:
        return {
            "classification": "renovated",
            "confidence": 0.6,
            "signals": renovated_matches
        }
    elif unrenovated_count > renovated_count:
        return {
            "classification": "partial",
            "confidence": 0.5,
            "signals": unrenovated_matches + renovated_matches
        }
    elif renovated_count > unrenovated_count:
        return {
            "classification": "partial",
            "confidence": 0.5,
            "signals": unrenovated_matches + renovated_matches
        }
    else:
        return {
            "classification": "uncertain",
            "confidence": 0.3,
            "signals": unrenovated_matches + renovated_matches
        }


# ─────────────────────────────────────────
# CLAUDE TEXT CLASSIFICATION
# ─────────────────────────────────────────
def classify_via_claude(description: str) -> dict:
    """
    Use Claude Haiku to classify renovation status from description.
    Only called when keyword matching returns uncertain/low confidence.
    """
    if not description or len(description.strip()) < 20:
        return {
            "classification": "uncertain",
            "confidence": 0.0,
            "signals": [],
            "reasoning": "Description too short to classify"
        }

    prompt = f"""You are a property analyst. Classify this Australian property listing description.

Determine if the property is renovated, unrenovated, partial, or uncertain.

Definitions:
- unrenovated: original condition, needs work, period features, deceased estate, cosmetic opportunity
- renovated: modern finishes, updated kitchen/bathroom, nothing to do, move in ready
- partial: some rooms updated but others original, mixed signals
- uncertain: not enough information to determine

Description:
{description[:1000]}

Respond in JSON only, no other text:
{{
    "classification": "<unrenovated|renovated|partial|uncertain>",
    "confidence": <float 0.0-1.0>,
    "signals": ["<key phrase 1>", "<key phrase 2>"],
    "reasoning": "<one sentence explanation>"
}}"""

    try:
        response = client.messages.create(
            model=MODELS["classification"],
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        return result

    except json.JSONDecodeError as e:
        print(f"    ✗ JSON parse error: {e}")
        return {"classification": "uncertain", "confidence": 0.0, "signals": [], "reasoning": "Parse error"}
    except Exception as e:
        print(f"    ✗ Claude text classification error: {e}")
        return {"classification": "uncertain", "confidence": 0.0, "signals": [], "reasoning": str(e)}


# ─────────────────────────────────────────
# COMBINED CLASSIFIER
# ─────────────────────────────────────────
def classify_listing_text(listing_id: str, description: str) -> dict:
    """
    Classify renovation status from listing description.
    Uses keyword matching first, falls back to Claude if uncertain.
    Updates listing in database.
    """
    print(f"  → Text classification...")

    # Step 1: Try keyword matching (free)
    keyword_result = classify_from_keywords(description)
    print(f"    Keyword match: {keyword_result['classification']} "
          f"(confidence: {keyword_result['confidence']:.0%})")

    if keyword_result["signals"]:
        print(f"    Signals: {', '.join(keyword_result['signals'][:5])}")

    # Step 2: If confident enough, use keyword result
    if keyword_result["confidence"] >= 0.6:
        final_result = keyword_result
        final_result["method"] = "keywords"
        print(f"    ✓ Using keyword result (high confidence)")
    else:
        # Step 3: Fall back to Claude for uncertain cases
        print(f"    → Low confidence — using Claude for classification...")
        claude_result = classify_via_claude(description)
        claude_result["method"] = "claude"
        final_result = claude_result
        print(f"    ✓ Claude: {claude_result['classification']} "
              f"(confidence: {claude_result.get('confidence', 0):.0%})")
        if claude_result.get("reasoning"):
            print(f"    Reasoning: {claude_result['reasoning']}")

    # Store text classification result — merge with any existing vision classification
    # Text classification stored as a note, vision score takes precedence
    existing = supabase.table("listings") \
        .select("classification") \
        .eq("id", listing_id) \
        .single() \
        .execute()

    existing_classification = existing.data.get("classification") if existing.data else None

    # If we already have a vision-based classification, don't overwrite it
    # Just return the text result for the pipeline to use
    if not existing_classification or existing_classification == "uncertain":
        update_listing(listing_id, {
            "classification": final_result["classification"]
        })
        print(f"    ✓ Updated listing classification: {final_result['classification']}")
    else:
        print(f"    → Vision classification already set: {existing_classification} (keeping)")

    return final_result


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    test_descriptions = [
        {
            "label": "Clear unrenovated",
            "text": "Deceased estate in original condition. Period features throughout including original kitchen and bathroom. Hardwood floorboards. Enormous potential for the astute buyer. Priced to sell. Land value location."
        },
        {
            "label": "Clear renovated",
            "text": "Fully renovated family home with nothing left to do. Modern kitchen featuring stone benchtops, soft close cabinetry and quality appliances. Freshly renovated bathroom with floor to ceiling tiles. New flooring throughout. Ducted heating and cooling. Move in ready."
        },
        {
            "label": "Partial / mixed",
            "text": "Charming period home with original floorboards and character features. Kitchen has been updated with modern appliances. Original bathroom. Large backyard. Great bones with some modern updates."
        },
        {
            "label": "Uncertain / minimal description",
            "text": "3 bedroom house in great location. Close to schools and shops. Contact agent for details."
        }
    ]

    print("Testing text classifier...\n")

    for test in test_descriptions:
        print(f"{'='*50}")
        print(f"TEST: {test['label']}")
        print(f"{'='*50}")

        # Test keyword classifier
        keyword_result = classify_from_keywords(test["text"])
        print(f"Keywords: {keyword_result['classification']} "
              f"({keyword_result['confidence']:.0%}) — {keyword_result['signals']}")

        # Test Claude classifier directly
        print("Claude: ", end="")
        claude_result = classify_via_claude(test["text"])
        print(f"{claude_result['classification']} "
              f"({claude_result.get('confidence', 0):.0%}) — {claude_result.get('reasoning', '')}")
        print()
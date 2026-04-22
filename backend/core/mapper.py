import os, json, re
import aiohttp
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        return False

load_dotenv()

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT = """You are a data migration expert. Your job is to map fields from a source CRM to a destination CRM.

Rules:
- Match fields by semantic meaning, not just name similarity
- Assign confidence 0.0-1.0 (1.0=certain match, 0.5=plausible, 0.0=no good match)
- If no good match exists in the destination, set dest_field to null and confidence to 0.0
- Flag fields that need data transformation with transform_needed: true
- Return ONLY valid JSON array, no prose, no markdown

Output format exactly:
[{"source_field":"string","dest_field":"string or null","source_type":"string","dest_type":"string or null","confidence":0.0,"reasoning":"one sentence","transform_needed":false,"transform_hint":"string or null"}]"""

class MappingEngine:

    def __init__(self):
        self.api_key = os.environ.get("OPENAI_API_KEY", "")

    async def propose_mappings(
        self,
        source_platform: str,
        dest_platform: str,
        object_type: str,
        source_schema: list[dict],
        dest_schema: list[dict],
    ) -> list[dict]:

        src_simplified = [{"name": f["name"], "label": f.get("label", f["name"]), "type": f.get("type", "string")} for f in source_schema]
        dst_simplified = [{"name": f["name"], "label": f.get("label", f["name"]), "type": f.get("type", "string")} for f in dest_schema]

        user_prompt = f"""Source platform: {source_platform}
Source object type: {object_type}
Source fields (name, label, type):
{json.dumps(src_simplified, indent=2)}

Destination platform: {dest_platform}
Destination fields (name, label, type):
{json.dumps(dst_simplified, indent=2)}

Map every source field to its best destination match."""

        if not self.api_key:
            return self._fallback_mappings(source_schema, dest_schema)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": "gpt-5.4-mini",
            "max_tokens": 4096,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_API_URL, headers=headers, json=body) as resp:
                resp.raise_for_status()
                data = await resp.json()

        text = data["choices"][0]["message"]["content"].strip()
        # Strip any accidental markdown fences
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        mappings = json.loads(text)
        return mappings

    def _fallback_mappings(self, source_schema: list, dest_schema: list) -> list:
        """Simple name-match fallback when no API key is set. For testing only."""
        dst_names = {f["name"].lower(): f for f in dest_schema}
        results = []
        for src in source_schema:
            name_lower = src["name"].lower()
            dst_match = dst_names.get(name_lower)
            results.append({
                "source_field": src["name"],
                "dest_field": dst_match["name"] if dst_match else None,
                "source_type": src.get("type", "string"),
                "dest_type": dst_match.get("type", "string") if dst_match else None,
                "confidence": 0.9 if dst_match else 0.0,
                "reasoning": "Exact name match" if dst_match else "No match found",
                "transform_needed": False,
                "transform_hint": None,
            })
        return results

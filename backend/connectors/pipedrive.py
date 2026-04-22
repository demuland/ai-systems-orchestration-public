import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

BASE_URL = "https://api.pipedrive.com"

PIPEDRIVE_OBJECTS = ["persons", "organizations", "deals"]

FIELD_ENDPOINTS = {
    "persons": "/v1/personFields",
    "organizations": "/v1/organizationFields",
    "deals": "/v1/dealFields",
}

RECORD_ENDPOINTS = {
    "persons": "/v1/persons",
    "organizations": "/v1/organizations",
    "deals": "/v1/deals",
}

CREATE_ENDPOINTS = {
    "persons": "/v1/persons",
    "organizations": "/v1/organizations",
    "deals": "/v1/deals",
}

class PipedriveConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_token = credentials.get("api_token")
        self._rate_limit_interval = 0.05  # ~20 req/sec to stay safe
        self._field_key_map: dict[str, dict] = {}   # object_type -> {hash_key: label}
        self._field_label_map: dict[str, dict] = {} # object_type -> {label: hash_key}

    def _params(self, extra: dict = None) -> dict:
        p = {"api_token": self.api_token}
        if extra:
            p.update(extra)
        return p

    async def _get(self, path: str, params: dict = None) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{BASE_URL}{path}", params=self._params(params)) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def _post(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{BASE_URL}{path}", params=self._params(), json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def test_connection(self) -> bool:
        try:
            result = await self._get("/v1/persons", {"limit": 1})
            return result.get("success", False)
        except Exception:
            return False

    async def get_object_types(self) -> list[str]:
        return PIPEDRIVE_OBJECTS

    async def _load_field_map(self, object_type: str):
        if object_type in self._field_key_map:
            return
        endpoint = FIELD_ENDPOINTS.get(object_type)
        if not endpoint:
            return
        data = await self._get(endpoint)
        key_map = {}    # hash -> label (for reading records)
        label_map = {}  # label -> hash (for writing records)
        for field in data.get("data", []):
            hash_key = field["key"]
            label = field.get("name", hash_key)
            key_map[hash_key] = label
            label_map[label] = hash_key
            # Also map lowercase label for case-insensitive lookup
            label_map[label.lower()] = hash_key
        self._field_key_map[object_type] = key_map
        self._field_label_map[object_type] = label_map

    async def get_schema(self, object_type: str) -> list[dict]:
        endpoint = FIELD_ENDPOINTS.get(object_type)
        if not endpoint:
            return []
        data = await self._get(endpoint)
        fields = []
        for f in data.get("data", []):
            fields.append({
                "name": f["key"],
                "label": f.get("name", f["key"]),
                "type": f.get("field_type", "varchar"),
                "required": f.get("mandatory_flag", False),
            })
        return fields

    async def get_records(self, object_type: str, batch_size: int = 100) -> AsyncGenerator[list[dict], None]:
        await self._load_field_map(object_type)
        endpoint = RECORD_ENDPOINTS.get(object_type)
        start = 0
        while True:
            data = await self._get(endpoint, {"limit": min(batch_size, 500), "start": start})
            items = data.get("data") or []
            if not items:
                break
            records = []
            for item in items:
                record = {"_id": str(item["id"]), "_object_type": object_type}
                for key, val in item.items():
                    if key == "id":
                        continue
                    label = self._field_key_map.get(object_type, {}).get(key, key)
                    record[label] = val

                # Extract inline associations from nested FK fields
                assocs = []
                if object_type in ("persons",) and item.get("org_id") and isinstance(item["org_id"], dict):
                    assocs.append({"to_object_type": "organization", "to_record_id": str(item["org_id"]["value"]), "relationship_type": "person_to_org"})
                if object_type in ("deals",):
                    if item.get("person_id") and isinstance(item["person_id"], dict):
                        assocs.append({"to_object_type": "person", "to_record_id": str(item["person_id"]["value"]), "relationship_type": "deal_to_person"})
                    if item.get("org_id") and isinstance(item["org_id"], dict):
                        assocs.append({"to_object_type": "organization", "to_record_id": str(item["org_id"]["value"]), "relationship_type": "deal_to_org"})
                record["_associations"] = assocs
                records.append(record)
            yield records
            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start += len(items)


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for a Pipedrive record."""
        assocs = []
        try:
            if object_type in ("persons", "contacts"):
                data = await self._get(f"/v1/persons/{record_id}/deals")
                for item in (data.get("data") or []):
                    assocs.append({"to_object_type": "deal", "to_record_id": str(item["id"]), "relationship_type": "person_to_deal"})
                # Person's org is available inline on the person record (org_id FK); no separate endpoint exists
            elif object_type in ("deals",):
                data = await self._get(f"/v1/deals/{record_id}/persons")
                for item in (data.get("data") or []):
                    assocs.append({"to_object_type": "person", "to_record_id": str(item["id"]), "relationship_type": "deal_to_person"})
                data2 = await self._get(f"/v1/deals/{record_id}")
                org_id = (data2.get("data") or {}).get("org_id", {})
                if org_id and isinstance(org_id, dict):
                    assocs.append({"to_object_type": "organization", "to_record_id": str(org_id["value"]), "relationship_type": "deal_to_org"})
        except Exception as e:
            print(f"[Pipedrive] Association fetch failed for {object_type}:{record_id}: {e}")
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Link records in Pipedrive via update — Pipedrive uses foreign keys not separate association calls."""
        try:
            # Deal to person: update deal with person_id
            if from_object_type in ("deals",) and to_object_type in ("persons", "person", "contacts"):
                await self._put(f"/v1/deals/{from_record_id}", {"person_id": int(to_record_id)})
                return True
            # Deal to org: update deal with org_id
            if from_object_type in ("deals",) and to_object_type in ("organizations", "organization", "companies"):
                await self._put(f"/v1/deals/{from_record_id}", {"org_id": int(to_record_id)})
                return True
            # Person to org: update person with org_id
            if from_object_type in ("persons", "person", "contacts") and to_object_type in ("organizations", "organization", "companies"):
                await self._put(f"/v1/persons/{from_record_id}", {"org_id": int(to_record_id)})
                return True
        except Exception as e:
            print(f"[Pipedrive] Association failed {from_object_type}:{from_record_id} -> {to_object_type}:{to_record_id}: {e}")
        return False

    async def _put(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.put(f"{BASE_URL}{path}", params=self._params(), json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        try:
            await self._load_field_map(object_type)
            label_map = self._field_label_map.get(object_type, {})
            translated = {}
            for k, v in data.items():
                dest_key = label_map.get(k) or label_map.get(k.lower()) or k
                translated[dest_key] = v
            endpoint = {"persons": "/v1/persons", "organizations": "/v1/organizations", "deals": "/v1/deals"}.get(object_type, f"/v1/{object_type}")
            await self._put(f"{endpoint}/{record_id}", translated)
            return True
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        """Search for existing Pipedrive record by name or email."""
        try:
            SEARCH_FIELD = {"persons": "name", "organizations": "name", "deals": "title"}
            field = SEARCH_FIELD.get(object_type)
            val = unique_data.get(field) or unique_data.get("name") or unique_data.get("email")
            if not val:
                return None
            data = await self._get("/v1/itemSearch", {"term": val, "item_type": object_type.rstrip("s"), "limit": 1})
            items = (data.get("data") or {}).get("items", [])
            return str(items[0]["item"]["id"]) if items else None
        except Exception:
            return None

    async def get_owner_reference_map(self) -> dict:
        """Return {user_id: user_name} for all Pipedrive users."""
        try:
            data = await self._get("/v1/users")
            return {str(u["id"]): u.get("name", str(u["id"])) for u in (data.get("data") or [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        """Return {stage_id: stage_name} for all Pipedrive pipeline stages."""
        try:
            data = await self._get("/v1/stages")
            return {str(s["id"]): s.get("name", str(s["id"])) for s in (data.get("data") or [])}
        except Exception:
            return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        # Ensure field maps are loaded so we can translate labels -> hash keys
        await self._load_field_map(object_type)
        label_map = self._field_label_map.get(object_type, {})

        translated = {}
        for k, v in data.items():
            if k.startswith("_") or v is None:
                continue
            # If key is a human-readable label, translate back to Pipedrive hash key
            # Try exact match first, then lowercase
            dest_key = label_map.get(k) or label_map.get(k.lower()) or k
            translated[dest_key] = v

        endpoint = CREATE_ENDPOINTS.get(object_type)
        result = await self._post(endpoint, translated)
        return str(result["data"]["id"])

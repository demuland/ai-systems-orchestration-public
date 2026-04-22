import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

HUBSPOT_OBJECTS = ["contacts", "companies", "deals"]
BASE_URL = "https://api.hubapi.com"

# Map plural object type names to singular for association tracking
SINGULAR = {"contacts": "contact", "companies": "company", "deals": "deal"}

class HubSpotConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_key = credentials.get("api_key") or credentials.get("access_token")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self._rate_limit_interval = 0.1  # ~10 req/sec to be safe

    async def _get(self, path: str, params: dict = None) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{BASE_URL}{path}", headers=self.headers, params=params or {}) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def _post(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{BASE_URL}{path}", headers=self.headers, json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def test_connection(self) -> bool:
        try:
            result = await self._get("/crm/v3/objects/contacts", {"limit": 1})
            return True
        except Exception:
            return False

    async def get_object_types(self) -> list[str]:
        return HUBSPOT_OBJECTS

    async def get_schema(self, object_type: str) -> list[dict]:
        data = await self._get(f"/crm/v3/properties/{object_type}")
        fields = []
        for prop in data.get("results", []):
            fields.append({
                "name": prop["name"],
                "label": prop.get("label", prop["name"]),
                "type": prop.get("type", "string"),
                "field_type": prop.get("fieldType", "text"),
                "required": False,
            })
        return fields


    async def get_records(self, object_type: str, batch_size: int = 100) -> AsyncGenerator[list[dict], None]:
        after = None
        schema = await self.get_schema(object_type)
        all_props = [f["name"] for f in schema]

        # Association types to fetch inline per object type
        ASSOC_MAP = {
            "contacts": ["companies"],
            "deals": ["contacts", "companies"],
            "companies": [],
        }
        assoc_types = ASSOC_MAP.get(object_type, [])

        while True:
            params = {"limit": min(batch_size, 100), "properties": ",".join(all_props)}
            if assoc_types:
                params["associations"] = ",".join(assoc_types)
            if after:
                params["after"] = after
            data = await self._get(f"/crm/v3/objects/{object_type}", params)
            results = data.get("results", [])
            if not results:
                break
            records = []
            for r in results:
                record = {"_id": r["id"], "_object_type": object_type}
                record.update(r.get("properties", {}))
                # Extract inline associations
                assocs = []
                for assoc_type, assoc_data in r.get("associations", {}).items():
                    for item in assoc_data.get("results", []):
                        assocs.append({
                            "to_object_type": SINGULAR.get(assoc_type, assoc_type.rstrip("s")),
                            "to_record_id": str(item["id"]),
                            "relationship_type": item.get("type", ""),
                        })
                record["_associations"] = assocs
                records.append(record)
            yield records
            paging = data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")
            if not after:
                break

    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for a single HubSpot record."""
        ASSOC_TARGETS = {
            "contacts": ["companies", "deals"],
            "deals": ["contacts", "companies"],
            "companies": ["contacts", "deals"],
        }
        targets = ASSOC_TARGETS.get(object_type, [])
        assocs = []
        for target in targets:
            try:
                data = await self._get(f"/crm/v4/objects/{object_type}/{record_id}/associations/{target}")
                for item in data.get("results", []):
                    assocs.append({
                        "to_object_type": SINGULAR.get(target, target.rstrip("s")),
                        "to_record_id": str(item.get("toObjectId", item.get("id", ""))),
                        "relationship_type": str(item.get("associationTypeId", "")),
                    })
            except Exception:
                pass
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Create an association between two HubSpot records using v4 associations API."""
        try:
            # Default association type IDs per pair
            ASSOC_TYPE_IDS = {
                ("contact", "company"):  [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 279}],
                ("contact", "deal"):     [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 4}],
                ("deal", "contact"):     [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
                ("deal", "company"):     [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 341}],
                ("company", "contact"):  [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 280}],
                ("company", "deal"):     [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 342}],
            }
            from_s = from_object_type.lower()
            to_s = to_object_type.lower()
            pair = (SINGULAR.get(from_s, from_s.rstrip("s")), SINGULAR.get(to_s, to_s.rstrip("s")))
            assoc_types = ASSOC_TYPE_IDS.get(pair, [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 1}])
            payload = {"inputs": [{"from": {"id": from_record_id}, "to": {"id": to_record_id}, "types": assoc_types}]}
            await self._post(f"/crm/v4/associations/{from_object_type}/{to_object_type}/batch/create", payload)
            return True
        except Exception as e:
            print(f"[HubSpot] Association creation failed {from_object_type}:{from_record_id} -> {to_object_type}:{to_record_id}: {e}")
            return False

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        """Update an existing HubSpot record."""
        try:
            properties = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
            async with aiohttp.ClientSession() as session:
                async with session.patch(
                    f"{BASE_URL}/crm/v3/objects/{object_type}/{record_id}",
                    headers=self.headers, json={"properties": properties}
                ) as resp:
                    return resp.status in (200, 204)
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        """Search for existing record by email (contacts) or name (companies/deals)."""
        try:
            SEARCH_FIELD = {"contacts": "email", "companies": "name", "deals": "dealname"}
            field = SEARCH_FIELD.get(object_type)
            if not field or not unique_data.get(field):
                return None
            payload = {"filterGroups": [{"filters": [{"propertyName": field, "operator": "EQ", "value": unique_data[field]}]}], "limit": 1}
            data = await self._post(f"/crm/v3/objects/{object_type}/search", payload)
            results = data.get("results", [])
            return results[0]["id"] if results else None
        except Exception:
            return None

    async def get_owner_reference_map(self) -> dict:
        """Return {owner_id: owner_email} for all HubSpot users."""
        try:
            data = await self._get("/crm/v3/owners", {"limit": 500})
            return {str(o["id"]): o.get("email", str(o["id"])) for o in data.get("results", [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        """Return {stage_id: stage_label} for all deal pipeline stages."""
        try:
            data = await self._get("/crm/v3/pipelines/deals")
            stage_map = {}
            for pipeline in data.get("results", []):
                for stage in pipeline.get("stages", []):
                    stage_map[stage["id"]] = stage.get("label", stage["id"])
            return stage_map
        except Exception:
            return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        properties = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
        result = await self._post(f"/crm/v3/objects/{object_type}", {"properties": properties})
        return result["id"]

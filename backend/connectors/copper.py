import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

# Copper CRM Developer API v1
# Auth: X-PW-AccessToken header + X-PW-Application + X-PW-UserEmail
# Base URL: https://api.copper.com/developer_api/v1
# Rate limit: not officially published — practical safe rate is ~10 req/sec
# Objects: people (contacts), companies, opportunities (deals)
# Note: Copper is Google Workspace native — primary users are Google shop teams

COPPER_OBJECTS = ["people", "companies", "opportunities"]
COPPER_BASE_URL = "https://api.copper.com/developer_api/v1"

class CopperConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_token = credentials.get("api_token")
        self.user_email = credentials.get("user_email")  # Required by Copper API
        self.headers = {
            "X-PW-AccessToken": self.api_token,
            "X-PW-Application": "developer_api",
            "X-PW-UserEmail": self.user_email,
            "Content-Type": "application/json",
        }
        self._rate_limit_interval = 0.15

    async def _post_search(self, path: str, data: dict) -> dict:
        """Copper uses POST for search/list operations — not standard GET."""
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{COPPER_BASE_URL}{path}", headers=self.headers, json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def _get(self, path: str) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{COPPER_BASE_URL}{path}", headers=self.headers) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def _post(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{COPPER_BASE_URL}{path}", headers=self.headers, json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def test_connection(self) -> bool:
        try:
            result = await self._post_search("/people/search", {"page_size": 1, "page_number": 1})
            return isinstance(result, list)
        except Exception:
            return False

    async def get_object_types(self) -> list[str]:
        return COPPER_OBJECTS

    async def get_schema(self, object_type: str) -> list[dict]:
        """Copper: GET /custom_field_definitions returns all custom fields.
        Standard fields are hardcoded per object type."""
        standard_fields = {
            "people": [
                {"name": "name", "label": "Full Name", "type": "string", "required": True, "custom": False},
                {"name": "emails", "label": "Emails", "type": "string", "required": False, "custom": False},
                {"name": "phone_numbers", "label": "Phone Numbers", "type": "string", "required": False, "custom": False},
                {"name": "title", "label": "Job Title", "type": "string", "required": False, "custom": False},
                {"name": "company_name", "label": "Company", "type": "string", "required": False, "custom": False},
                {"name": "linkedin_url", "label": "LinkedIn", "type": "string", "required": False, "custom": False},
                {"name": "address", "label": "Address", "type": "string", "required": False, "custom": False},
            ],
            "companies": [
                {"name": "name", "label": "Company Name", "type": "string", "required": True, "custom": False},
                {"name": "email_domain", "label": "Email Domain", "type": "string", "required": False, "custom": False},
                {"name": "phone_numbers", "label": "Phone Numbers", "type": "string", "required": False, "custom": False},
                {"name": "website", "label": "Website", "type": "string", "required": False, "custom": False},
                {"name": "linkedin_url", "label": "LinkedIn", "type": "string", "required": False, "custom": False},
                {"name": "address", "label": "Address", "type": "string", "required": False, "custom": False},
            ],
            "opportunities": [
                {"name": "name", "label": "Opportunity Name", "type": "string", "required": True, "custom": False},
                {"name": "monetary_value", "label": "Value", "type": "number", "required": False, "custom": False},
                {"name": "status", "label": "Status", "type": "string", "required": False, "custom": False},
                {"name": "close_date", "label": "Close Date", "type": "date", "required": False, "custom": False},
                {"name": "pipeline_id", "label": "Pipeline", "type": "string", "required": False, "custom": False},
                {"name": "pipeline_stage_id", "label": "Stage", "type": "string", "required": False, "custom": False},
                {"name": "win_probability", "label": "Win Probability", "type": "number", "required": False, "custom": False},
            ],
        }
        fields = list(standard_fields.get(object_type, []))
        # Fetch custom field definitions
        try:
            custom_fields = await self._get("/custom_field_definitions")
            for f in custom_fields:
                # Copper custom fields apply to multiple object types
                available_on = f.get("available_on", [])
                obj_map = {"person": "people", "company": "companies", "opportunity": "opportunities"}
                if any(obj_map.get(o) == object_type for o in available_on):
                    fields.append({
                        "name": str(f.get("id", "")),
                        "label": f.get("name", ""),
                        "type": f.get("data_type", "string"),
                        "required": False,
                        "custom": True,
                    })
        except Exception as e:
            print(f"[Copper] Custom field fetch failed for {object_type}: {e}")
        return fields

    async def get_records(self, object_type: str, batch_size: int = 200) -> AsyncGenerator[list[dict], None]:
        """Copper uses POST /search for listing records with pagination."""
        page = 1
        while True:
            results = await self._post_search(f"/{object_type}/search", {
                "page_size": min(batch_size, 200),
                "page_number": page,
                "sort_by": "id",
                "sort_direction": "asc",
            })
            if not results or not isinstance(results, list):
                break
            records = []
            for item in results:
                record = {"_id": str(item.get("id", "")), "_object_type": object_type}
                # Flatten email/phone arrays to strings for portability
                if "emails" in item:
                    item["emails"] = ", ".join(e.get("email", "") for e in item["emails"] if e.get("email"))
                if "phone_numbers" in item:
                    item["phone_numbers"] = ", ".join(p.get("number", "") for p in item["phone_numbers"] if p.get("number"))
                record.update({k: v for k, v in item.items() if k != "id"})
                records.append(record)
            yield records
            if len(results) < batch_size:
                break
            page += 1


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for a Copper record via the related items API."""
        assocs = []
        try:
            ENTITY_MAP = {"persons": "person", "people": "person", "contacts": "person",
                          "companies": "company", "organizations": "company",
                          "deals": "opportunity", "opportunities": "opportunity"}
            entity = ENTITY_MAP.get(object_type, object_type.rstrip("s"))

            data = await self._post(f"/developer_api/v1/{object_type}/{record_id}/related", {})
            for item in data if isinstance(data, list) else []:
                assocs.append({
                    "to_object_type": item.get("type", "unknown"),
                    "to_record_id": str(item.get("id", "")),
                    "relationship_type": f"{entity}_to_{item.get('type', 'unknown')}",
                })
        except Exception as e:
            # Copper related items endpoint may not exist for all types
            # Fall back to inline FK extraction
            pass

        # Also extract inline FKs from the record itself
        try:
            record_data = await self._get(f"/developer_api/v1/{object_type}/{record_id}")
            if isinstance(record_data, dict):
                if record_data.get("company_id"):
                    assocs.append({"to_object_type": "company", "to_record_id": str(record_data["company_id"]), "relationship_type": "person_to_company"})
                if record_data.get("primary_contact_id"):
                    assocs.append({"to_object_type": "person", "to_record_id": str(record_data["primary_contact_id"]), "relationship_type": "opportunity_to_person"})
                if record_data.get("company") and isinstance(record_data["company"], dict):
                    assocs.append({"to_object_type": "company", "to_record_id": str(record_data["company"]["id"]), "relationship_type": "opportunity_to_company"})
        except Exception:
            pass

        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Create associations in Copper via record update — Copper uses FK-based relationships."""
        try:
            if from_object_type in ("persons", "people", "contacts") and to_object_type in ("companies", "company"):
                await self._put(f"/developer_api/v1/people/{from_record_id}", {"company_id": int(to_record_id)})
                return True
            if from_object_type in ("deals", "opportunities") and to_object_type in ("persons", "people", "contacts"):
                await self._put(f"/developer_api/v1/opportunities/{from_record_id}", {"primary_contact_id": int(to_record_id)})
                return True
            if from_object_type in ("deals", "opportunities") and to_object_type in ("companies", "company"):
                await self._put(f"/developer_api/v1/opportunities/{from_record_id}", {"company_id": int(to_record_id)})
                return True
        except Exception as e:
            print(f"[Copper] Association failed: {e}")
        return False

    async def _put(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"https://api.copper.com{path}",
                headers=self.headers, json=data
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        try:
            ENDPOINT_MAP = {"persons": "people", "contacts": "people", "companies": "companies",
                            "deals": "opportunities", "opportunities": "opportunities"}
            endpoint = ENDPOINT_MAP.get(object_type, object_type)
            await self._put(f"/developer_api/v1/{endpoint}/{record_id}", data)
            return True
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        try:
            ENDPOINT_MAP = {"persons": "people", "contacts": "people", "companies": "companies",
                            "deals": "opportunities"}
            endpoint = ENDPOINT_MAP.get(object_type, object_type)
            search_val = unique_data.get("name") or unique_data.get("email")
            if not search_val:
                return None
            data = await self._post(f"/developer_api/v1/{endpoint}/search", {"name": search_val, "page_size": 1})
            items = data if isinstance(data, list) else []
            return str(items[0]["id"]) if items else None
        except Exception:
            return None

    async def get_owner_reference_map(self) -> dict:
        try:
            data = await self._post("/developer_api/v1/users/search", {"page_size": 200})
            return {str(u["id"]): u.get("email", str(u["id"])) for u in (data if isinstance(data, list) else [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        try:
            data = await self._get("/developer_api/v1/pipeline_stages")
            return {str(s["id"]): s.get("name", str(s["id"])) for s in (data if isinstance(data, list) else [])}
        except Exception:
            return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        clean = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
        # Copper expects emails as array of objects
        if "emails" in clean and isinstance(clean["emails"], str):
            clean["emails"] = [{"email": e.strip(), "category": "work"}
                                for e in clean["emails"].split(",") if e.strip()]
        result = await self._post(f"/{object_type}", clean)
        return str(result.get("id", ""))

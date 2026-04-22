import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

# Zoho CRM v8 API
# Auth: OAuth 2.0. Credentials must include access_token and domain (e.g. zohoapis.com)
# Rate limit: 4,000 requests/day minimum, up to 25,000 or 500/user license

ZOHO_OBJECTS = ["Contacts", "Accounts", "Deals", "Leads"]

class ZohoConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.access_token = credentials.get("access_token")
        self.domain = credentials.get("domain", "zohoapis.com")
        self.base_url = f"https://www.{self.domain}/crm/v8"
        self.headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }
        self._rate_limit_interval = 0.15  # conservative — daily limit matters more than per-second
        self.ZOHO_OBJECT_MAP = {
            "contacts": "Contacts",
            "accounts": "Accounts",
            "deals": "Deals",
            "leads": "Leads",
            "Contacts": "Contacts",
            "Accounts": "Accounts",
            "Deals": "Deals",
            "Leads": "Leads",
        }

    async def _get(self, path: str, params: dict = None) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}{path}", headers=self.headers, params=params or {}) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def _post(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.base_url}{path}", headers=self.headers, json=data) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        return await retry_request(_do)

    async def test_connection(self) -> bool:
        try:
            result = await self._get("/Contacts", {"per_page": 1})
            return "data" in result or "info" in result
        except Exception:
            return False

    async def get_object_types(self) -> list[str]:
        return ZOHO_OBJECTS

    async def get_schema(self, object_type: str) -> list[dict]:
        """Zoho: GET /settings/fields?module=Contacts returns all field definitions"""
        try:
            data = await self._get("/settings/fields", {"module": object_type})
            fields = []
            for f in data.get("fields", []):
                fields.append({
                    "name": f.get("api_name", f.get("field_label", "")),
                    "label": f.get("field_label", f.get("api_name", "")),
                    "type": f.get("data_type", "string"),
                    "required": f.get("system_mandatory", False),
                    "custom": not f.get("system_mandatory", True),
                })
            return fields
        except Exception as e:
            print(f"[Zoho] Schema fetch failed for {object_type}: {e}")
            return []

    async def get_records(self, object_type: str, batch_size: int = 200) -> AsyncGenerator[list[dict], None]:
        page = 1
        while True:
            data = await self._get(f"/{object_type}", {
                "per_page": min(batch_size, 200),
                "page": page
            })
            records_raw = data.get("data", [])
            if not records_raw:
                break
            records = []
            for r in records_raw:
                record = {"_id": str(r.get("id", "")), "_object_type": object_type}
                record.update({k: v for k, v in r.items() if k != "id"})
                records.append(record)
            yield records
            info = data.get("info", {})
            if not info.get("more_records", False):
                break
            page += 1


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for a Zoho CRM record using the Related Records API."""
        module = self.ZOHO_OBJECT_MAP.get(object_type, object_type)
        RELATED_MAP = {
            "Contacts": ["Deals", "Accounts"],
            "Accounts": ["Contacts", "Deals"],
            "Deals": ["Contacts"],
        }
        targets = RELATED_MAP.get(module, [])
        assocs = []
        for target in targets:
            try:
                data = await self._get(f"/{module}/{record_id}/{target}")
                for item in data.get("data", []):
                    assocs.append({
                        "to_object_type": target.lower().rstrip("s"),
                        "to_record_id": str(item.get("id", "")),
                        "relationship_type": f"{module.lower()}_to_{target.lower()}",
                    })
            except Exception as e:
                print(f"[Zoho] Association fetch failed {module}:{record_id} -> {target}: {e}")
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Link records in Zoho using the Related Records API."""
        try:
            from_module = self.ZOHO_OBJECT_MAP.get(from_object_type, from_object_type)
            to_module = self.ZOHO_OBJECT_MAP.get(to_object_type, to_object_type)
            payload = {"data": [{"id": to_record_id}]}
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    f"{self.base_url}/{from_module}/{from_record_id}/{to_module}",
                    headers={**self.headers, "Content-Type": "application/json"},
                    json=payload
                ) as resp:
                    return resp.status in (200, 201, 204)
        except Exception as e:
            print(f"[Zoho] Association failed: {e}")
            return False

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        """Update an existing Zoho record."""
        try:
            module = self.ZOHO_OBJECT_MAP.get(object_type, object_type)
            payload = {"data": [{**data, "id": record_id}]}
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    f"{self.base_url}/{module}",
                    headers={**self.headers, "Content-Type": "application/json"},
                    json=payload
                ) as resp:
                    return resp.status in (200, 201, 204)
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        """Search Zoho for existing record by email or name."""
        try:
            module = self.ZOHO_OBJECT_MAP.get(object_type, object_type)
            SEARCH_FIELD = {"contacts": "Email", "leads": "Email", "accounts": "Account_Name", "deals": "Deal_Name"}
            field = SEARCH_FIELD.get(object_type.lower())
            val = unique_data.get(field) or unique_data.get("email") or unique_data.get("name")
            if not val:
                return None
            data = await self._get(f"/{module}/search", {"criteria": f"({field}:equals:{val})"})
            records = data.get("data", [])
            return str(records[0]["id"]) if records else None
        except Exception:
            return None

    async def get_owner_reference_map(self) -> dict:
        """Return {user_id: user_email} for all Zoho users."""
        try:
            data = await self._get("/users", {"type": "AllUsers"})
            return {str(u["id"]): u.get("email", str(u["id"])) for u in data.get("users", [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        """Return {stage_value: stage_display} for Zoho deal stages."""
        try:
            data = await self._get("/settings/fields", {"module": "Deals"})
            for field in data.get("fields", []):
                if field.get("api_name") == "Stage":
                    return {opt["actual_value"]: opt["display_value"] for opt in field.get("pick_list_values", [])}
        except Exception:
            pass
        return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        clean = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
        result = await self._post(f"/{object_type}", {"data": [clean]})
        return str(result["data"][0]["details"]["id"])

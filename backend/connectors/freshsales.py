import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

# Freshsales (Freshworks CRM) API
# Auth: API key via "Authorization: Token token=YOUR_API_KEY" header
# Base URL: https://{bundle_alias}.myfreshworks.com/crm/sales/api
# Rate limit: 1,000 requests/hour. 429 on breach.
# Objects: contacts, sales_accounts (companies), deals

FS_OBJECTS = ["contacts", "sales_accounts", "deals"]

class FreshsalesConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_key = credentials.get("api_key")
        self.bundle_alias = credentials.get("bundle_alias")  # e.g. "mycompany"
        self.base_url = f"https://{self.bundle_alias}.myfreshworks.com/crm/sales/api"
        self.headers = {
            "Authorization": f"Token token={self.api_key}",
            "Content-Type": "application/json",
        }
        self._rate_limit_interval = 0.12  # ~1000/hour = ~0.28/sec, stay conservative

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
            result = await self._get("/contacts/view/1", {"per_page": 1})
            return True
        except Exception:
            try:
                # Fallback: try contacts list endpoint
                result = await self._get("/contacts", {"per_page": 1, "page": 1})
                return True
            except Exception:
                return False

    async def get_object_types(self) -> list[str]:
        return FS_OBJECTS

    async def get_schema(self, object_type: str) -> list[dict]:
        """Freshsales: GET /settings/{object_type}/fields returns all field definitions"""
        schema_endpoints = {
            "contacts": "/settings/contacts/fields",
            "sales_accounts": "/settings/sales_accounts/fields",
            "deals": "/settings/deals/fields",
        }
        endpoint = schema_endpoints.get(object_type)
        if not endpoint:
            return []
        try:
            data = await self._get(endpoint)
            fields = []
            for f in data.get("fields", []):
                fields.append({
                    "name": f.get("name", f.get("label", "")),
                    "label": f.get("label", f.get("name", "")),
                    "type": f.get("field_type", "text"),
                    "required": f.get("required", False),
                    "custom": f.get("is_custom", False),
                })
            return fields
        except Exception as e:
            print(f"[Freshsales] Schema fetch failed for {object_type}: {e}")
            return []

    async def get_records(self, object_type: str, batch_size: int = 25) -> AsyncGenerator[list[dict], None]:
        """Freshsales paginates using view_id + page. Default view: all records."""
        # Use filter/view endpoints for bulk fetching
        # Freshsales default page size is 25, max around 100
        view_endpoints = {
            "contacts": "/contacts/view/1",      # view 1 = All Contacts
            "sales_accounts": "/sales_accounts/view/1",
            "deals": "/deals/view/1",
        }
        endpoint = view_endpoints.get(object_type, f"/{object_type}/view/1")
        page = 1
        while True:
            data = await self._get(endpoint, {
                "per_page": min(batch_size, 100),
                "page": page,
                "sort": "id",
                "sort_type": "asc",
            })
            # Response key matches object type
            items = data.get(object_type, [])
            if not items:
                break
            records = []
            for item in items:
                record = {"_id": str(item.get("id", "")), "_object_type": object_type}
                record.update({k: v for k, v in item.items() if k != "id"})
                records.append(record)
            yield records
            meta = data.get("meta", {})
            if not meta.get("has_next_page", False):
                break
            page += 1


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for a Freshsales record.
        Freshsales embeds associations in the record itself — we re-fetch with includes."""
        assocs = []
        try:
            if object_type in ("contacts",):
                data = await self._get(f"/contacts/{record_id}", {"include": "sales_accounts,deals"})
                contact = data.get("contact", {})
                for acct in contact.get("sales_accounts", []):
                    assocs.append({"to_object_type": "company", "to_record_id": str(acct["id"]), "relationship_type": "contact_to_account"})
                for deal in contact.get("deals", []):
                    assocs.append({"to_object_type": "deal", "to_record_id": str(deal["id"]), "relationship_type": "contact_to_deal"})
            elif object_type in ("deals",):
                data = await self._get(f"/deals/{record_id}", {"include": "contacts,sales_account"})
                deal = data.get("deal", {})
                if deal.get("sales_account_id"):
                    assocs.append({"to_object_type": "company", "to_record_id": str(deal["sales_account_id"]), "relationship_type": "deal_to_account"})
                for contact in deal.get("contacts", []):
                    assocs.append({"to_object_type": "contact", "to_record_id": str(contact["id"]), "relationship_type": "deal_to_contact"})
        except Exception as e:
            print(f"[Freshsales] Association fetch failed for {object_type}:{record_id}: {e}")
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Link records in Freshsales via record update — associations are FK-based."""
        try:
            if from_object_type in ("contacts",) and to_object_type in ("companies", "company", "accounts"):
                await self._put(f"/contacts/{from_record_id}", {
                    "contact": {"sales_accounts": [{"id": int(to_record_id), "is_primary": True}]}
                })
                return True
            if from_object_type in ("deals",) and to_object_type in ("companies", "company", "accounts"):
                await self._put(f"/deals/{from_record_id}", {"deal": {"sales_account_id": int(to_record_id)}})
                return True
            if from_object_type in ("deals",) and to_object_type in ("contacts", "contact"):
                await self._put(f"/deals/{from_record_id}/contacts/add", {"ids": [int(to_record_id)]})
                return True
        except Exception as e:
            print(f"[Freshsales] Association failed: {e}")
        return False

    async def _put(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.base_url}{path}",
                headers=self.headers, json=data
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        try:
            obj_key = {"contacts": "contact", "deals": "deal", "sales_accounts": "sales_account"}.get(object_type, object_type.rstrip("s"))
            await self._put(f"/{object_type}/{record_id}", {obj_key: data})
            return True
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        try:
            if object_type in ("contacts",) and unique_data.get("email"):
                data = await self._get(f"/filtered_search/contact", params=None)
                return None  # Requires POST filtered search — skip for now
        except Exception:
            pass
        return None

    async def get_owner_reference_map(self) -> dict:
        try:
            data = await self._get("/selector/owners")
            return {str(u["id"]): u.get("email", str(u["id"])) for u in data.get("users", [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        try:
            data = await self._get("/selector/deal_stages")
            return {str(s["id"]): s.get("name", str(s["id"])) for s in data.get("deal_stages", [])}
        except Exception:
            return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        clean = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
        # Freshsales wraps payload in singular object key
        wrappers = {
            "contacts": "contact",
            "sales_accounts": "sales_account",
            "deals": "deal",
        }
        wrapper = wrappers.get(object_type, object_type)
        result = await self._post(f"/{object_type}", {wrapper: clean})
        return str(result.get(wrapper, {}).get("id", ""))

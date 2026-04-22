import aiohttp
import asyncio
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

# ActiveCampaign API v3
# Auth: API key in header as "Api-Token"
# Base URL is account-specific: https://{account}.api-us1.com/api/3
# Rate limit: 5 requests/second standard. Bulk import: 100 requests/minute.
# Objects: contacts, deals, accounts (orgs), custom fields

AC_OBJECTS = ["contacts", "deals", "accounts"]

class ActiveCampaignConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_key = credentials.get("api_key")
        self.account_url = credentials.get("account_url", "").rstrip("/")
        # account_url format: https://youraccountname.api-us1.com
        self.base_url = f"{self.account_url}/api/3"
        self.headers = {
            "Api-Token": self.api_key,
            "Content-Type": "application/json",
        }
        self._rate_limit_interval = 0.2  # 5 req/sec

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
            result = await self._get("/contacts", {"limit": 1})
            return "contacts" in result
        except Exception:
            return False

    async def get_object_types(self) -> list[str]:
        return AC_OBJECTS

    async def get_schema(self, object_type: str) -> list[dict]:
        """ActiveCampaign has separate custom field endpoints per object type."""
        field_endpoints = {
            "contacts": "/fields",        # contact custom fields
            "deals": "/dealCustomFieldMeta",
            "accounts": "/accountCustomFieldMeta",
        }
        # Start with standard fields
        standard_fields = {
            "contacts": [
                {"name": "email", "label": "Email", "type": "email", "required": True, "custom": False},
                {"name": "firstName", "label": "First Name", "type": "string", "required": False, "custom": False},
                {"name": "lastName", "label": "Last Name", "type": "string", "required": False, "custom": False},
                {"name": "phone", "label": "Phone", "type": "string", "required": False, "custom": False},
                {"name": "orgname", "label": "Organization", "type": "string", "required": False, "custom": False},
            ],
            "deals": [
                {"name": "title", "label": "Title", "type": "string", "required": True, "custom": False},
                {"name": "value", "label": "Value", "type": "number", "required": False, "custom": False},
                {"name": "currency", "label": "Currency", "type": "string", "required": False, "custom": False},
                {"name": "stage", "label": "Stage", "type": "string", "required": False, "custom": False},
                {"name": "status", "label": "Status", "type": "number", "required": False, "custom": False},
                {"name": "percent", "label": "Probability %", "type": "number", "required": False, "custom": False},
            ],
            "accounts": [
                {"name": "name", "label": "Account Name", "type": "string", "required": True, "custom": False},
                {"name": "accountUrl", "label": "Website", "type": "string", "required": False, "custom": False},
                {"name": "phone", "label": "Phone", "type": "string", "required": False, "custom": False},
            ],
        }
        fields = list(standard_fields.get(object_type, []))
        # Fetch custom fields
        endpoint = field_endpoints.get(object_type)
        if endpoint:
            try:
                data = await self._get(endpoint, {"limit": 100})
                # Different response keys per endpoint
                custom_key = {"contacts": "fields", "deals": "dealCustomFieldMeta", "accounts": "accountCustomFieldMeta"}.get(object_type, "fields")
                for f in data.get(custom_key, []):
                    fields.append({
                        "name": f.get("id") or f.get("fieldId", ""),
                        "label": f.get("title") or f.get("fieldLabel", ""),
                        "type": f.get("type", "string"),
                        "required": False,
                        "custom": True,
                    })
            except Exception as e:
                print(f"[ActiveCampaign] Custom field fetch failed for {object_type}: {e}")
        return fields

    async def get_records(self, object_type: str, batch_size: int = 100) -> AsyncGenerator[list[dict], None]:
        endpoints = {
            "contacts": "/contacts",
            "deals": "/deals",
            "accounts": "/accounts",
        }
        endpoint = endpoints.get(object_type, f"/{object_type}")
        offset = 0
        while True:
            data = await self._get(endpoint, {
                "limit": min(batch_size, 100),
                "offset": offset,
            })
            items = data.get(object_type, [])
            if not items:
                break
            records = []
            for item in items:
                record = {"_id": str(item.get("id", "")), "_object_type": object_type}
                record.update({k: v for k, v in item.items() if k != "id"})
                records.append(record)
            yield records
            # ActiveCampaign returns meta with total count
            meta = data.get("meta", {})
            total = int(meta.get("total", 0))
            offset += len(items)
            if offset >= total or len(items) < batch_size:
                break


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch associations for an ActiveCampaign record."""
        assocs = []
        try:
            if object_type in ("contacts",):
                # Get deals associated with this contact
                data = await self._get(f"/contacts/{record_id}/contactDeals")
                for item in data.get("contactDeals", []):
                    assocs.append({
                        "to_object_type": "deal",
                        "to_record_id": str(item.get("deal", "")),
                        "relationship_type": "contact_to_deal",
                    })
                # Get account associated with contact
                contact_data = await self._get(f"/contacts/{record_id}")
                account_id = contact_data.get("contact", {}).get("accountId")
                if account_id:
                    assocs.append({
                        "to_object_type": "company",
                        "to_record_id": str(account_id),
                        "relationship_type": "contact_to_account",
                    })
            elif object_type in ("deals",):
                data = await self._get(f"/deals/{record_id}/contactDeals")
                for item in data.get("contactDeals", []):
                    assocs.append({
                        "to_object_type": "contact",
                        "to_record_id": str(item.get("contact", "")),
                        "relationship_type": "deal_to_contact",
                    })
        except Exception as e:
            print(f"[ActiveCampaign] Association fetch failed for {object_type}:{record_id}: {e}")
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Create associations in ActiveCampaign."""
        try:
            # Contact to deal association
            if from_object_type in ("contacts",) and to_object_type in ("deals", "deal"):
                payload = {"contactDeal": {"contact": from_record_id, "deal": to_record_id}}
                await self._post("/contactDeals", payload)
                return True
            # Contact to account
            if from_object_type in ("contacts",) and to_object_type in ("companies", "company", "accounts", "account"):
                await self._put(f"/contacts/{from_record_id}", {"contact": {"accountId": to_record_id}})
                return True
        except Exception as e:
            print(f"[ActiveCampaign] Association failed: {e}")
        return False

    async def _put(self, path: str, data: dict) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.base_url}{path}",
                headers={"Api-Token": self.api_key, "Content-Type": "application/json"},
                json=data
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        try:
            endpoint = {"contacts": f"/contacts/{record_id}", "deals": f"/deals/{record_id}", "accounts": f"/accounts/{record_id}"}.get(object_type)
            if not endpoint:
                return False
            obj_key = object_type.rstrip("s")
            await self._put(endpoint, {obj_key: data})
            return True
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        try:
            if object_type in ("contacts",) and unique_data.get("email"):
                data = await self._get("/contacts", {"email": unique_data["email"]})
                contacts = data.get("contacts", [])
                return str(contacts[0]["id"]) if contacts else None
        except Exception:
            pass
        return None

    async def get_owner_reference_map(self) -> dict:
        try:
            data = await self._get("/users")
            return {str(u["id"]): u.get("email", str(u["id"])) for u in data.get("users", [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        try:
            data = await self._get("/dealStages")
            return {str(s["id"]): s.get("title", str(s["id"])) for s in data.get("dealStages", [])}
        except Exception:
            return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        clean = {k: v for k, v in data.items() if not k.startswith("_") and v is not None}
        wrappers = {
            "contacts": "contact",
            "deals": "deal",
            "accounts": "account",
        }
        wrapper = wrappers.get(object_type, object_type[:-1])
        result = await self._post(f"/{object_type}", {wrapper: clean})
        return str(result.get(wrapper, {}).get("id", ""))

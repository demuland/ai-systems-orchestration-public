import asyncio
import json
import re
import aiohttp

# ─────────────────────────────────────────────────────────────────────────────
# FieldProvisioner
#
# Runs BEFORE the migration executor. For every approved field mapping where
# the destination field does not yet exist in the destination platform, this
# class creates it automatically via the platform's API.
#
# After creating each field, it updates the field_mappings row in the database
# so the executor uses the correct destination field key.
#
# Platform support:
#   HubSpot      POST /crm/v3/properties/{objectType}
#   Pipedrive    POST /v1/{objectType}Fields
#   Zoho CRM     POST /crm/v3/settings/fields?module={Module}
#   Monday.com   GraphQL mutation create_column
#   ActiveCampaign POST /api/3/fields (contacts), /api/3/dealCustomFieldMeta (deals)
#   Freshsales   NO API for field creation — flags fields for manual creation
#   Copper       POST /developer_api/v1/custom_field_definitions
# ─────────────────────────────────────────────────────────────────────────────

# ── Type mapping tables ──────────────────────────────────────────────────────

# Map generic internal types → HubSpot (type, fieldType) pairs
HUBSPOT_TYPE_MAP = {
    "string":   ("string", "text"),
    "text":     ("string", "textarea"),
    "number":   ("number", "number"),
    "integer":  ("number", "number"),
    "float":    ("number", "number"),
    "double":   ("number", "number"),
    "date":     ("date", "date"),
    "datetime": ("datetime", "date"),
    "bool":     ("bool", "booleancheckbox"),
    "boolean":  ("bool", "booleancheckbox"),
    "enum":     ("enumeration", "select"),
    "select":   ("enumeration", "select"),
    "dropdown": ("enumeration", "select"),
    "phone":    ("string", "phonenumber"),
    "url":      ("string", "text"),
    "currency": ("number", "number"),
}

# Map generic types → Pipedrive field_type values
PIPEDRIVE_TYPE_MAP = {
    "string":   "varchar",
    "text":     "text",
    "number":   "double",
    "integer":  "double",
    "float":    "double",
    "double":   "double",
    "date":     "date",
    "datetime": "date",
    "bool":     "yesno",
    "boolean":  "yesno",
    "enum":     "enum",
    "select":   "enum",
    "dropdown": "enum",
    "phone":    "phone",
    "url":      "varchar",
    "currency": "monetary",
    "set":      "set",
}

# Map generic types → Zoho CRM data_type values
ZOHO_TYPE_MAP = {
    "string":   "text",
    "text":     "textarea",
    "number":   "double",
    "integer":  "integer",
    "float":    "double",
    "double":   "double",
    "date":     "date",
    "datetime": "datetime",
    "bool":     "boolean",
    "boolean":  "boolean",
    "enum":     "picklist",
    "select":   "picklist",
    "dropdown": "picklist",
    "phone":    "phone",
    "url":      "website",
    "currency": "currency",
}

# Map generic types → Monday column types
MONDAY_TYPE_MAP = {
    "string":   "text",
    "text":     "long_text",
    "number":   "numbers",
    "integer":  "numbers",
    "float":    "numbers",
    "double":   "numbers",
    "date":     "date",
    "datetime": "date",
    "bool":     "checkbox",
    "boolean":  "checkbox",
    "enum":     "dropdown",
    "select":   "dropdown",
    "dropdown": "dropdown",
    "phone":    "phone",
    "url":      "link",
    "currency": "numbers",
}

# Map generic types → ActiveCampaign field type values
ACTIVECAMPAIGN_TYPE_MAP = {
    "string":   "text",
    "text":     "textarea",
    "number":   "number",
    "integer":  "number",
    "float":    "number",
    "double":   "number",
    "date":     "date",
    "datetime": "datetime",
    "bool":     "checkbox",
    "boolean":  "checkbox",
    "enum":     "dropdown",
    "select":   "dropdown",
    "dropdown": "dropdown",
    "phone":    "text",
    "url":      "text",
    "currency": "number",
}

# Map generic types → Copper data_type values
COPPER_TYPE_MAP = {
    "string":   "String",
    "text":     "Text",
    "number":   "Float",
    "integer":  "Float",
    "float":    "Float",
    "double":   "Float",
    "date":     "Date",
    "datetime": "Date",
    "bool":     "Checkbox",
    "boolean":  "Checkbox",
    "enum":     "Dropdown",
    "select":   "Dropdown",
    "dropdown": "Dropdown",
    "phone":    "String",
    "url":      "URL",
    "currency": "Currency",
}

def _slugify(name: str) -> str:
    """Convert a human-readable label to a safe API field name."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug or "custom_field"


class FieldProvisioner:

    def __init__(self, dest_platform: str, dest_credentials: dict, db):
        self.platform = dest_platform
        self.creds = dest_credentials
        self.db = db

    async def provision(self, migration_id: str, preview_only: bool = False) -> dict:
        """
        Main entry point. Reads all accepted mappings where dest_field is null
        or appears to be a placeholder that doesn't exist yet, then creates
        those fields in the destination platform.

        Returns a summary: {created: [...], skipped: [...], failed: [...], manual: [...]}
        """
        # Pull approved mappings where the effective destination field is still empty.
        unmapped = self.db.execute(
            """SELECT id, object_type, dest_object_type, source_field, source_type,
                      dest_field, operator_override, dest_type, llm_reasoning
               FROM field_mappings
               WHERE migration_id = ?
               AND operator_action IN ('accepted', 'corrected')
               AND (COALESCE(operator_override, dest_field, '') = '')""",
            (migration_id,)
        ).fetchall()

        if not unmapped:
            return {"created": [], "skipped": [], "failed": [], "manual": []}

        unmapped = [dict(r) for r in unmapped]
        summary = {"created": [], "skipped": [], "failed": [], "manual": []}

        for mapping in unmapped:
            field_name = mapping["source_field"]
            field_label = field_name.replace("_", " ").title()
            field_type = mapping.get("source_type", "string") or "string"
            object_type = mapping.get("dest_object_type") or mapping["object_type"]
            mapping_id = mapping["id"]

            if preview_only:
                if self.platform == "freshsales":
                    summary["manual"].append({
                        "field": field_name,
                        "object_type": object_type,
                        "message": "Freshsales does not support custom field creation via API. Please create this field manually in Freshsales Admin Settings, then re-run mapping."
                    })
                else:
                    summary["created"].append({"field": field_name, "object_type": object_type, "preview": True})
                continue

            try:
                if self.platform == "hubspot":
                    new_key = await self._create_hubspot_field(field_name, field_label, field_type, object_type)
                elif self.platform == "pipedrive":
                    new_key = await self._create_pipedrive_field(field_name, field_label, field_type, object_type)
                elif self.platform == "zoho":
                    new_key = await self._create_zoho_field(field_name, field_label, field_type, object_type)
                elif self.platform == "monday":
                    new_key = await self._create_monday_field(field_name, field_label, field_type, object_type)
                elif self.platform == "activecampaign":
                    new_key = await self._create_activecampaign_field(field_name, field_label, field_type, object_type)
                elif self.platform == "freshsales":
                    # Freshsales has no public API for field creation
                    summary["manual"].append({
                        "field": field_name,
                        "object_type": object_type,
                        "message": "Freshsales does not support custom field creation via API. Please create this field manually in Freshsales Admin Settings, then re-run mapping."
                    })
                    continue
                elif self.platform == "copper":
                    new_key = await self._create_copper_field(field_name, field_label, field_type, object_type)
                else:
                    summary["skipped"].append({"field": field_name, "reason": f"Platform {self.platform} not supported for field creation"})
                    continue

                # Update mapping row with the newly created field key
                self.db.execute(
                    "UPDATE field_mappings SET dest_field = ?, dest_type = ? WHERE id = ?",
                    (new_key, field_type, mapping_id)
                )
                self.db.commit()
                summary["created"].append({"field": field_name, "dest_key": new_key, "object_type": object_type})

            except Exception as e:
                summary["failed"].append({
                    "field": field_name,
                    "object_type": object_type,
                    "error": str(e)
                })

        return summary

    # ── HubSpot ──────────────────────────────────────────────────────────────
    async def _create_hubspot_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        token = self.creds.get("api_key") or self.creds.get("access_token")
        hs_type, hs_field_type = HUBSPOT_TYPE_MAP.get(field_type.lower(), ("string", "text"))
        slug = _slugify(name)

        # HubSpot requires a groupName — use the default group for each object type
        GROUP_NAMES = {
            "contacts": "contactinformation",
            "companies": "companyinformation",
            "deals": "dealinformation",
        }
        group = GROUP_NAMES.get(object_type, "contactinformation")

        payload = {
            "name": slug,
            "label": label,
            "type": hs_type,
            "fieldType": hs_field_type,
            "groupName": group,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.hubapi.com/crm/v3/properties/{object_type}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload
            ) as resp:
                if resp.status == 409:
                    # Field already exists — return the slug as the key
                    return slug
                resp.raise_for_status()
                data = await resp.json()
                return data.get("name", slug)

    # ── Pipedrive ─────────────────────────────────────────────────────────────
    async def _create_pipedrive_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        token = self.creds.get("api_token")
        pd_type = PIPEDRIVE_TYPE_MAP.get(field_type.lower(), "varchar")

        # Pipedrive field endpoints per object type
        FIELD_ENDPOINTS = {
            "persons": "/v1/personFields",
            "organizations": "/v1/organizationFields",
            "deals": "/v1/dealFields",
            "contacts": "/v1/personFields",
            "companies": "/v1/organizationFields",
        }
        endpoint = FIELD_ENDPOINTS.get(object_type, "/v1/personFields")

        payload = {"name": label, "field_type": pd_type}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.pipedrive.com{endpoint}",
                params={"api_token": token},
                json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                # Pipedrive returns a 40-char hash as the field key
                return data["data"]["key"]

    # ── Zoho CRM ──────────────────────────────────────────────────────────────
    async def _create_zoho_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        token = self.creds.get("access_token")
        zoho_type = ZOHO_TYPE_MAP.get(field_type.lower(), "text")

        # Zoho module names (capitalized)
        MODULE_MAP = {
            "leads": "Leads",
            "contacts": "Contacts",
            "accounts": "Accounts",
            "deals": "Deals",
        }
        module = MODULE_MAP.get(object_type, "Contacts")
        api_name = _slugify(name) + "_axm"  # suffix to avoid collisions

        payload = {
            "fields": [{
                "field_label": label,
                "data_type": zoho_type,
                "api_name": api_name,
            }]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://www.zohoapis.com/crm/v3/settings/fields",
                params={"module": module},
                headers={
                    "Authorization": f"Zoho-oauthtoken {token}",
                    "Content-Type": "application/json"
                },
                json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                created = data.get("fields", [{}])[0]
                return created.get("api_name", api_name)

    # ── Monday.com ────────────────────────────────────────────────────────────
    async def _create_monday_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        token = self.creds.get("api_token")
        monday_type = MONDAY_TYPE_MAP.get(field_type.lower(), "text")

        # Monday needs a board_id — stored in creds or object_type maps to a board
        board_id = self.creds.get("board_id") or self.creds.get(f"{object_type}_board_id")
        if not board_id:
            raise ValueError("Monday.com board_id not found in credentials. Add board_id to the destination credentials.")

        query = """
        mutation ($boardId: ID!, $title: String!, $columnType: ColumnType!) {
            create_column(board_id: $boardId, title: $title, column_type: $columnType) {
                id
                title
            }
        }
        """
        variables = {"boardId": board_id, "title": label, "columnType": monday_type}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.monday.com/v2",
                headers={"Authorization": token, "Content-Type": "application/json", "API-Version": "2024-10"},
                json={"query": query, "variables": variables}
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                col = data.get("data", {}).get("create_column", {})
                return col.get("id", _slugify(name))

    # ── ActiveCampaign ────────────────────────────────────────────────────────
    async def _create_activecampaign_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        api_key = self.creds.get("api_key")
        base_url = (self.creds.get("account_url") or self.creds.get("base_url") or "").rstrip("/")
        ac_type = ACTIVECAMPAIGN_TYPE_MAP.get(field_type.lower(), "text")

        headers = {
            "Api-Token": api_key,
            "Content-Type": "application/json"
        }

        if object_type in ("contacts",):
            # Contact custom field
            payload = {"field": {"type": ac_type, "title": label, "visible": 1}}
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/3/fields",
                    headers=headers,
                    json=payload
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    field_id = data["field"]["id"]
                    return f"field[%{field_id}%,0]"  # AC contact field reference format

        elif object_type in ("deals",):
            # Deal custom field
            payload = {"dealCustomFieldMeta": {"fieldLabel": label, "fieldType": ac_type, "isRequired": 0}}
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/3/dealCustomFieldMeta",
                    headers=headers,
                    json=payload
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    field_id = data["dealCustomFieldMeta"]["id"]
                    return str(field_id)
        else:
            raise ValueError(f"ActiveCampaign: unsupported object type for field creation: {object_type}")

    # ── Copper ────────────────────────────────────────────────────────────────
    async def _create_copper_field(self, name: str, label: str, field_type: str, object_type: str) -> str:
        api_key = self.creds.get("api_token") or self.creds.get("api_key")
        user_email = self.creds.get("user_email")
        copper_type = COPPER_TYPE_MAP.get(field_type.lower(), "String")

        # Copper available_on values per object type
        COPPER_ENTITY_MAP = {
            "leads": "lead",
            "persons": "person",
            "people": "person",
            "contacts": "person",
            "companies": "company",
            "organizations": "company",
            "deals": "opportunity",
            "opportunities": "opportunity",
        }
        entity = COPPER_ENTITY_MAP.get(object_type, "person")

        payload = {
            "name": label,
            "data_type": copper_type,
            "available_on": [entity]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.copper.com/developer_api/v1/custom_field_definitions",
                headers={
                    "X-PW-AccessToken": api_key,
                    "X-PW-Application": "developer_api",
                    "X-PW-UserEmail": user_email or "",
                    "Content-Type": "application/json"
                },
                json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return str(data["id"])

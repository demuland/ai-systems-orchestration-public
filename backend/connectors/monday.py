import aiohttp
import asyncio
import json
from typing import AsyncGenerator
from connectors.base import BaseConnector, retry_request

# Monday.com GraphQL API v2
# All requests are POST to https://api.monday.com/v2
# Auth: Bearer token in Authorization header
# Rate limit: complexity budget based — practical limit ~100 complex queries/minute
# API version: 2025-04 (current stable as of early 2026)

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_VERSION = "2026-01"

# Monday doesn't have traditional CRM objects — it has boards with items
# We treat each board as an "object type" and items as records
# For schema discovery we discover all boards and their column definitions

class MondayConnector(BaseConnector):

    def __init__(self, credentials: dict):
        self.api_token = credentials.get("api_token")
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "API-Version": MONDAY_API_VERSION,
        }
        self._rate_limit_interval = 0.2  # conservative for complexity budget
        self._boards: list[dict] = []  # cached board list
        self._canonical_board_ids = {
            "contacts": str(credentials.get("contacts_board_id", "")).strip(),
            "companies": str(credentials.get("companies_board_id", "")).strip(),
            "deals": str(credentials.get("deals_board_id", "")).strip(),
        }

    def _resolve_board_id(self, object_type: str) -> str:
        canonical = object_type.lower()
        if canonical.startswith("board_"):
            return canonical.replace("board_", "", 1)
        board_id = self._canonical_board_ids.get(canonical)
        if board_id:
            return board_id
        return object_type.replace("board_", "")

    async def _query(self, query: str, variables: dict = None) -> dict:
        await asyncio.sleep(self._rate_limit_interval)
        body = {"query": query}
        if variables:
            body["variables"] = variables
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.post(MONDAY_API_URL, headers=self.headers, json=body) as resp:
                    resp.raise_for_status()
                    result = await resp.json()
                    if "errors" in result:
                        error_msg = result["errors"][0].get("message", "GraphQL error")
                        raise Exception(f"Monday API GraphQL error: {error_msg}")
                    return result.get("data", {})
        return await retry_request(_do)

    async def test_connection(self) -> bool:
        try:
            result = await self._query("{ me { id name } }")
            return "me" in result
        except Exception:
            return False

    async def _get_boards(self) -> list[dict]:
        if self._boards:
            return self._boards
        result = await self._query("""
            {
                boards(limit: 100, state: active) {
                    id
                    name
                    description
                    board_kind
                    columns {
                        id
                        title
                        type
                    }
                }
            }
        """)
        self._boards = result.get("boards", [])
        return self._boards

    async def get_object_types(self) -> list[str]:
        """Prefer explicit CRM board mappings; otherwise expose raw board IDs."""
        configured = [name for name, board_id in self._canonical_board_ids.items() if board_id]
        if configured:
            return configured
        boards = await self._get_boards()
        return [f"board_{b['id']}" for b in boards]

    async def get_schema(self, object_type: str) -> list[dict]:
        """Columns on a board are the schema fields."""
        board_id = self._resolve_board_id(object_type)
        boards = await self._get_boards()
        board = next((b for b in boards if str(b["id"]) == board_id), None)
        if not board:
            return []
        fields = []
        for col in board.get("columns", []):
            fields.append({
                "name": col["id"],
                "label": col["title"],
                "type": col["type"],
                "required": False,
                "custom": col["type"] not in ["name", "people", "status", "date"],
            })
        return fields

    async def get_records(self, object_type: str, batch_size: int = 100) -> AsyncGenerator[list[dict], None]:
        board_id = self._resolve_board_id(object_type)
        cursor = None
        while True:
            if cursor:
                query = """
                    query ($board_id: ID!, $limit: Int!, $cursor: String!) {
                        boards(ids: [$board_id]) {
                            items_page(limit: $limit, cursor: $cursor) {
                                cursor
                                items {
                                    id
                                    name
                                    column_values {
                                        id
                                        text
                                        value
                                        type
                                        ... on TextValue { text }
                                        ... on StatusValue { label }
                                        ... on DateValue { date }
                                        ... on NumbersValue { number }
                                        ... on PeopleValue { persons_and_teams { id kind } }
                                    }
                                }
                            }
                        }
                    }
                """
                variables = {"board_id": board_id, "limit": batch_size, "cursor": cursor}
            else:
                query = """
                    query ($board_id: ID!, $limit: Int!) {
                        boards(ids: [$board_id]) {
                            items_page(limit: $limit) {
                                cursor
                                items {
                                    id
                                    name
                                    column_values {
                                        id
                                        text
                                        value
                                        type
                                    }
                                }
                            }
                        }
                    }
                """
                variables = {"board_id": board_id, "limit": batch_size}

            data = await self._query(query, variables)
            boards_data = data.get("boards", [])
            if not boards_data:
                break
            items_page = boards_data[0].get("items_page", {})
            items = items_page.get("items", [])
            if not items:
                break

            records = []
            for item in items:
                record = {
                    "_id": str(item["id"]),
                    "_object_type": object_type,
                    "name": item["name"],
                }
                for col_val in item.get("column_values", []):
                    # Prefer text representation for portability
                    record[col_val["id"]] = col_val.get("text") or col_val.get("value")
                records.append(record)
            yield records

            cursor = items_page.get("cursor")
            if not cursor:
                break


    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """Fetch Connect Boards column values for a Monday item to find linked items."""
        assocs = []
        try:
            query = """
            query ($itemId: ID!) {
                items(ids: [$itemId]) {
                    column_values {
                        id
                        type
                        ... on BoardRelationValue {
                            linked_item_ids
                            linked_items {
                                id
                                board { id }
                            }
                        }
                    }
                }
            }
            """
            result = await self._query(query, {"itemId": record_id})
            items = result.get("items", [])
            for item in items:
                for col in item.get("column_values", []):
                    if col.get("type") == "board_relation":
                        for linked in col.get("linked_items", []):
                            assocs.append({
                                "to_object_type": "item",
                                "to_record_id": str(linked["id"]),
                                "relationship_type": f"board_relation_{col['id']}",
                            })
        except Exception as e:
            print(f"[Monday] Association fetch failed for {record_id}: {e}")
        return assocs

    async def create_association(self, from_object_type: str, from_record_id: str,
                                  to_object_type: str, to_record_id: str,
                                  relationship_type: str = None) -> bool:
        """Link Monday items via a Connect Boards column."""
        try:
            # Extract column_id from relationship_type if available
            column_id = "connect_boards"
            if relationship_type and relationship_type.startswith("board_relation_"):
                column_id = relationship_type.replace("board_relation_", "")

            mutation = """
            mutation ($itemId: ID!, $columnId: String!, $value: JSON!) {
                change_column_value(item_id: $itemId, column_id: $columnId, value: $value) {
                    id
                }
            }
            """
            value = json.dumps({"item_ids": [int(to_record_id)]})
            result = await self._query(mutation, {
                "itemId": from_record_id,
                "columnId": column_id,
                "value": value,
            })
            return True
        except Exception as e:
            print(f"[Monday] Association creation failed: {e}")
            return False

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        """Update Monday item column values."""
        try:
            for column_id, value in data.items():
                mutation = """
                mutation ($itemId: ID!, $columnId: String!, $value: JSON!) {
                    change_column_value(item_id: $itemId, column_id: $columnId, value: $value) { id }
                }
                """
                await self._query(mutation, {
                    "itemId": record_id,
                    "columnId": column_id,
                    "value": json.dumps(value) if not isinstance(value, str) else value,
                })
            return True
        except Exception:
            return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> str:
        """Search Monday items by exact item name on the given board."""
        try:
            name = unique_data.get("name") or unique_data.get("title")
            if not name:
                return None
            board_id = self._resolve_board_id(object_type)
            query = """
            query ($boardId: ID!, $limit: Int!) {
                boards(ids: [$boardId]) {
                    items_page(limit: $limit) {
                        items { id name }
                    }
                }
            }
            """
            result = await self._query(query, {"boardId": board_id, "limit": 100})
            boards = result.get("boards", [])
            if not boards:
                return None
            items = boards[0].get("items_page", {}).get("items", [])
            for item in items:
                if (item.get("name") or "").strip().lower() == str(name).strip().lower():
                    return str(item.get("id"))
            return None
        except Exception:
            return None

    async def get_owner_reference_map(self) -> dict:
        try:
            query = "query { users { id email name } }"
            result = await self._query(query, {})
            return {str(u["id"]): u.get("email", str(u["id"])) for u in result.get("users", [])}
        except Exception:
            return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        # Monday uses status columns not pipeline stages
        return {}

    async def create_record(self, object_type: str, data: dict) -> str:
        board_id = self._resolve_board_id(object_type)
        item_name = data.get("name", "Imported Item")
        # Build column_values JSON string from remaining fields
        import json
        col_values = {k: str(v) for k, v in data.items()
                      if not k.startswith("_") and k != "name" and v is not None}
        mutation = """
            mutation ($board_id: ID!, $item_name: String!, $col_values: JSON!) {
                create_item(board_id: $board_id, item_name: $item_name, column_values: $col_values) {
                    id
                }
            }
        """
        result = await self._query(mutation, {
            "board_id": board_id,
            "item_name": item_name,
            "col_values": json.dumps(col_values)
        })
        return str(result["create_item"]["id"])

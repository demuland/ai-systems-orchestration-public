from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional, Callable
import asyncio
import random


async def retry_request(fn: Callable, max_retries: int = 5, base_delay: float = 2.0):
    """
    Execute an async callable with exponential backoff + jitter on 429/5xx.
    Respects Retry-After header when present. Raises on final failure.
    """
    import aiohttp
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except aiohttp.ClientResponseError as e:
            last_exc = e
            if e.status == 429 or e.status >= 500:
                if attempt == max_retries:
                    raise
                retry_after = None
                if hasattr(e, "headers") and e.headers:
                    try:
                        retry_after = float(e.headers.get("Retry-After", 0))
                    except (ValueError, TypeError):
                        retry_after = None
                delay = retry_after if retry_after else (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                await asyncio.sleep(delay)
            else:
                raise
        except Exception as e:
            raise
    raise last_exc

class BaseConnector(ABC):

    @abstractmethod
    async def test_connection(self) -> bool:
        pass

    @abstractmethod
    async def get_schema(self, object_type: str) -> list[dict]:
        """Return list of {name, type, label, required} dicts."""
        pass

    @abstractmethod
    async def get_object_types(self) -> list[str]:
        pass

    @abstractmethod
    async def get_records(self, object_type: str, batch_size: int = 100) -> AsyncGenerator[list[dict], None]:
        """
        Yield batches of records. Each record must include:
          _id           — source record ID
          _object_type  — object type string
          _associations — list of {to_object_type, to_record_id, relationship_type}
                          extracted inline during fetch (optional but preferred)
        """
        pass

    @abstractmethod
    async def create_record(self, object_type: str, data: dict) -> str:
        """Create a record in destination. Returns new record ID."""
        pass

    # ── Relationship methods ──────────────────────────────────────────────────

    async def get_record_associations(self, object_type: str, record_id: str) -> list[dict]:
        """
        Return associations for a single record.
        Each item: {to_object_type, to_record_id, relationship_type}
        Default: return empty list (connectors override where supported).
        """
        return []

    async def create_association(
        self,
        from_object_type: str,
        from_record_id: str,
        to_object_type: str,
        to_record_id: str,
        relationship_type: Optional[str] = None,
    ) -> bool:
        """
        Create an association between two records in the destination.
        Returns True on success, False if not supported.
        Default: no-op returning False (connectors override where supported).
        """
        return False

    async def update_record(self, object_type: str, record_id: str, data: dict) -> bool:
        """
        Update an existing record. Used for post-create reference remapping.
        Returns True on success.
        Default: no-op returning False.
        """
        return False

    async def find_existing_record(self, object_type: str, unique_data: dict) -> Optional[str]:
        """
        Look up an existing record by unique fields (e.g. email for contacts).
        Returns destination record ID if found, None otherwise.
        Used for idempotent migration — prevents duplicate creation on retry.
        Default: returns None (no dedup).
        """
        return None

    async def get_owner_reference_map(self) -> dict:
        """
        Return {owner_name_or_email: owner_id} for all users in this platform.
        Used to remap owner references during migration.
        Default: empty dict.
        """
        return {}

    async def get_stage_reference_map(self, pipeline_id: str = None) -> dict:
        """
        Return {stage_name: stage_id} for all pipeline stages.
        Used to remap stage references during migration.
        Default: empty dict.
        """
        return {}

from pydantic import BaseModel
from typing import Optional

class MigrationCreate(BaseModel):
    client_name: str
    source_platform: str
    dest_platform: str
    source_credentials: dict
    dest_credentials: dict

class MigrationResponse(BaseModel):
    id: str
    client_name: str
    source_platform: str
    dest_platform: str
    status: str

class MappingApproval(BaseModel):
    action: str  # "accepted" | "corrected" | "skipped"
    override_dest_field: Optional[str] = None

class ExecutionStart(BaseModel):
    dry_run: bool = False

class MigrationStatus(BaseModel):
    status: str
    total_records: int
    migrated_records: int
    failed_records: int

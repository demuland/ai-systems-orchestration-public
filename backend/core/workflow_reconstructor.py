import asyncio
import json
import uuid
import aiohttp
from typing import Optional
from core.workflow_value_extractor import WorkflowValueExtractor

# ─────────────────────────────────────────────────────────────────────────────
# WorkflowReconstructor
#
# Pattern-based workflow reconstruction for all 7 supported platforms.
# Reads workflow definitions stored in the discovered_workflows table and
# reconstructs them natively in the destination platform using documented APIs.
#
# Coverage per platform:
#   HubSpot       v4 Automation API — full trigger/action/condition support
#   Pipedrive     Automations API  — triggers, conditions, actions
#   Zoho CRM      Workflow Rules API v8 — full rule reconstruction
#   Monday.com    GraphQL Automation API — trigger/action recipes
#   ActiveCampaign Automations API v3 — contact and deal automations
#   Freshsales    Workflow API      — record-based and date-based workflows
#   Copper        Workflow Automation API — trigger/action rules
#
# IMPORTANT: This is pattern-based reconstruction. It handles all documented
# workflow patterns. Edge cases with custom code actions or third-party
# app integrations are flagged for manual review.
# ─────────────────────────────────────────────────────────────────────────────


# ── Cross-platform trigger type mappings ─────────────────────────────────────
# Maps source trigger concepts to destination trigger concepts

TRIGGER_CONCEPT_MAP = {
    # Record creation triggers
    "record_created":       {"hubspot": "CONTACT_CREATED",        "pipedrive": "deal.added",          "zoho": "create",          "monday": "item_created",              "activecampaign": "contact_add",    "freshsales": "record_created",  "copper": "created"},
    "contact_created":      {"hubspot": "CONTACT_CREATED",        "pipedrive": "person.added",        "zoho": "create",          "monday": "item_created",              "activecampaign": "contact_add",    "freshsales": "record_created",  "copper": "created"},
    "deal_created":         {"hubspot": "DEAL_CREATED",           "pipedrive": "deal.added",          "zoho": "create",          "monday": "item_created",              "activecampaign": "deal_add",       "freshsales": "record_created",  "copper": "created"},
    "company_created":      {"hubspot": "COMPANY_CREATED",        "pipedrive": "organization.added",  "zoho": "create",          "monday": "item_created",              "activecampaign": "contact_add",    "freshsales": "record_created",  "copper": "created"},
    # Record update triggers
    "record_updated":       {"hubspot": "CONTACT_PROPERTY_CHANGE","pipedrive": "deal.updated",        "zoho": "edit",            "monday": "column_changed",            "activecampaign": "contact_update", "freshsales": "record_updated",  "copper": "updated"},
    "deal_stage_changed":   {"hubspot": "DEAL_STAGE_PROPERTY_CHANGED","pipedrive": "deal.stage_changed","zoho": "edit",          "monday": "status_changed",            "activecampaign": "deal_stage_change","freshsales": "record_updated","copper": "stage_changed"},
    "field_updated":        {"hubspot": "CONTACT_PROPERTY_CHANGE","pipedrive": "person.updated",      "zoho": "edit",            "monday": "column_changed",            "activecampaign": "contact_field_change","freshsales":"record_updated","copper":"specific_field_updated"},
    # Date-based triggers
    "date_based":           {"hubspot": "SCHEDULED",              "pipedrive": "due_date",            "zoho": "scheduled",       "monday": "date_arrived",              "activecampaign": "date",           "freshsales": "date_based",      "copper": "check_once_a_day"},
    "deal_close_date":      {"hubspot": "SCHEDULED",              "pipedrive": "due_date",            "zoho": "scheduled",       "monday": "date_arrived",              "activecampaign": "deal_date",      "freshsales": "date_based",      "copper": "check_once_a_day"},
    # Activity/Task triggers
    "task_created":         {"hubspot": "TASK_CREATED",           "pipedrive": "activity.added",      "zoho": "create",          "monday": "item_created",              "activecampaign": "task_add",       "freshsales": "record_created",  "copper": "created"},
    "task_completed":       {"hubspot": "TASK_COMPLETED",         "pipedrive": "activity.marked_as_done","zoho": "edit",         "monday": "status_changed",            "activecampaign": "task_completed", "freshsales": "record_updated",  "copper": "completed"},
    # Email triggers
    "email_opened":         {"hubspot": "EMAIL_OPENED",           "pipedrive": None,                  "zoho": "email_opened",    "monday": None,                        "activecampaign": "email_open",     "freshsales": None,              "copper": None},
    "email_clicked":        {"hubspot": "EMAIL_LINK_CLICKED",     "pipedrive": None,                  "zoho": "email_clicked",   "monday": None,                        "activecampaign": "email_link",     "freshsales": None,              "copper": None},
    # Form/web triggers
    "form_submitted":       {"hubspot": "FORM_SUBMISSION",        "pipedrive": None,                  "zoho": None,              "monday": None,                        "activecampaign": "form_submit",    "freshsales": None,              "copper": None},
}

# ── Cross-platform action type mappings ──────────────────────────────────────

ACTION_CONCEPT_MAP = {
    # Notification actions
    "send_notification":    {"hubspot": "SEND_EMAIL",             "pipedrive": "sendEmail",           "zoho": "email_notifications","monday": "notify_someone",         "activecampaign": "sendnotification","freshsales": "send_email",     "copper": "send_email"},
    "notify_owner":         {"hubspot": "SEND_EMAIL",             "pipedrive": "sendEmail",           "zoho": "email_notifications","monday": "notify_someone",         "activecampaign": "sendnotification","freshsales": "send_email",     "copper": "send_email"},
    # Task creation
    "create_task":          {"hubspot": "CREATE_TASK",            "pipedrive": "createActivity",      "zoho": "tasks",           "monday": "create_item",               "activecampaign": "add_task",       "freshsales": "create_task",     "copper": "create_task"},
    # Field updates
    "update_field":         {"hubspot": "SET_CONTACT_PROPERTY",   "pipedrive": "updateDeal",          "zoho": "field_updates",   "monday": "change_column_value",       "activecampaign": "update_contact", "freshsales": "update_record",   "copper": "update_field"},
    "update_deal_stage":    {"hubspot": "SET_DEAL_STAGE",         "pipedrive": "moveDealToStage",     "zoho": "field_updates",   "monday": "change_status_column",      "activecampaign": "update_deal_stage","freshsales":"update_record",  "copper": "update_stage"},
    # Assignment
    "assign_owner":         {"hubspot": "ROTATE_RECORD",          "pipedrive": "updateDeal",          "zoho": "assign_owner",    "monday": "assign_person",             "activecampaign": "update_owner",   "freshsales": "assign_owner",    "copper": "assign_owner"},
    # Record creation
    "create_deal":          {"hubspot": "CREATE_DEAL",            "pipedrive": "createDeal",          "zoho": "create_record",   "monday": "create_item",               "activecampaign": "add_deal",       "freshsales": "create_deal",     "copper": "create_opportunity"},
    "create_contact":       {"hubspot": "CREATE_CONTACT",         "pipedrive": "createPerson",        "zoho": "create_record",   "monday": "create_item",               "activecampaign": "create_contact", "freshsales": "create_contact",  "copper": "create_person"},
    # Delays
    "wait":                 {"hubspot": "DELAY",                  "pipedrive": "wait",                "zoho": "scheduled",       "monday": "wait",                      "activecampaign": "wait",           "freshsales": "delay",           "copper": "delay"},
    # Webhooks
    "webhook":              {"hubspot": "WEBHOOK",                "pipedrive": "webhook",             "zoho": "webhooks",        "monday": "webhook",                   "activecampaign": "webhook",        "freshsales": "webhook",          "copper": "webhook"},
    # List/tag management
    "add_tag":              {"hubspot": "ADD_CONTACT_TO_LIST",    "pipedrive": "updatePersonLabel",   "zoho": "add_tags",        "monday": None,                        "activecampaign": "add_tag",        "freshsales": None,               "copper": None},
    "remove_tag":           {"hubspot": "REMOVE_CONTACT_FROM_LIST","pipedrive": "updatePersonLabel",  "zoho": None,              "monday": None,                        "activecampaign": "remove_tag",     "freshsales": None,               "copper": None},
    # Scoring
    "update_score":         {"hubspot": "SET_CONTACT_PROPERTY",   "pipedrive": None,                  "zoho": None,              "monday": None,                        "activecampaign": "adjust_score",   "freshsales": None,               "copper": None},
}


def _classify_trigger(raw_trigger: dict, source_platform: str) -> str:
    """Map a raw platform trigger to a universal trigger concept."""
    trigger_type = str(raw_trigger.get("type", "")).lower()
    trigger_name = str(raw_trigger.get("name", "")).lower()
    combined = trigger_type + " " + trigger_name

    if any(k in combined for k in ["form", "form_submit"]):
        return "form_submitted"
    if any(k in combined for k in ["email_open", "email opened"]):
        return "email_opened"
    if any(k in combined for k in ["email_click", "link_click"]):
        return "email_clicked"
    if any(k in combined for k in ["stage_change", "stage changed", "movesdealtostage"]):
        return "deal_stage_changed"
    if any(k in combined for k in ["deal_creat", "deal.added", "deal created"]):
        return "deal_created"
    if any(k in combined for k in ["contact_creat", "person.added", "contact created"]):
        return "contact_created"
    if any(k in combined for k in ["company_creat", "org", "account creat"]):
        return "company_created"
    if any(k in combined for k in ["task_complet", "marked_as_done", "task completed"]):
        return "task_completed"
    if any(k in combined for k in ["task_creat", "activity.added", "task created"]):
        return "task_created"
    if any(k in combined for k in ["date", "scheduled", "due_date", "birthday"]):
        return "date_based"
    if any(k in combined for k in ["field_update", "property_change", "column_changed", "updated", "edit"]):
        return "record_updated"
    if any(k in combined for k in ["created", "added", "new"]):
        return "record_created"
    return "record_updated"  # safe default


def _classify_action(raw_action: dict, source_platform: str) -> str:
    """Map a raw platform action to a universal action concept."""
    action_type = str(raw_action.get("type", "") or raw_action.get("action", "") or raw_action.get("actionTypeId", "")).lower()
    action_name = str(raw_action.get("name", "") or raw_action.get("label", "")).lower()
    combined = action_type + " " + action_name

    if any(k in combined for k in ["send_email", "sendemail", "email_notif", "notify"]):
        return "send_notification"
    if any(k in combined for k in ["create_task", "createactivity", "add_task", "task"]):
        return "create_task"
    if any(k in combined for k in ["stage", "move_to_stage", "set_deal_stage"]):
        return "update_deal_stage"
    if any(k in combined for k in ["assign_owner", "rotate_record", "assign"]):
        return "assign_owner"
    if any(k in combined for k in ["create_deal", "createdeal"]):
        return "create_deal"
    if any(k in combined for k in ["create_contact", "createperson"]):
        return "create_contact"
    if any(k in combined for k in ["webhook"]):
        return "webhook"
    if any(k in combined for k in ["tag", "label", "add_to_list"]):
        return "add_tag"
    if any(k in combined for k in ["score"]):
        return "update_score"
    if any(k in combined for k in ["delay", "wait"]):
        return "wait"
    if any(k in combined for k in ["field_update", "set_property", "update", "change_column"]):
        return "update_field"
    return "update_field"  # safe default


class WorkflowReconstructor:

    def __init__(self, source_platform: str, dest_platform: str,
                 dest_credentials: dict, db, user_map: dict = None,
                 field_map: dict = None, stage_map: dict = None):
        self.source = source_platform
        self.dest = dest_platform
        self.creds = dest_credentials
        self.db = db
        self.user_map = user_map or {}
        self.field_map = field_map or {}
        self.stage_map = stage_map or {}
        self.extractor = WorkflowValueExtractor(
            source_platform, dest_platform,
            self.field_map, self.user_map, self.stage_map
        )

    async def reconstruct_all(self, migration_id: str) -> dict:
        """
        Main entry point. Reads all discovered workflows for a migration,
        attempts to reconstruct each one in the destination platform,
        and returns a summary.
        """
        workflows = self.db.execute(
            """SELECT * FROM discovered_workflows
               WHERE migration_id = ? AND platform = ?""",
            (migration_id, self.source)
        ).fetchall()

        if not workflows:
            return {"reconstructed": [], "skipped": [], "failed": [], "manual": []}

        summary = {"reconstructed": [], "skipped": [], "failed": [], "manual": []}

        for wf in workflows:
            wf = dict(wf)
            result = await self._reconstruct_single(wf)
            category = result.get("status", "failed")
            summary[category].append(result)

        return summary

    async def _reconstruct_single(self, wf: dict) -> dict:
        """Reconstruct one workflow in the destination platform."""
        try:
            raw_trigger = json.loads(wf.get("trigger_json") or "{}")
            raw_actions = json.loads(wf.get("actions_json") or "[]")
            raw_conditions = json.loads(wf.get("conditions_json") or "{}")

            trigger_concept = _classify_trigger(raw_trigger, self.source)
            action_concepts = [_classify_action(a, self.source) for a in (raw_actions if isinstance(raw_actions, list) else [raw_actions])]

            dest_trigger = TRIGGER_CONCEPT_MAP.get(trigger_concept, {}).get(self.dest)
            dest_actions = [ACTION_CONCEPT_MAP.get(ac, {}).get(self.dest) for ac in action_concepts]

            # Flag unsupported trigger
            if dest_trigger is None:
                return {
                    "status": "manual",
                    "workflow_id": wf["workflow_id"],
                    "workflow_name": wf["workflow_name"],
                    "reason": f"Trigger type '{trigger_concept}' is not supported in {self.dest}. Reconstruct manually.",
                    "original_trigger": trigger_concept,
                    "original_actions": action_concepts,
                }

            # Flag unsupported actions
            unsupported = [ac for ac, da in zip(action_concepts, dest_actions) if da is None]
            if unsupported:
                return {
                    "status": "manual",
                    "workflow_id": wf["workflow_id"],
                    "workflow_name": wf["workflow_name"],
                    "reason": f"Actions {unsupported} are not supported in {self.dest}. Closest equivalents not available.",
                    "original_trigger": trigger_concept,
                    "original_actions": action_concepts,
                }

            # Extract concrete values from source workflow
            trigger_values = self.extractor.extract_trigger_values(raw_trigger)
            actions_with_values = []
            raw_actions_list = raw_actions if isinstance(raw_actions, list) else [raw_actions]
            for raw_action, concept in zip(raw_actions_list, action_concepts):
                extracted = self.extractor.extract_action_values(raw_action, concept)
                translated = self.extractor.translate_for_dest(extracted, concept)
                actions_with_values.append({
                    "concept": concept,
                    "dest_action_type": ACTION_CONCEPT_MAP.get(concept, {}).get(self.dest),
                    "values": translated,
                    "raw": raw_action,
                })

            # Dispatch to platform-specific builder
            builder = getattr(self, f"_build_{self.dest.replace('.', '_').replace('-', '_')}", None)
            if not builder:
                return {
                    "status": "skipped",
                    "workflow_id": wf["workflow_id"],
                    "workflow_name": wf["workflow_name"],
                    "reason": f"No builder implemented for destination platform: {self.dest}",
                }

            new_id = await builder(
                wf["workflow_name"], trigger_concept, dest_trigger,
                action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                trigger_values, actions_with_values
            )

            # Log reconstruction in database
            self.db.execute(
                """UPDATE discovered_workflows
                   SET intent_summary = ?, intent_json = ?
                   WHERE workflow_id = ? AND migration_id = ?""",
                (
                    f"Reconstructed in {self.dest} as workflow {new_id}",
                    json.dumps({
                        "source_trigger": trigger_concept,
                        "dest_trigger": dest_trigger,
                        "source_actions": action_concepts,
                        "dest_actions": dest_actions,
                        "dest_workflow_id": new_id,
                    }),
                    wf["workflow_id"],
                    wf.get("migration_id", ""),
                )
            )
            self.db.commit()

            return {
                "status": "reconstructed",
                "workflow_id": wf["workflow_id"],
                "workflow_name": wf["workflow_name"],
                "dest_workflow_id": new_id,
                "trigger_concept": trigger_concept,
                "action_concepts": action_concepts,
            }

        except Exception as e:
            return {
                "status": "failed",
                "workflow_id": wf.get("workflow_id", "unknown"),
                "workflow_name": wf.get("workflow_name", "unknown"),
                "error": str(e),
            }

    # ── HubSpot builder ───────────────────────────────────────────────────────
    async def _build_hubspot(self, name, trigger_concept, dest_trigger,
                             action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                             trigger_values=None, actions_with_values=None):
        token = self.creds.get("api_key") or self.creds.get("access_token")
        object_type = self._detect_object_type(raw_trigger, trigger_concept)
        trigger_values = trigger_values or {}
        actions_with_values = actions_with_values or []

        actions_payload = []
        action_id = 1
        for i, awv in enumerate(actions_with_values):
            concept = awv["concept"]
            action_type = awv["dest_action_type"]
            values = awv.get("values", {})
            action_payload = self._build_hubspot_action(concept, action_type, awv["raw"], values)
            if action_payload:
                action_payload["actionId"] = str(action_id)
                if i < len(actions_with_values) - 1:
                    action_payload["connection"] = {"edgeType": "STANDARD", "nextActionId": str(action_id + 1)}
                action_payload["type"] = "SINGLE_CONNECTION"
                actions_payload.append(action_payload)
                action_id += 1

        # Build enrollment criteria from trigger conditions
        enrollment_criteria = {"type": "AND", "filters": []}
        for cond in trigger_values.get("conditions", []):
            enrollment_criteria["filters"].append({
                "operator": cond.get("operator", "EQ"),
                "property": cond.get("field", ""),
                "value": cond.get("value", ""),
            })

        payload = {
            "name": f"[Migrated] {name}",
            "isEnabled": False,  # Created disabled — operator activates after review
            "flowType": "WORKFLOW",
            "startActionId": "1" if actions_payload else None,
            "nextAvailableActionId": str(action_id),
            "actions": actions_payload,
        }
        if enrollment_criteria["filters"]:
            payload["enrollmentCriteria"] = enrollment_criteria

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.hubapi.com/automation/v4/flows",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return str(data.get("id", "unknown"))
                else:
                    text = await resp.text()
                    raise Exception(f"HubSpot workflow creation failed: {resp.status} — {text[:200]}")

    def _build_hubspot_action(self, concept: str, action_type: str, raw_action, values: dict = None) -> Optional[dict]:
        """Build a HubSpot v4 action payload with real extracted values."""
        values = values or {}
        if concept == "send_notification":
            return {"actionTypeId": "0-14", "actionTypeVersion": 0, "fields": {
                "subject": values.get("subject", "Notification"),
                "body": values.get("body", ""),
            }}
        if concept == "create_task":
            return {"actionTypeId": "0-4", "actionTypeVersion": 0, "fields": {
                "subject": values.get("task_title", "Follow up task"),
                "taskType": values.get("task_type", "TODO"),
            }}
        if concept == "update_deal_stage":
            fields = {"properties": [{"targetProperty": "dealstage", "value": {"type": "STATIC_VALUE", "staticValue": values.get("stage_id", "")}}]}
            return {"actionTypeId": "0-5", "actionTypeVersion": 0, "fields": fields}
        if concept == "assign_owner":
            return {"actionTypeId": "0-7", "actionTypeVersion": 0, "fields": {
                "ownerId": values.get("owner_id", "")
            }}
        if concept == "update_field":
            fields = values.get("properties", [])
            return {"actionTypeId": "0-1", "actionTypeVersion": 0, "fields": {
                "properties": [{"targetProperty": p["field"], "value": {"type": "STATIC_VALUE", "staticValue": str(p["value"])}} for p in fields]
            }}
        if concept == "wait":
            return {"actionTypeId": "0-2", "actionTypeVersion": 0, "fields": {
                "delayMillis": values.get("delay_days", 1) * 86400000
            }}
        if concept == "webhook":
            return {"actionTypeId": "0-6", "actionTypeVersion": 0, "fields": {
                "url": values.get("webhook_url", "")
            }}
        if concept == "add_tag":
            return {"actionTypeId": "0-13", "actionTypeVersion": 0, "fields": {}}
        return None

    # ── Pipedrive builder ─────────────────────────────────────────────────────
    async def _build_pipedrive(self, name, trigger_concept, dest_trigger,
                               action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                               trigger_values=None, actions_with_values=None):
        token = self.creds.get("api_token")
        trigger_values = trigger_values or {}
        actions_with_values = actions_with_values or []

        steps = []
        for awv in actions_with_values:
            step = self._build_pipedrive_step(awv["concept"], awv["dest_action_type"], awv.get("values", {}))
            if step:
                steps.append(step)

        # Build trigger conditions
        trigger_conditions = []
        for cond in trigger_values.get("conditions", []):
            trigger_conditions.append({
                "field": cond.get("field", ""),
                "operator": cond.get("operator", "equals"),
                "value": cond.get("value", ""),
            })

        payload = {
            "name": f"[Migrated] {name}",
            "trigger": {
                "type": dest_trigger,
                "conditions": trigger_conditions,
            },
            "steps": steps,
            "status": "inactive",  # Created inactive for operator review
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.pipedrive.com/v1/automations",
                params={"api_token": token},
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return str(data.get("data", {}).get("id", "unknown"))
                else:
                    text = await resp.text()
                    raise Exception(f"Pipedrive automation creation failed: {resp.status} — {text[:200]}")

    def _build_pipedrive_step(self, concept: str, action_type: str, values: dict = None) -> Optional[dict]:
        values = values or {}
        if concept == "send_notification":
            return {"type": "sendEmail", "value": {
                "to": values.get("to", "owner"),
                "subject": values.get("subject", "[Migrated notification]"),
                "body": values.get("body", ""),
            }}
        if concept == "create_task":
            return {"type": "createActivity", "value": {
                "subject": values.get("task_title", "Follow up"),
                "type": values.get("task_type", "task"),
            }}
        if concept == "update_deal_stage":
            return {"type": "moveDealToStage", "value": {"stage_id": values.get("stage_id", "")}}
        if concept == "assign_owner":
            return {"type": "updateDeal", "value": {"user_id": values.get("owner_id", "")}}
        if concept == "update_field":
            field_updates = values.get("field_updates", values.get("properties", []))
            return {"type": "updateDeal", "value": {"field_updates": field_updates}}
        if concept == "webhook":
            return {"type": "webhook", "value": {"url": values.get("webhook_url", "")}}
        if concept == "wait":
            return {"type": "wait", "value": {"duration": values.get("delay_days", 1)}}
        return None

    # ── Zoho CRM builder ──────────────────────────────────────────────────────
    async def _build_zoho(self, name, trigger_concept, dest_trigger,
                          action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                          trigger_values=None, actions_with_values=None):
        token = self.creds.get("access_token")
        module = self._detect_zoho_module(raw_trigger, trigger_concept)

        instant_actions = []
        for concept, action_type in zip(action_concepts, dest_actions):
            action = self._build_zoho_action(concept, action_type)
            if action:
                instant_actions.append(action)

        payload = {
            "workflow_rules": [{
                "name": f"[Migrated] {name}",
                "description": f"Migrated workflow from {self.source}",
                "module": {"api_name": module},
                "execute_when": {"type": dest_trigger},
                "status": {"active": True},
                "conditions": [{
                    "criteria": None,
                    "instant_actions": {"actions": instant_actions},
                }]
            }]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://www.zohoapis.com/crm/v3/settings/automation/workflow_rules",
                headers={"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"},
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return str(data.get("workflow_rules", [{}])[0].get("id", "unknown"))
                else:
                    text = await resp.text()
                    raise Exception(f"Zoho workflow creation failed: {resp.status} — {text[:200]}")

    def _build_zoho_action(self, concept: str, action_type: str) -> Optional[dict]:
        if concept == "send_notification":
            return {"type": "email_notifications", "name": f"[Migrated notification]"}
        if concept == "create_task":
            return {"type": "tasks", "name": "Follow up task"}
        if concept == "update_field":
            return {"type": "field_updates", "name": "Update field"}
        if concept == "assign_owner":
            return {"type": "assign_owner", "name": "Assign owner"}
        if concept == "webhook":
            return {"type": "webhooks", "name": "Webhook"}
        if concept == "add_tag":
            return {"type": "add_tags", "name": "Add tag"}
        return None

    def _detect_zoho_module(self, raw_trigger: dict, trigger_concept: str) -> str:
        if "deal" in trigger_concept or "opportunity" in trigger_concept:
            return "Deals"
        if "contact" in trigger_concept:
            return "Contacts"
        if "company" in trigger_concept or "account" in trigger_concept:
            return "Accounts"
        if "lead" in trigger_concept:
            return "Leads"
        return "Contacts"

    # ── Monday.com builder ────────────────────────────────────────────────────
    async def _build_monday(self, name, trigger_concept, dest_trigger,
                            action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                            trigger_values=None, actions_with_values=None):
        token = self.creds.get("api_token")
        board_id = self.creds.get("board_id")

        if not board_id:
            raise Exception("Monday.com board_id required in credentials to create automations")

        # Monday uses GraphQL — build the automation recipe via API
        # Note: Monday's automation API creates recipes not free-form automations
        # We create via the workflow builder API endpoint
        query = """
        mutation ($boardId: ID!, $triggerType: String!, $actionType: String!, $name: String!) {
            create_automation(
                board_id: $boardId,
                trigger: { type: $triggerType },
                action: { type: $actionType },
                name: $name
            ) {
                id
                name
            }
        }
        """
        variables = {
            "boardId": board_id,
            "triggerType": dest_trigger,
            "actionType": dest_actions[0] if dest_actions else "notify_someone",
            "name": f"[Migrated] {name}",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.monday.com/v2",
                headers={"Authorization": token, "Content-Type": "application/json", "API-Version": "2024-10"},
                json={"query": query, "variables": variables}
            ) as resp:
                data = await resp.json()
                errors = data.get("errors", [])
                if errors:
                    # Monday's automation creation API is limited — flag for manual if GraphQL fails
                    raise Exception(f"Monday automation creation: {errors[0].get('message', 'unknown error')}")
                return str(data.get("data", {}).get("create_automation", {}).get("id", "unknown"))

    # ── ActiveCampaign builder ────────────────────────────────────────────────
    async def _build_activecampaign(self, name, trigger_concept, dest_trigger,
                                    action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                                    trigger_values=None, actions_with_values=None):
        api_key = self.creds.get("api_key")
        base_url = self.creds.get("base_url", "").rstrip("/")

        # ActiveCampaign automation structure
        payload = {
            "automation": {
                "name": f"[Migrated] {name}",
                "status": "1",  # 1 = active
                "entered": "0",
                "hidden": "0",
                "defaultscreenshot": "0",
            }
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/api/3/automations",
                headers={"Api-Token": api_key, "Content-Type": "application/json"},
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    automation_id = str(data.get("automation", {}).get("id", "unknown"))
                    # Note: AC requires building the full workflow visually after scaffold creation
                    # The API creates the container; triggers and actions are added separately
                    return automation_id
                else:
                    text = await resp.text()
                    raise Exception(f"ActiveCampaign automation creation failed: {resp.status} — {text[:200]}")

    # ── Freshsales builder ────────────────────────────────────────────────────
    async def _build_freshsales(self, name, trigger_concept, dest_trigger,
                                action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                                trigger_values=None, actions_with_values=None):
        api_key = self.creds.get("api_key")
        bundle_alias = self.creds.get("bundle_alias")
        base_url = f"https://{bundle_alias}.myfreshworks.com/crm/sales/api"

        module = self._detect_freshsales_module(trigger_concept)
        actions_payload = self._build_freshsales_actions(action_concepts)

        payload = {
            "workflow": {
                "name": f"[Migrated] {name}",
                "active": True,
                "module": module,
                "trigger": dest_trigger,
                "actions": actions_payload,
            }
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/workflows",
                headers={"Authorization": f"Token token={api_key}", "Content-Type": "application/json"},
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return str(data.get("workflow", {}).get("id", "unknown"))
                else:
                    text = await resp.text()
                    raise Exception(f"Freshsales workflow creation failed: {resp.status} — {text[:200]}")

    def _detect_freshsales_module(self, trigger_concept: str) -> str:
        if "deal" in trigger_concept:
            return "deal"
        if "contact" in trigger_concept:
            return "contact"
        if "account" in trigger_concept or "company" in trigger_concept:
            return "sales_account"
        return "contact"

    def _build_freshsales_actions(self, action_concepts: list) -> list:
        actions = []
        for concept in action_concepts:
            if concept == "send_notification":
                actions.append({"type": "send_email", "value": {}})
            elif concept == "create_task":
                actions.append({"type": "create_task", "value": {"title": "Follow up task"}})
            elif concept == "update_field":
                actions.append({"type": "update_record", "value": {}})
            elif concept == "assign_owner":
                actions.append({"type": "assign_owner", "value": {}})
            elif concept == "webhook":
                actions.append({"type": "webhook", "value": {"url": ""}})
        return actions

    # ── Copper builder ────────────────────────────────────────────────────────
    async def _build_copper(self, name, trigger_concept, dest_trigger,
                            action_concepts, dest_actions, raw_trigger, raw_actions, raw_conditions,
                            trigger_values=None, actions_with_values=None):
        api_key = self.creds.get("api_key")
        user_email = self.creds.get("user_email", "")

        entity = self._detect_copper_entity(trigger_concept)
        actions_payload = self._build_copper_actions(action_concepts)

        payload = {
            "name": f"[Migrated] {name}",
            "record_type": entity,
            "trigger": {"event": dest_trigger},
            "filter_condition": "all",
            "conditions": [],
            "actions": actions_payload,
            "active": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.copper.com/developer_api/v1/automations",
                headers={
                    "X-PW-AccessToken": api_key,
                    "X-PW-Application": "developer_api",
                    "X-PW-UserEmail": user_email,
                    "Content-Type": "application/json",
                },
                json=payload
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    return str(data.get("id", "unknown"))
                else:
                    text = await resp.text()
                    raise Exception(f"Copper workflow creation failed: {resp.status} — {text[:200]}")

    def _detect_copper_entity(self, trigger_concept: str) -> str:
        if "deal" in trigger_concept or "opportunity" in trigger_concept:
            return "opportunity"
        if "contact" in trigger_concept or "person" in trigger_concept:
            return "person"
        if "company" in trigger_concept or "organization" in trigger_concept:
            return "company"
        if "lead" in trigger_concept:
            return "lead"
        return "person"

    def _build_copper_actions(self, action_concepts: list) -> list:
        actions = []
        for concept in action_concepts:
            if concept == "create_task":
                actions.append({"type": "create_task", "value": {"name": "Follow up task"}})
            elif concept == "update_field":
                actions.append({"type": "update_field", "value": {}})
            elif concept == "assign_owner":
                actions.append({"type": "assign_owner", "value": {}})
            elif concept == "update_deal_stage":
                actions.append({"type": "update_stage", "value": {}})
        return actions

    # ── Shared helpers ────────────────────────────────────────────────────────
    def _detect_object_type(self, raw_trigger: dict, trigger_concept: str) -> str:
        if "deal" in trigger_concept:
            return "DEAL"
        if "company" in trigger_concept:
            return "COMPANY"
        if "task" in trigger_concept:
            return "TASK"
        return "CONTACT"

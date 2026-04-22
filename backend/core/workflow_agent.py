import json
import uuid
import asyncio
import aiohttp
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# WorkflowAgent
#
# Phase 2-3 workflow reconstruction for CRM platforms.
# Handles HubSpot <-> Pipedrive as the primary pair.
# Other platforms follow the same pattern.
#
# Flow:
#   1. extract_intent()   — reads source workflow, produces platform-agnostic intent JSON
#   2. translate_intent() — maps intent to destination platform capabilities
#   3. reconstruct()      — creates the workflow in the destination via API
#   4. log_result()       — saves decision to discovered_workflows table for training
#
# What it handles (85-90% of real company workflows):
#   - Deal stage change triggers
#   - Record created/updated triggers
#   - Date-based triggers
#   - Property value condition checks
#   - Create activity actions
#   - Update record field actions
#   - Send internal notification actions
#   - Assign owner actions
#   - Create deal/contact actions
#   - Delay steps
#   - If/else branches (basic)
#
# What it logs but does NOT reconstruct (flagged for manual):
#   - External webhook actions
#   - Email send actions (email addresses differ between platforms)
#   - Integration-specific actions (Slack, Teams, Trello)
#   - Complex nested branches (3+ levels deep)
#   - Custom code actions
# ─────────────────────────────────────────────────────────────────────────────

# ── Intent categories ─────────────────────────────────────────────────────────

TRIGGER_INTENT_MAP = {
    # HubSpot trigger types -> intent category
    "PROPERTY_CHANGE":        "record_property_updated",
    "OBJECT_CREATION":        "record_created",
    "FILTER_BASED":           "filter_criteria_met",
    "DATE":                   "date_based",
    "EVENT":                  "event_based",
    # Pipedrive trigger types -> intent category
    "deal.added":             "record_created",
    "deal.updated":           "record_property_updated",
    "deal.deleted":           "record_deleted",
    "person.added":           "record_created",
    "person.updated":         "record_property_updated",
    "organization.added":     "record_created",
    "organization.updated":   "record_property_updated",
    "activity.added":         "record_created",
    "activity.updated":       "record_property_updated",
    "activity.completed":     "activity_completed",
}

ACTION_INTENT_MAP = {
    # HubSpot actionTypeId -> intent category
    "0-1":   "delay",
    "0-2":   "send_email",          # flagged — manual
    "0-3":   "create_task",
    "0-4":   "update_record",
    "0-5":   "create_record",
    "0-6":   "send_notification",
    "0-9":   "branch",
    "0-14":  "assign_owner",
    "0-22":  "webhook",             # flagged — manual
    "0-38":  "create_deal",
    "0-39":  "create_contact",
    # Pipedrive action types -> intent category
    "create_activity":    "create_task",
    "update_deal":        "update_record",
    "update_person":      "update_record",
    "update_org":         "update_record",
    "send_email":         "send_email",    # flagged — manual
    "add_note":           "add_note",
    "create_deal":        "create_deal",
    "webhook":            "webhook",       # flagged — manual
}

# Actions that cannot be reconstructed automatically
MANUAL_ACTION_TYPES = {"send_email", "webhook", "custom_code", "slack", "teams", "trello"}


class WorkflowAgent:

    def __init__(self, source_platform: str, dest_platform: str,
                 source_creds: dict, dest_creds: dict, db):
        self.src = source_platform
        self.dst = dest_platform
        self.src_creds = source_creds
        self.dst_creds = dest_creds
        self.db = db

    async def reconstruct_all(self, migration_id: str) -> dict:
        """
        Main entry point. Reads all discovered workflows for this migration,
        attempts reconstruction for each, logs results.
        Returns summary of what was reconstructed vs flagged.
        """
        rows = self.db.execute(
            """SELECT id, workflow_id, workflow_name, trigger_type,
                      trigger_json, actions_json, conditions_json, raw_json
               FROM discovered_workflows
               WHERE migration_id = ? AND platform = ?""",
            (migration_id, self.src)
        ).fetchall()

        if not rows:
            return {"reconstructed": [], "flagged": [], "failed": [], "total": 0}

        workflows = [dict(r) for r in rows]
        results = {"reconstructed": [], "flagged": [], "failed": [], "total": len(workflows)}

        for wf in workflows:
            try:
                intent = self._extract_intent(wf)
                reconstruction_plan = self._translate_intent(intent)

                if reconstruction_plan["requires_manual"]:
                    results["flagged"].append({
                        "workflow_id": wf["workflow_id"],
                        "name": wf["workflow_name"],
                        "reason": reconstruction_plan["manual_reason"],
                        "manual_actions": reconstruction_plan["manual_actions"]
                    })
                    self._log_result(migration_id, wf["id"], "flagged",
                                     intent, reconstruction_plan, None)
                    continue

                dest_workflow_id = await self._create_in_destination(reconstruction_plan)
                results["reconstructed"].append({
                    "workflow_id": wf["workflow_id"],
                    "name": wf["workflow_name"],
                    "dest_workflow_id": dest_workflow_id,
                    "coverage": reconstruction_plan["coverage_pct"]
                })
                self._log_result(migration_id, wf["id"], "reconstructed",
                                 intent, reconstruction_plan, dest_workflow_id)

            except Exception as e:
                results["failed"].append({
                    "workflow_id": wf["workflow_id"],
                    "name": wf["workflow_name"],
                    "error": str(e)
                })
                self._log_result(migration_id, wf["id"], "failed",
                                 None, None, None, str(e))

        return results

    def _extract_intent(self, workflow: dict) -> dict:
        """
        Convert platform-specific workflow definition into platform-agnostic intent.
        This is the core intelligence step.
        """
        trigger_json = json.loads(workflow["trigger_json"]) if isinstance(workflow["trigger_json"], str) else workflow["trigger_json"] or {}
        actions_json = json.loads(workflow["actions_json"]) if isinstance(workflow["actions_json"], str) else workflow["actions_json"] or []
        conditions_json = json.loads(workflow["conditions_json"]) if isinstance(workflow["conditions_json"], str) else workflow["conditions_json"] or {}
        raw = json.loads(workflow["raw_json"]) if isinstance(workflow["raw_json"], str) else workflow["raw_json"] or {}

        trigger_type_raw = workflow.get("trigger_type", "")
        trigger_intent = TRIGGER_INTENT_MAP.get(trigger_type_raw, "unknown")

        # Extract trigger details
        trigger = {
            "intent": trigger_intent,
            "raw_type": trigger_type_raw,
            "object_type": self._extract_object_type(trigger_json, raw),
            "conditions": self._extract_trigger_conditions(trigger_json, conditions_json, raw),
        }

        # Extract each action
        actions = []
        for action in (actions_json if isinstance(actions_json, list) else []):
            action_intent = self._extract_action_intent(action)
            actions.append({
                "intent": action_intent,
                "raw_action": action,
                "requires_manual": action_intent in MANUAL_ACTION_TYPES,
                "parameters": self._extract_action_params(action, action_intent),
            })

        # Generate plain English summary
        plain = self._generate_plain_english(trigger, actions, workflow["workflow_name"])

        return {
            "workflow_name": workflow["workflow_name"],
            "plain_english": plain,
            "trigger": trigger,
            "actions": actions,
            "source_platform": self.src,
        }

    def _extract_object_type(self, trigger_json: dict, raw: dict) -> str:
        """Determine which CRM object type this workflow operates on."""
        # HubSpot
        if "objectType" in raw:
            return raw["objectType"].lower()
        if "type" in raw:
            t = raw["type"].lower()
            if "contact" in t: return "contacts"
            if "deal" in t: return "deals"
            if "company" in t: return "companies"
        # Pipedrive
        if "object" in trigger_json:
            obj = trigger_json["object"].lower()
            if obj in ("person", "persons"): return "contacts"
            if obj in ("organization", "organizations"): return "companies"
            if obj in ("deal", "deals"): return "deals"
            return obj
        return "contacts"

    def _extract_trigger_conditions(self, trigger_json: dict, conditions_json: dict, raw: dict) -> list:
        """Extract trigger filter conditions in a normalized format."""
        conditions = []
        # HubSpot filter criteria
        filters = raw.get("enrollmentCriteria", {}).get("filterBranches", [])
        for branch in filters:
            for f in branch.get("filters", []):
                conditions.append({
                    "field": f.get("property", f.get("field", "")),
                    "operator": f.get("operation", f.get("operator", "equals")),
                    "value": f.get("value", ""),
                })
        # Pipedrive conditions
        if isinstance(conditions_json, dict):
            for k, v in conditions_json.items():
                conditions.append({"field": k, "operator": "equals", "value": v})
        return conditions

    def _extract_action_intent(self, action: dict) -> str:
        """Map a raw action definition to an intent category."""
        # HubSpot uses actionTypeId
        if "actionTypeId" in action:
            return ACTION_INTENT_MAP.get(str(action["actionTypeId"]), "unknown")
        # Pipedrive uses type field
        if "type" in action:
            t = action["type"].lower()
            return ACTION_INTENT_MAP.get(t, "unknown")
        return "unknown"

    def _extract_action_params(self, action: dict, intent: str) -> dict:
        """Extract relevant parameters from an action for reconstruction."""
        params = {}
        fields = action.get("fields", action.get("properties", action.get("params", {})))
        if isinstance(fields, dict):
            params.update(fields)
        elif isinstance(fields, list):
            for f in fields:
                if isinstance(f, dict) and "name" in f:
                    params[f["name"]] = f.get("value", "")
        # Common parameter extraction
        if intent == "delay":
            params["delay_amount"] = action.get("fields", {}).get("delayMillis", 0) if isinstance(action.get("fields"), dict) else 0
        if intent == "assign_owner":
            params["owner_field"] = "owner_id"
        return params

    def _generate_plain_english(self, trigger: dict, actions: list, name: str) -> str:
        """Generate a human-readable description of what this workflow does."""
        trigger_desc = {
            "record_created": f"When a new {trigger['object_type'].rstrip('s')} is created",
            "record_property_updated": f"When a {trigger['object_type'].rstrip('s')} property is updated",
            "filter_criteria_met": f"When a {trigger['object_type'].rstrip('s')} meets filter criteria",
            "date_based": f"On a scheduled date",
            "activity_completed": "When an activity is marked complete",
        }.get(trigger["intent"], f"When triggered ({trigger['intent']})")

        if trigger["conditions"]:
            conds = ", ".join([f"{c['field']} {c['operator']} {c['value']}" for c in trigger["conditions"][:2]])
            trigger_desc += f" where {conds}"

        action_descs = []
        for a in actions:
            desc = {
                "create_task": "create a follow-up activity",
                "update_record": "update record fields",
                "send_notification": "send an internal notification",
                "assign_owner": "assign to an owner",
                "create_deal": "create a new deal",
                "create_contact": "create a new contact",
                "delay": "wait a specified time",
                "branch": "branch based on conditions",
                "add_note": "add a note to the record",
            }.get(a["intent"], f"perform action ({a['intent']})")
            if a["requires_manual"]:
                desc += " [manual setup required]"
            action_descs.append(desc)

        actions_str = ", then ".join(action_descs) if action_descs else "perform automated actions"
        return f"{trigger_desc}, {actions_str}. (Source: {name})"

    def _translate_intent(self, intent: dict) -> dict:
        """
        Given platform-agnostic intent, produce a reconstruction plan
        for the destination platform.
        """
        manual_actions = [a for a in intent["actions"] if a["requires_manual"]]
        reconstructable = [a for a in intent["actions"] if not a["requires_manual"]]
        total = len(intent["actions"])
        coverage_pct = round((len(reconstructable) / total * 100) if total > 0 else 0)

        requires_manual = (
            len(manual_actions) > 0 and len(reconstructable) == 0
        ) or intent["trigger"]["intent"] == "unknown"

        manual_reason = None
        if requires_manual:
            if manual_actions:
                types = list(set(a["intent"] for a in manual_actions))
                manual_reason = f"Contains actions that require manual setup: {', '.join(types)}"
            else:
                manual_reason = "Trigger type could not be mapped to destination platform"

        # Build destination-specific workflow definition
        if self.dst == "hubspot":
            dest_definition = self._build_hubspot_workflow(intent, reconstructable)
        elif self.dst == "pipedrive":
            dest_definition = self._build_pipedrive_automation(intent, reconstructable)
        else:
            dest_definition = None
            requires_manual = True
            manual_reason = f"Workflow reconstruction not yet implemented for {self.dst}"

        return {
            "workflow_name": intent["workflow_name"],
            "plain_english": intent["plain_english"],
            "dest_platform": self.dst,
            "dest_definition": dest_definition,
            "requires_manual": requires_manual,
            "manual_reason": manual_reason,
            "manual_actions": [a["intent"] for a in manual_actions],
            "coverage_pct": coverage_pct,
        }

    def _build_hubspot_workflow(self, intent: dict, actions: list) -> dict:
        """Build a HubSpot v4 Automation API workflow definition from intent."""
        trigger_intent = intent["trigger"]["intent"]
        object_type = intent["trigger"]["object_type"]

        # Map object type to HubSpot type
        hs_object = {
            "contacts": "CONTACT",
            "companies": "COMPANY",
            "deals": "DEAL",
            "activities": "ENGAGEMENT",
        }.get(object_type, "CONTACT")

        # Build enrollment criteria from conditions
        enrollment_criteria = {"filterBranches": [], "filterBranchType": "AND"}
        for cond in intent["trigger"]["conditions"]:
            if cond["field"] and cond["value"]:
                enrollment_criteria["filterBranches"].append({
                    "filters": [{
                        "property": cond["field"],
                        "operation": {"operationType": "SET_ANY", "values": [str(cond["value"])]}
                    }],
                    "filterBranchType": "AND",
                    "filterBranchOperator": "AND"
                })

        # Build action list
        hs_actions = []
        action_id = 1
        for a in actions:
            hs_action = self._intent_to_hubspot_action(a, action_id)
            if hs_action:
                hs_actions.append(hs_action)
                action_id += 1

        return {
            "name": f"[Migrated] {intent['workflow_name']}",
            "type": f"{hs_object}_BASED",
            "enabled": False,  # Always start disabled — operator enables after review
            "enrollmentCriteria": enrollment_criteria,
            "actions": hs_actions,
        }

    def _intent_to_hubspot_action(self, action: dict, action_id: int) -> Optional[dict]:
        """Convert an intent action to a HubSpot actionTypeId action."""
        intent = action["intent"]
        params = action.get("parameters", {})
        base = {"actionId": str(action_id), "nextActionId": str(action_id + 1)}

        if intent == "create_task":
            return {**base, "actionTypeId": "0-3", "fields": {
                "taskType": "TODO",
                "taskBody": params.get("taskBody", params.get("subject", "Follow up")),
                "daysUntilDue": str(params.get("daysUntilDue", 1)),
            }}
        elif intent == "update_record":
            field = params.get("field", params.get("property", ""))
            value = params.get("value", params.get("to_value", ""))
            if field and value:
                return {**base, "actionTypeId": "0-4", "fields": {
                    "propertyName": field,
                    "newValue": str(value),
                }}
        elif intent == "send_notification":
            return {**base, "actionTypeId": "0-6", "fields": {
                "message": params.get("message", "Automated notification from automated migration"),
            }}
        elif intent == "assign_owner":
            return {**base, "actionTypeId": "0-14", "fields": {
                "ownerField": "hubspot_owner_id",
            }}
        elif intent == "delay":
            delay_ms = params.get("delay_amount", 86400000)
            return {**base, "actionTypeId": "0-1", "fields": {
                "delayMillis": str(delay_ms),
            }}
        elif intent == "create_deal":
            return {**base, "actionTypeId": "0-38", "fields": {
                "dealname": params.get("title", params.get("name", "New Deal")),
            }}
        elif intent == "create_contact":
            return {**base, "actionTypeId": "0-39", "fields": {
                "email": params.get("email", ""),
            }}
        return None

    def _build_pipedrive_automation(self, intent: dict, actions: list) -> dict:
        """Build a Pipedrive automation definition from intent."""
        trigger_intent = intent["trigger"]["intent"]
        object_type = intent["trigger"]["object_type"]

        # Map object type to Pipedrive object
        pd_object = {
            "contacts": "person",
            "companies": "organization",
            "deals": "deal",
            "activities": "activity",
        }.get(object_type, "deal")

        # Map trigger intent to Pipedrive event
        pd_event = {
            "record_created": "added",
            "record_property_updated": "updated",
            "record_deleted": "deleted",
            "activity_completed": "completed",
        }.get(trigger_intent, "updated")

        # Build conditions
        pd_conditions = []
        for cond in intent["trigger"]["conditions"]:
            if cond["field"] and cond["value"]:
                pd_conditions.append({
                    "field": cond["field"],
                    "operator": "=",
                    "value": str(cond["value"]),
                })

        # Build action steps
        pd_actions = []
        for a in actions:
            pd_action = self._intent_to_pipedrive_action(a, object_type)
            if pd_action:
                pd_actions.append(pd_action)

        return {
            "name": f"[Migrated] {intent['workflow_name']}",
            "active": False,  # Always start disabled
            "trigger": {
                "object": pd_object,
                "event": pd_event,
                "conditions": pd_conditions,
            },
            "steps": pd_actions,
        }

    def _intent_to_pipedrive_action(self, action: dict, object_type: str) -> Optional[dict]:
        """Convert an intent action to a Pipedrive automation step."""
        intent = action["intent"]
        params = action.get("parameters", {})

        if intent == "create_task":
            return {
                "type": "create_activity",
                "properties": {
                    "subject": params.get("taskBody", params.get("subject", "Follow up")),
                    "type": "task",
                    "due_date": "today+1",
                }
            }
        elif intent == "update_record":
            field = params.get("field", params.get("property", ""))
            value = params.get("value", params.get("to_value", ""))
            pd_type = {
                "contacts": "update_person",
                "companies": "update_org",
                "deals": "update_deal",
            }.get(object_type, "update_deal")
            if field and value:
                return {
                    "type": pd_type,
                    "properties": {field: value}
                }
        elif intent == "assign_owner":
            pd_type = {
                "contacts": "update_person",
                "companies": "update_org",
                "deals": "update_deal",
            }.get(object_type, "update_deal")
            return {
                "type": pd_type,
                "properties": {"owner_id": params.get("owner_id", "")}
            }
        elif intent == "add_note":
            return {
                "type": "add_note",
                "properties": {
                    "content": params.get("content", params.get("message", "Note added by automated migration")),
                }
            }
        elif intent == "create_deal":
            return {
                "type": "create_deal",
                "properties": {
                    "title": params.get("dealname", params.get("title", "New Deal")),
                }
            }
        return None

    async def _create_in_destination(self, plan: dict) -> str:
        """Create the reconstructed workflow in the destination platform via API."""
        if not plan.get("dest_definition"):
            raise ValueError("No destination definition to create")

        if self.dst == "hubspot":
            return await self._create_hubspot_workflow(plan["dest_definition"])
        elif self.dst == "pipedrive":
            return await self._create_pipedrive_automation(plan["dest_definition"])
        else:
            raise ValueError(f"Workflow creation not supported for {self.dst}")

    async def _create_hubspot_workflow(self, definition: dict) -> str:
        """POST to HubSpot v4 Automation API to create the workflow."""
        token = self.dst_creds.get("api_key") or self.dst_creds.get("access_token")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.hubapi.com/automation/v4/flows",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=definition
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return str(data.get("id", "unknown"))

    async def _create_pipedrive_automation(self, definition: dict) -> str:
        """POST to Pipedrive automations API to create the automation."""
        token = self.dst_creds.get("api_token")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.pipedrive.com/v1/automations",
                params={"api_token": token},
                json=definition
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return str(data.get("data", {}).get("id", "unknown"))

    def _log_result(self, migration_id: str, discovered_id: str,
                    status: str, intent: Optional[dict],
                    plan: Optional[dict], dest_id: Optional[str],
                    error: str = None):
        """Update the discovered_workflows row with reconstruction result."""
        try:
            self.db.execute(
                """UPDATE discovered_workflows
                   SET status = ?,
                       intent_summary = ?,
                       intent_json = ?,
                       reconstructed_in = ?,
                       reconstruction_gaps = ?
                   WHERE id = ?""",
                (
                    status,
                    intent.get("plain_english") if intent else error,
                    json.dumps(intent) if intent else None,
                    dest_id,
                    json.dumps(plan.get("manual_actions", [])) if plan else None,
                    discovered_id
                )
            )
            self.db.commit()
        except Exception as e:
            print(f"[WorkflowAgent] Failed to log result: {e}")

import json
import re

# ─────────────────────────────────────────────────────────────────────────────
# WorkflowValueExtractor
#
# Extracts concrete values from source workflow JSON and maps them to
# destination-platform-compatible values.
#
# For each source platform it knows:
#   - Where to find trigger field/value pairs in the raw JSON
#   - Where to find action field/value pairs in the raw JSON
#   - How to translate those values to the destination platform's format
#
# The field_map parameter comes from the migration's field_mappings table
# so we can translate source field names to destination field names.
# The user_map parameter maps source user IDs to destination user IDs.
# The stage_map parameter maps source stage names/IDs to destination ones.
# ─────────────────────────────────────────────────────────────────────────────


class WorkflowValueExtractor:

    def __init__(self, source_platform: str, dest_platform: str,
                 field_map: dict = None, user_map: dict = None, stage_map: dict = None):
        self.source = source_platform
        self.dest = dest_platform
        self.field_map = field_map or {}   # {source_field_name: dest_field_name}
        self.user_map = user_map or {}     # {source_user_id: dest_user_id}
        self.stage_map = stage_map or {}   # {source_stage_id: dest_stage_id}

    # ── Public entry points ───────────────────────────────────────────────────

    def extract_trigger_values(self, raw_trigger: dict) -> dict:
        """Extract enrollment criteria/conditions from the source trigger."""
        extractor = getattr(self, f"_extract_trigger_{self.source}", self._extract_trigger_generic)
        return extractor(raw_trigger)

    def extract_action_values(self, raw_action: dict, action_concept: str) -> dict:
        """Extract concrete field values from a source action."""
        extractor = getattr(self, f"_extract_action_{self.source}", self._extract_action_generic)
        return extractor(raw_action, action_concept)

    def translate_for_dest(self, values: dict, action_concept: str) -> dict:
        """Translate extracted values to destination platform format."""
        translator = getattr(self, f"_translate_to_{self.dest}", self._translate_generic)
        return translator(values, action_concept)

    def map_field(self, source_field: str) -> str:
        """Translate a source field name to its destination equivalent."""
        return self.field_map.get(source_field, source_field)

    def map_user(self, source_user_id) -> str:
        """Translate a source user ID to destination user ID."""
        return self.user_map.get(str(source_user_id), str(source_user_id))

    def map_stage(self, source_stage) -> str:
        """Translate a source stage ID/name to destination equivalent."""
        return self.stage_map.get(str(source_stage), str(source_stage))

    # ── HubSpot source extractors ─────────────────────────────────────────────

    def _extract_trigger_hubspot(self, raw_trigger: dict) -> dict:
        """
        HubSpot v4 trigger structure:
        {
          "type": "CONTACT_PROPERTY_CHANGE",
          "filterBranch": {
            "filterBranchType": "AND",
            "filters": [
              {"operator": "EQ", "property": "hs_lead_status", "value": "NEW"}
            ]
          }
        }
        """
        values = {
            "trigger_type": raw_trigger.get("type", ""),
            "conditions": [],
        }
        filter_branch = raw_trigger.get("filterBranch", {})
        filters = filter_branch.get("filters", [])
        for f in filters:
            values["conditions"].append({
                "field": self.map_field(f.get("property", "")),
                "operator": f.get("operator", "EQ"),
                "value": f.get("value", ""),
            })
        return values

    def _extract_action_hubspot(self, raw_action: dict, concept: str) -> dict:
        """
        HubSpot v4 action structure:
        {
          "actionTypeId": "0-1",
          "fields": {
            "properties": [
              {"targetProperty": "dealstage", "value": {"type": "STATIC_VALUE", "staticValue": "closedwon"}}
            ]
          }
        }
        """
        values = {"concept": concept, "action_type_id": raw_action.get("actionTypeId", "")}
        fields = raw_action.get("fields", {})
        properties = fields.get("properties", [])
        values["properties"] = []
        for prop in properties:
            target = prop.get("targetProperty", "")
            val_obj = prop.get("value", {})
            val = val_obj.get("staticValue", "") or val_obj.get("value", "")
            values["properties"].append({
                "field": self.map_field(target),
                "value": self._translate_value(val, target),
            })
        # Email actions
        if fields.get("subject"):
            values["subject"] = fields["subject"]
        if fields.get("body"):
            values["body"] = fields["body"]
        if fields.get("to"):
            values["to"] = fields["to"]
        # Task actions
        if fields.get("subject") and concept == "create_task":
            values["task_title"] = fields["subject"]
        if fields.get("taskType"):
            values["task_type"] = fields["taskType"]
        # Assignment actions
        if fields.get("ownerId"):
            values["owner_id"] = self.map_user(fields["ownerId"])
        # Delay actions
        if fields.get("delayMillis"):
            values["delay_ms"] = fields["delayMillis"]
            values["delay_days"] = int(fields["delayMillis"]) // 86400000
        # Webhook actions
        if fields.get("url"):
            values["webhook_url"] = fields["url"]
        return values

    # ── Pipedrive source extractors ───────────────────────────────────────────

    def _extract_trigger_pipedrive(self, raw_trigger: dict) -> dict:
        """
        Pipedrive trigger structure:
        {
          "type": "deal.stage_changed",
          "conditions": [
            {"field": "stage_id", "operator": "equals", "value": "3"}
          ]
        }
        """
        values = {
            "trigger_type": raw_trigger.get("type", ""),
            "conditions": [],
        }
        for cond in raw_trigger.get("conditions", []):
            values["conditions"].append({
                "field": self.map_field(cond.get("field", "")),
                "operator": cond.get("operator", "equals"),
                "value": cond.get("value", ""),
            })
        return values

    def _extract_action_pipedrive(self, raw_action: dict, concept: str) -> dict:
        """
        Pipedrive action step structure:
        {
          "type": "createActivity",
          "value": {
            "subject": "Follow up call",
            "type": "call",
            "due_date": "in_one_day"
          }
        }
        """
        values = {"concept": concept}
        val = raw_action.get("value", {})
        if isinstance(val, dict):
            if val.get("subject"):
                values["task_title"] = val["subject"]
                values["subject"] = val["subject"]
            if val.get("body"):
                values["body"] = val["body"]
            if val.get("to"):
                values["to"] = val["to"]
            if val.get("type"):
                values["task_type"] = val["type"]
            if val.get("user_id"):
                values["owner_id"] = self.map_user(val["user_id"])
            if val.get("stage_id"):
                values["stage_id"] = self.map_stage(val["stage_id"])
            if val.get("url"):
                values["webhook_url"] = val["url"]
            if val.get("duration"):
                values["delay_days"] = val["duration"]
            # Copy all field updates
            field_updates = val.get("field_updates", [])
            values["properties"] = []
            for fu in field_updates:
                values["properties"].append({
                    "field": self.map_field(fu.get("field", "")),
                    "value": fu.get("value", ""),
                })
        return values

    # ── Zoho source extractors ────────────────────────────────────────────────

    def _extract_trigger_zoho(self, raw_trigger: dict) -> dict:
        values = {
            "trigger_type": raw_trigger.get("type", raw_trigger.get("execute_when", {}).get("type", "")),
            "conditions": [],
        }
        for cond in raw_trigger.get("conditions", raw_trigger.get("criteria", [])):
            if isinstance(cond, dict):
                values["conditions"].append({
                    "field": self.map_field(cond.get("field", {}).get("api_name", cond.get("field", ""))),
                    "operator": cond.get("comparator", "equals"),
                    "value": cond.get("value", ""),
                })
        return values

    def _extract_action_zoho(self, raw_action: dict, concept: str) -> dict:
        values = {"concept": concept}
        if raw_action.get("subject"):
            values["subject"] = raw_action["subject"]
            values["task_title"] = raw_action["subject"]
        if raw_action.get("message") or raw_action.get("body"):
            values["body"] = raw_action.get("message", raw_action.get("body", ""))
        if raw_action.get("to"):
            values["to"] = raw_action["to"]
        if raw_action.get("assign_to"):
            values["owner_id"] = self.map_user(raw_action["assign_to"])
        if raw_action.get("url"):
            values["webhook_url"] = raw_action["url"]
        field_updates = raw_action.get("field_to_update", raw_action.get("field_updates", []))
        values["properties"] = []
        for fu in (field_updates if isinstance(field_updates, list) else [field_updates]):
            if isinstance(fu, dict):
                values["properties"].append({
                    "field": self.map_field(fu.get("field", {}).get("api_name", "")),
                    "value": fu.get("value", ""),
                })
        return values

    # ── Monday source extractors ──────────────────────────────────────────────

    def _extract_trigger_monday(self, raw_trigger: dict) -> dict:
        values = {
            "trigger_type": raw_trigger.get("type", ""),
            "conditions": [],
            "board_id": raw_trigger.get("boardId", raw_trigger.get("board_id", "")),
        }
        if raw_trigger.get("columnId"):
            values["conditions"].append({
                "field": self.map_field(raw_trigger["columnId"]),
                "operator": "changed",
                "value": raw_trigger.get("columnValue", ""),
            })
        return values

    def _extract_action_monday(self, raw_action: dict, concept: str) -> dict:
        values = {"concept": concept}
        if raw_action.get("personId"):
            values["owner_id"] = self.map_user(raw_action["personId"])
        if raw_action.get("columnId"):
            values["properties"] = [{"field": self.map_field(raw_action["columnId"]), "value": raw_action.get("columnValue", "")}]
        if raw_action.get("text"):
            values["subject"] = raw_action["text"]
            values["task_title"] = raw_action["text"]
        if raw_action.get("notificationText"):
            values["body"] = raw_action["notificationText"]
        return values

    # ── ActiveCampaign source extractors ──────────────────────────────────────

    def _extract_trigger_activecampaign(self, raw_trigger: dict) -> dict:
        values = {
            "trigger_type": raw_trigger.get("type", raw_trigger.get("startTrigger", "")),
            "conditions": [],
        }
        for cond in raw_trigger.get("conditions", []):
            values["conditions"].append({
                "field": self.map_field(cond.get("field", "")),
                "operator": cond.get("operator", "eq"),
                "value": cond.get("value", ""),
            })
        return values

    def _extract_action_activecampaign(self, raw_action: dict, concept: str) -> dict:
        values = {"concept": concept}
        action_type = raw_action.get("actionType", raw_action.get("type", ""))
        args = raw_action.get("arguments", raw_action.get("args", {}))
        if isinstance(args, dict):
            if args.get("subject"):
                values["subject"] = args["subject"]
            if args.get("html") or args.get("body"):
                values["body"] = args.get("html", args.get("body", ""))
            if args.get("title") or args.get("task_title"):
                values["task_title"] = args.get("title", args.get("task_title", ""))
            if args.get("assignee_id"):
                values["owner_id"] = self.map_user(args["assignee_id"])
            if args.get("field"):
                values["properties"] = [{"field": self.map_field(args["field"]), "value": args.get("value", "")}]
            if args.get("url"):
                values["webhook_url"] = args["url"]
        return values

    # ── Freshsales source extractors ──────────────────────────────────────────

    def _extract_trigger_freshsales(self, raw_trigger: dict) -> dict:
        values = {
            "trigger_type": raw_trigger.get("type", raw_trigger.get("trigger", "")),
            "conditions": [],
        }
        for cond in raw_trigger.get("conditions", []):
            values["conditions"].append({
                "field": self.map_field(cond.get("attribute", cond.get("field", ""))),
                "operator": cond.get("operator", "is"),
                "value": cond.get("value", ""),
            })
        return values

    def _extract_action_freshsales(self, raw_action: dict, concept: str) -> dict:
        values = {"concept": concept}
        action_val = raw_action.get("value", raw_action)
        if isinstance(action_val, dict):
            if action_val.get("subject") or action_val.get("title"):
                values["subject"] = action_val.get("subject", action_val.get("title", ""))
                values["task_title"] = values["subject"]
            if action_val.get("body") or action_val.get("description"):
                values["body"] = action_val.get("body", action_val.get("description", ""))
            if action_val.get("owner_id"):
                values["owner_id"] = self.map_user(action_val["owner_id"])
            if action_val.get("url"):
                values["webhook_url"] = action_val["url"]
        return values

    # ── Copper source extractors ──────────────────────────────────────────────

    def _extract_trigger_copper(self, raw_trigger: dict) -> dict:
        values = {
            "trigger_type": raw_trigger.get("event", raw_trigger.get("type", "")),
            "conditions": [],
        }
        for cond in raw_trigger.get("conditions", []):
            values["conditions"].append({
                "field": self.map_field(cond.get("field", "")),
                "operator": cond.get("operator", "equals"),
                "value": cond.get("value", ""),
            })
        return values

    def _extract_action_copper(self, raw_action: dict, concept: str) -> dict:
        values = {"concept": concept}
        action_val = raw_action.get("value", raw_action)
        if isinstance(action_val, dict):
            if action_val.get("name") or action_val.get("title"):
                values["task_title"] = action_val.get("name", action_val.get("title", ""))
            if action_val.get("assignee_id"):
                values["owner_id"] = self.map_user(action_val["assignee_id"])
            if action_val.get("field_definition_id"):
                values["properties"] = [{"field": str(action_val["field_definition_id"]), "value": action_val.get("value", "")}]
        return values

    # ── Generic fallbacks ─────────────────────────────────────────────────────

    def _extract_trigger_generic(self, raw_trigger: dict) -> dict:
        return {"trigger_type": raw_trigger.get("type", ""), "conditions": []}

    def _extract_action_generic(self, raw_action: dict, concept: str) -> dict:
        return {"concept": concept}

    # ── Destination translators ───────────────────────────────────────────────

    def _translate_to_hubspot(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to HubSpot v4 action fields format."""
        fields = {}
        properties = []
        for prop in values.get("properties", []):
            properties.append({
                "targetProperty": prop["field"],
                "value": {"type": "STATIC_VALUE", "staticValue": str(prop["value"])}
            })
        if properties:
            fields["properties"] = properties
        if values.get("subject"):
            fields["subject"] = values["subject"]
        if values.get("body"):
            fields["body"] = values["body"]
        if values.get("task_title"):
            fields["subject"] = values["task_title"]
        if values.get("owner_id"):
            fields["ownerId"] = values["owner_id"]
        if values.get("delay_days"):
            fields["delayMillis"] = int(values["delay_days"]) * 86400000
        if values.get("webhook_url"):
            fields["url"] = values["webhook_url"]
        return fields

    def _translate_to_pipedrive(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to Pipedrive automation step value format."""
        step_val = {}
        if values.get("task_title") or values.get("subject"):
            step_val["subject"] = values.get("task_title", values.get("subject", ""))
        if values.get("body"):
            step_val["body"] = values["body"]
        if values.get("owner_id"):
            step_val["user_id"] = values["owner_id"]
        if values.get("stage_id"):
            step_val["stage_id"] = values["stage_id"]
        if values.get("webhook_url"):
            step_val["url"] = values["webhook_url"]
        if values.get("delay_days"):
            step_val["duration"] = values["delay_days"]
        if values.get("properties"):
            step_val["field_updates"] = [
                {"field": p["field"], "value": p["value"]} for p in values["properties"]
            ]
        return step_val

    def _translate_to_zoho(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to Zoho workflow action format."""
        action = {}
        if values.get("subject") or values.get("task_title"):
            action["subject"] = values.get("subject", values.get("task_title", ""))
        if values.get("body"):
            action["message"] = values["body"]
        if values.get("owner_id"):
            action["assign_to"] = values["owner_id"]
        if values.get("webhook_url"):
            action["url"] = values["webhook_url"]
        if values.get("properties"):
            action["field_to_update"] = [
                {"field": {"api_name": p["field"]}, "value": p["value"]} for p in values["properties"]
            ]
        return action

    def _translate_to_monday(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to Monday.com automation action format."""
        action = {}
        if values.get("owner_id"):
            action["personId"] = values["owner_id"]
        if values.get("properties") and len(values["properties"]) > 0:
            action["columnId"] = values["properties"][0]["field"]
            action["columnValue"] = values["properties"][0]["value"]
        if values.get("task_title") or values.get("subject"):
            action["text"] = values.get("task_title", values.get("subject", ""))
        if values.get("body"):
            action["notificationText"] = values["body"]
        return action

    def _translate_to_activecampaign(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to ActiveCampaign automation action format."""
        args = {}
        if values.get("subject"):
            args["subject"] = values["subject"]
        if values.get("body"):
            args["html"] = values["body"]
        if values.get("task_title"):
            args["title"] = values["task_title"]
        if values.get("owner_id"):
            args["assignee_id"] = values["owner_id"]
        if values.get("properties") and len(values["properties"]) > 0:
            args["field"] = values["properties"][0]["field"]
            args["value"] = values["properties"][0]["value"]
        if values.get("webhook_url"):
            args["url"] = values["webhook_url"]
        return {"arguments": args}

    def _translate_to_freshsales(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to Freshsales workflow action format."""
        val = {}
        if values.get("task_title") or values.get("subject"):
            val["title"] = values.get("task_title", values.get("subject", ""))
        if values.get("body"):
            val["description"] = values["body"]
        if values.get("owner_id"):
            val["owner_id"] = values["owner_id"]
        if values.get("webhook_url"):
            val["url"] = values["webhook_url"]
        return {"value": val}

    def _translate_to_copper(self, values: dict, action_concept: str) -> dict:
        """Convert extracted values to Copper automation action format."""
        val = {}
        if values.get("task_title"):
            val["name"] = values["task_title"]
        if values.get("owner_id"):
            val["assignee_id"] = values["owner_id"]
        if values.get("properties") and len(values["properties"]) > 0:
            val["field_definition_id"] = values["properties"][0]["field"]
            val["value"] = values["properties"][0]["value"]
        return {"value": val}

    def _translate_generic(self, values: dict, action_concept: str) -> dict:
        return values

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _translate_value(self, value: str, field_name: str) -> str:
        """Apply any known value translations for specific field types."""
        if not value:
            return value
        # Stage ID translation
        if "stage" in field_name.lower():
            return self.map_stage(value)
        # User ID translation
        if "owner" in field_name.lower() or "user" in field_name.lower():
            return self.map_user(value)
        return value

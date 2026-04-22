from connectors.base import BaseConnector

class SchemaDiscovery:

    def __init__(self, source: BaseConnector, dest: BaseConnector):
        self.source = source
        self.dest = dest

    async def discover_all(self) -> dict:
        src_types = await self.source.get_object_types()
        dst_types = await self.dest.get_object_types()

        # Map common object types across platforms
        type_map = self._build_type_map(src_types, dst_types)

        result = {}
        for src_type, dst_type in type_map.items():
            src_schema = await self.source.get_schema(src_type)
            dst_schema = await self.dest.get_schema(dst_type)
            result[src_type] = {
                "source": src_schema,
                "dest": dst_schema,
                "dest_object_type": dst_type,
            }
        return result

    def _build_type_map(self, src_types: list, dst_types: list) -> dict:
        # Normalize names for comparison
        ALIASES = {
            "contacts": ["contacts", "persons", "people"],
            "companies": ["companies", "organizations", "accounts", "sales_accounts"],
            "deals": ["deals", "opportunities"],
        }
        result = {}
        for canonical, variants in ALIASES.items():
            src_match = next((t for t in src_types if t.lower() in variants), None)
            dst_match = next((t for t in dst_types if t.lower() in variants), None)
            if src_match and dst_match:
                result[src_match] = dst_match
        return result

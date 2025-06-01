import os
import json
import requests
from typing import Dict, List, Optional


class AirbyteAPIError(Exception):
    """Raised if an Airbyte API call returns an error status."""
    pass


class AirbyteClient:
    """
    A thin wrapper around Airbyte Cloud’s REST API (v1). 
    Requires:
      • api_url (e.g. "https://api.airbyte.com/v1")
      • api_token
      • workspace_id

    Methods:
      • create_source / check_source
      • create_destination / check_destination
      • discover_schema
      • create_connection / get_connection
    """
    def __init__(self, api_url: str, api_token: str, workspace_id: str):
        self.api_url = api_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        self.workspace_id = workspace_id

    def _post(self, path: str, payload: Dict) -> Dict:
        """Internal helper for POST requests. Raises AirbyteAPIError on non-2xx."""
        full_url = f"{self.api_url}{path}"
        resp = requests.post(full_url, json=payload, headers=self.headers, timeout=60)
        try:
            payload = resp.json()
        except ValueError:
            raise AirbyteAPIError(f"Non-JSON response from {full_url}: {resp.text}")
        if not resp.ok:
            msg = payload.get("message") or payload
            raise AirbyteAPIError(f"Airbyte API POST {path} → HTTP {resp.status_code}: {msg}")
        return payload

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict:
        """Internal helper for GET requests. Raises AirbyteAPIError on non-2xx."""
        full_url = f"{self.api_url}{path}"
        resp = requests.get(full_url, headers=self.headers, params=params, timeout=60)
        try:
            payload = resp.json()
        except ValueError:
            raise AirbyteAPIError(f"Non-JSON response from {full_url}: {resp.text}")
        if not resp.ok:
            msg = payload.get("message") or payload
            raise AirbyteAPIError(f"Airbyte API GET {path} → HTTP {resp.status_code}: {msg}")
        return payload

    # -------------------------------------------------------------------------
    # 1) SOURCES
    # -------------------------------------------------------------------------
    def create_source(self, name: str, definition_id: str, config: Dict) -> str:
        """
        Create a new Airbyte Source.
        Returns the new sourceId.
        """
        payload = {
            "name": name,
            "sourceDefinitionId": definition_id,
            "workspaceId": self.workspace_id,
            "connectionConfiguration": config,
        }
        resp = self._post("/sources/create", payload)
        return resp["sourceId"]

    def check_source(self, source_id: str) -> bool:
        """
        Check (validate) a Source. Returns True if status == "succeeded".
        """
        resp = self._post("/sources/check_connection", {"sourceId": source_id})
        return resp.get("status") == "succeeded"

    # -------------------------------------------------------------------------
    # 2) DESTINATIONS
    # -------------------------------------------------------------------------
    def create_destination(self, name: str, definition_id: str, config: Dict) -> str:
        """
        Create a new Airbyte Destination.
        Returns the new destinationId.
        """
        payload = {
            "name": name,
            "destinationDefinitionId": definition_id,
            "workspaceId": self.workspace_id,
            "connectionConfiguration": config,
        }
        resp = self._post("/destinations/create", payload)
        return resp["destinationId"]

    def check_destination(self, destination_id: str) -> bool:
        """
        Check (validate) a Destination. Returns True if status == "succeeded".
        """
        resp = self._post("/destinations/check_connection", {"destinationId": destination_id})
        return resp.get("status") == "succeeded"

    # -------------------------------------------------------------------------
    # 3) DISCOVER SCHEMA (to build syncCatalog)
    # -------------------------------------------------------------------------
    def discover_schema(
        self,
        source_id: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        tables: Optional[List[str]] = None,
    ) -> Dict:
        """
        Calls /connections/discover_schema to retrieve a Catalog object.
        If `database`, `schema`, and `tables` are provided, it will filter on those.
        """
        payload: Dict = {"sourceId": source_id, "connectorType": "source"}
        if database or schema or tables:
            payload["schema"] = {}
            if database:
                payload["schema"]["database"] = database
            if schema:
                payload["schema"]["schema"] = schema
            if tables:
                payload["schema"]["tables"] = tables
        resp = self._post("/connections/discover_schema", payload)
        return resp  # full discover response, contains “streams” array

    # -------------------------------------------------------------------------
    # 4) CONNECTIONS
    # -------------------------------------------------------------------------
    def create_connection(
        self,
        name: str,
        source_id: str,
        destination_id: str,
        namespace_format: str,
        schedule: Dict,
        sync_catalog: Dict,
        auto_propagate_schema: bool = True,
        status: str = "active",
    ) -> str:
        """
        Create a new Airbyte Connection (CDC or Full). Returns connectionId.
        - name: human‐readable name of the connection
        - source_id / destination_id: IDs from create_source/create_destination
        - namespace_format: e.g. "${SOURCE_NAMESPACE}"
        - schedule: {"units": 5, "timeUnit": "minutes"}
        - sync_catalog: {"streams": [ ... ]} as built from discover_schema
        - auto_propagate_schema: True/False
        - status: "active" or "inactive"
        """
        payload = {
            "name": name,
            "sourceId": source_id,
            "destinationId": destination_id,
            "namespaceFormat": namespace_format,
            "schedule": schedule,
            "syncCatalog": sync_catalog,
            "autoPropagateSchema": auto_propagate_schema,
            "status": status,
        }
        resp = self._post("/connections/create", payload)
        return resp["connectionId"]

    def get_connection(self, connection_id: str) -> Dict:
        """
        GET /connections/get?connectionId=<id>
        Returns the full connection JSON.
        """
        return self._get("/connections/get", params={"connectionId": connection_id})

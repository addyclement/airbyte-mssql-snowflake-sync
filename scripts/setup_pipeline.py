#!/usr/bin/env python3
"""
setup_pipeline.py

Usage (local or CI):
    $ export AIRBYTE_API_TOKEN="<your-airbyte-token>"
    $ export AIRBYTE_WORKSPACE_ID="<your-workspace-id>"
    $ export SQLSERVER_HOST="..."
    $ export SQLSERVER_PORT="1433"
    $ export SQLSERVER_USERNAME="..."
    $ export SQLSERVER_PASSWORD="..."
    $ export SNOWFLAKE_ACCOUNT="abc-xyz"
    $ export SNOWFLAKE_USERNAME="..."
    $ export SNOWFLAKE_PASSWORD="..."
    $ export SNOWFLAKE_ROLE="MY_APP_ROLE"
    $ export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    $ python3 scripts/setup_pipeline.py

You can also call this from GitHub Actions (see .github/workflows/airbyte_setup.yml).
"""

import os
import sys
# Ensure that the “scripts/” directory is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sys
import yaml
import json
from typing import Dict, List

from scripts.airbyte_client import AirbyteClient, AirbyteAPIError


def load_and_render_yaml(path: str) -> Dict:
    """
    Load a YAML file, substitute any ${VARNAME} with os.environ["VARNAME"],
    then return a Python dict.
    """
    raw = open(path, 'r').read()
    # Substitute ${VAR} with environment variables
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError as e:
        print(f"[ERROR] Failed to parse YAML {path}: {e}", file=sys.stderr)
        sys.exit(1)


def build_sync_catalog(
    client: AirbyteClient,
    source_id: str,
    database: str,
    schema: str,
    tables: List[str],
    sync_mode: str,
    dest_sync_mode: str,
) -> Dict:
    """
    1) Calls discover_schema on the MSSQL source (filtered to our 4 tables).
    2) Filters streams so that only the specified tables remain.
    3) Constructs the required `syncCatalog` payload, where each stream is
       incremental and uses append_dedup in Snowflake.
    """
    discover_resp = client.discover_schema(
        source_id=source_id,
        database=database,
        schema=schema,
        tables=tables
    )
    all_streams = discover_resp.get("streams", [])
    if not all_streams:
        raise Exception(f"No streams returned from discover_schema for tables {tables}")

    # Filter only those streams whose `.stream.name` is in our `tables` list.
    filtered = []
    for stream_entry in all_streams:
        name = stream_entry["stream"]["name"]
        if name in tables:
            # Each stream_entry has: 
            #   stream: { name, jsonSchema, supportedSyncModes, sourceDefinedCursor, sourceDefinedPrimaryKey }
            st = {
                "stream": {
                    "name": name,
                    "jsonSchema": stream_entry["stream"]["jsonSchema"],
                    "supportedSyncModes": stream_entry["stream"]["supportedSyncModes"],
                },
                "syncMode": sync_mode,
                "destinationSyncMode": dest_sync_mode,
                # use the CDC cursor that SQL Server defined
                "cursorField": stream_entry["stream"].get("sourceDefinedCursor", []),
                # use the PK that SQL Server defined
                "primaryKey": stream_entry["stream"].get("sourceDefinedPrimaryKey", []),
            }
            filtered.append(st)

    if len(filtered) != len(tables):
        missing = set(tables) - {s["stream"]["name"] for s in filtered}
        raise Exception(f"Some tables were not discovered: {missing}")

    return {"streams": filtered}


def main():
    # -----------------------------
    # 1) load env
    # -----------------------------
    try:
        api_token = os.environ["AIRBYTE_API_TOKEN"]
        workspace_id = os.environ["AIRBYTE_WORKSPACE_ID"]
    except KeyError as e:
        print(f"[ERROR] Missing required environment variable: {e}", file=sys.stderr)
        sys.exit(1)

    # DB‐related env vars (SQL Server + Snowflake)
    required_vars = [
        "SQLSERVER_HOST", "SQLSERVER_PORT", "SQLSERVER_USERNAME", "SQLSERVER_PASSWORD",
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USERNAME", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE"
    ]
    for var in required_vars:
        if var not in os.environ:
            print(f"[ERROR] Missing required environment variable: {var}", file=sys.stderr)
            sys.exit(1)

    # Instantiate Airbyte Client
    api_url = "https://api.airbyte.com/v1"
    client = AirbyteClient(api_url=api_url, api_token=api_token, workspace_id=workspace_id)

    # -----------------------------
    # 2) create + validate MSSQL source
    # -----------------------------
    print("→ Loading MSSQL source config...")
    mssql_cfg = load_and_render_yaml("configs/mssql_source.yaml")
    try:
        src_name = mssql_cfg["name"]
        src_def_id = mssql_cfg["definitionId"]
        src_conn_cfg = mssql_cfg["connectionConfiguration"]
    except KeyError as e:
        print(f"[ERROR] mssql_source.yaml missing key: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Creating MSSQL Source: {src_name} …")
    try:
        source_id = client.create_source(name=src_name, definition_id=src_def_id, config=src_conn_cfg)
    except AirbyteAPIError as e:
        print(f"[ERROR] Failed to create MSSQL source: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Validating MSSQL Source (ID={source_id}) …")
    if not client.check_source(source_id):
        print("[ERROR] MSSQL source check failed.", file=sys.stderr)
        sys.exit(1)
    print(f"✅ MSSQL source created & validated (sourceId={source_id})")

    # -----------------------------
    # 3) create + validate Snowflake destination
    # -----------------------------
    print("→ Loading Snowflake destination config...")
    snow_cfg = load_and_render_yaml("configs/snowflake_destination.yaml")
    try:
        dst_name = snow_cfg["name"]
        dst_def_id = snow_cfg["definitionId"]
        dst_conn_cfg = snow_cfg["connectionConfiguration"]
    except KeyError as e:
        print(f"[ERROR] snowflake_destination.yaml missing key: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Creating Snowflake Destination: {dst_name} …")
    try:
        destination_id = client.create_destination(name=dst_name, definition_id=dst_def_id, config=dst_conn_cfg)
    except AirbyteAPIError as e:
        print(f"[ERROR] Failed to create Snowflake destination: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Validating Snowflake Destination (ID={destination_id}) …")
    if not client.check_destination(destination_id):
        print("[ERROR] Snowflake destination check failed.", file=sys.stderr)
        sys.exit(1)
    print(f"✅ Snowflake destination created & validated (destinationId={destination_id})")

    # -----------------------------
    # 4) build syncCatalog + create connection
    # -----------------------------
    print("→ Loading Connection config (CDC → Snowflake) …")
    conn_cfg = load_and_render_yaml("configs/connection.yaml")
    try:
        conn_name = conn_cfg["name"]
        namespace_format = conn_cfg.get("namespaceFormat", "${SOURCE_NAMESPACE}")
        schedule = conn_cfg["schedule"]
        tables = conn_cfg["tables"]
        sync_mode = conn_cfg["syncMode"]
        dest_sync_mode = conn_cfg["destinationSyncMode"]
        auto_propagate = conn_cfg.get("autoPropagateSchema", True)
    except KeyError as e:
        print(f"[ERROR] connection.yaml missing key: {e}", file=sys.stderr)
        sys.exit(1)

    print("→ Discovering schema for MSSQL source …")
    try:
        sync_catalog = build_sync_catalog(
            client=client,
            source_id=source_id,
            database="LoanDataServices",
            schema="dbo",
            tables=tables,
            sync_mode=sync_mode,
            dest_sync_mode=dest_sync_mode
        )
    except Exception as e:
        print(f"[ERROR] build_sync_catalog failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"→ Creating Connection: {conn_name} (5-minute schedule) …")
    try:
        connection_id = client.create_connection(
            name=conn_name,
            source_id=source_id,
            destination_id=destination_id,
            namespace_format=namespace_format,
            schedule=schedule,
            sync_catalog=sync_catalog,
            auto_propagate_schema=auto_propagate,
            status="active"
        )
    except AirbyteAPIError as e:
        print(f"[ERROR] Failed to create connection: {e}", file=sys.stderr)
        sys.exit(1)

    print("✅ Airbyte CDC pipeline configured successfully!")
    print(f"   • Source ID:       {source_id}")
    print(f"   • Destination ID:  {destination_id}")
    print(f"   • Connection ID:   {connection_id}")
    print("You can now log into Airbyte Cloud and ‘Sync Now’ when ready.")


if __name__ == "__main__":
    main()

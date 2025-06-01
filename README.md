# Airbyte CDC-Only Sync (MSSQL → Snowflake) via Python/OOP & GitHub Actions

This repository shows how to configure a **CDC-only** Airbyte connection from
a Microsoft SQL Server (`LoanDataServices`) to Snowflake (`BRONZE.LoanDataServicesClone`)
using a fully Pythonized, object-oriented approach. The configuration is stored
in YAML files under `configs/`, and a `GitHub Actions` workflow (`.github/workflows/airbyte_setup.yml`)
will invoke a Python script to create the Airbyte Source, Destination, and Connection.

---

## 🗂 Directory Layout

```
├── .github
│   └── workflows
│       └── airbyte_setup.yml        # GitHub Actions workflow
├── configs
│   ├── mssql_source.yaml            # MSSQL source config (CDC mode)
│   ├── snowflake_destination.yaml   # Snowflake destination config
│   └── connection.yaml              # Connection config (5-minute schedule, tables, etc.)
├── scripts
│   ├── airbyte_client.py            # OOP wrapper around Airbyte Cloud API
│   └── setup_pipeline.py            # Entrypoint: loads config, calls AirbyteClient
├── requirements.txt                 # dependencies: requests, PyYAML
└── README.md                        # this file
```

---

## 🔐 Prerequisites & GitHub Secrets

Before you run the workflow (or invoke `setup_pipeline.py` locally), make sure:

1. **You have an Airbyte Cloud workspace** with a valid **API token** and its **workspace ID**.  
   - Store them in GitHub Secrets as `AIRBYTE_API_TOKEN` and `AIRBYTE_WORKSPACE_ID`.

2. **Your SQL Server (`LoanDataServices`)** is already CDC-enabled on the four tables:
   - `dbo.Customers`
   - `dbo.Loan`
   - `dbo.LoanInstallment`
   - `dbo.LoanHistory`

3. **You have a Snowflake account** with a user/role that can create tables under:
   ```
   DATABASE = BRONZE
   SCHEMA   = LoanDataServicesClone
   ```
   - Store the Snowflake connection info in GitHub Secrets as:
     - `SNOWFLAKE_ACCOUNT`      (e.g. `xyz12345` if your URL is `xyz12345.snowflakecomputing.com`)
     - `SNOWFLAKE_USERNAME`
     - `SNOWFLAKE_PASSWORD`
     - `SNOWFLAKE_ROLE`         (the role with `OWNERSHIP` on `BRONZE.LoanDataServicesClone`)
     - `SNOWFLAKE_WAREHOUSE`    (the compute warehouse, e.g. `COMPUTE_WH`)

4. **You also have a SQL Server user** with minimal read+CDC privileges on those four tables:
   - Store in GitHub Secrets:
     - `SQLSERVER_HOST`
     - `SQLSERVER_PORT`         (usually `1433`)
     - `SQLSERVER_USERNAME`
     - `SQLSERVER_PASSWORD`

In total, your GitHub repository needs these **9 secrets**:

```text
  • AIRBYTE_API_TOKEN
  • AIRBYTE_WORKSPACE_ID
  • SQLSERVER_HOST
  • SQLSERVER_PORT
  • SQLSERVER_USERNAME
  • SQLSERVER_PASSWORD
  • SNOWFLAKE_ACCOUNT
  • SNOWFLAKE_USERNAME
  • SNOWFLAKE_PASSWORD
  • SNOWFLAKE_ROLE
  • SNOWFLAKE_WAREHOUSE
```

---

## ⚙️ Configuration Files

1. **`configs/mssql_source.yaml`**  
   Defines the MSSQL source with `replication_method: CDC` and four tables under `dbo`.

2. **`configs/snowflake_destination.yaml`**  
   Defines the Snowflake destination—must match your Snowflake account, role, warehouse, database, schema.

3. **`configs/connection.yaml`**  
   Defines the name of the Airbyte connection, 5-minute schedule, which tables to sync, and CDC vs. full refresh settings.

You can edit these YAMLs to:

- Add more tables under `include_tables` (just mirror them in `connection.yaml`).  
- Change the namespace or schedule if needed.  
- Copy this pattern to create “dev”, “qa”, or any other environment easily by cloning/renaming the YAML files.

---

## 🛠 Running Locally

If you want to run the same code on your local machine (instead of via GitHub Actions):

1. Clone this repo.  
2. Create a Python 3.9+ virtual environment (recommended).  
3. Install dependencies:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. Export environment variables (or create a `.env` and `source` it):

   ```bash
   export AIRBYTE_API_TOKEN="…"
   export AIRBYTE_WORKSPACE_ID="…"
   export SQLSERVER_HOST="…"
   export SQLSERVER_PORT="1433"
   export SQLSERVER_USERNAME="…"
   export SQLSERVER_PASSWORD="…"
   export SNOWFLAKE_ACCOUNT="…"
   export SNOWFLAKE_USERNAME="…"
   export SNOWFLAKE_PASSWORD="…"
   export SNOWFLAKE_ROLE="…"
   export SNOWFLAKE_WAREHOUSE="…"
   ```

5. Run the setup script:

   ```bash
   python scripts/setup_pipeline.py
   ```

6. Log into Airbyte Cloud → Workspace → Connections and verify that:
   - The new Source (“LoanDataServices_MSSQL_CDC”) exists.
   - The new Destination (“BRONZE_LoanDataServicesClone_Snowflake”) exists.
   - The new Connection (“MSSQL_CDC_to_Snowflake_BronzeClone”) is scheduled every 5 minutes and lists exactly four tables with `syncMode: incremental`.

7. When you’re ready to actually move data, click **Sync Now** in the Airbyte UI.  

---

## ✅ Testing & Validation

... (README content continues as above)

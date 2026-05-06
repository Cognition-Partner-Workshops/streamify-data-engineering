# Databricks notebook source

# MAGIC %md
# MAGIC # Run dbt Commands
# MAGIC
# MAGIC Utility notebook invoked by the Databricks Workflow to execute dbt
# MAGIC commands. The `dbt_command` widget is set by the workflow task
# MAGIC parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("dbt_command", "dbt run --profiles-dir . --target prod", "dbt Command")

dbt_command = dbutils.widgets.get("dbt_command")
print(f"Will execute: {dbt_command}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execute dbt
# MAGIC
# MAGIC The dbt project lives at `databricks/dbt/` in the repo.
# MAGIC We `cd` into it and run the requested command.

# COMMAND ----------

import subprocess, sys

result = subprocess.run(
    dbt_command,
    shell=True,
    cwd="/Workspace/Repos/streamify-data-engineering/databricks/dbt",
    capture_output=True,
    text=True,
)

print(result.stdout)
if result.stderr:
    print(result.stderr, file=sys.stderr)

if result.returncode != 0:
    raise RuntimeError(f"dbt command failed with exit code {result.returncode}")

print("dbt command completed successfully.")

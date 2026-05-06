# Databricks notebook source

# MAGIC %md
# MAGIC # Run dbt Commands
# MAGIC
# MAGIC Utility notebook invoked by the Databricks Workflow to execute dbt
# MAGIC commands. Uses structured widget parameters to avoid shell injection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Individual parameters are exposed as widgets so that the workflow
# MAGIC can customise behaviour without allowing arbitrary shell commands.

# COMMAND ----------

dbutils.widgets.dropdown("dbt_subcommand", "run", ["run", "seed", "test", "build", "deps", "debug"], "dbt Subcommand")
dbutils.widgets.text("dbt_target", "prod", "Target Profile")
dbutils.widgets.text("dbt_select", "", "Model Selector (--select)")
dbutils.widgets.dropdown("run_deps_first", "true", ["true", "false"], "Run dbt deps First")

dbt_subcommand = dbutils.widgets.get("dbt_subcommand")
dbt_target = dbutils.widgets.get("dbt_target")
dbt_select = dbutils.widgets.get("dbt_select").strip()
run_deps_first = dbutils.widgets.get("run_deps_first") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build and execute dbt command
# MAGIC
# MAGIC The dbt project lives at `databricks/dbt/` in the repo.
# MAGIC Commands are constructed as argument lists (no shell=True) to
# MAGIC prevent command injection.

# COMMAND ----------

import re, subprocess, sys

DBT_PROJECT_DIR = "/Workspace/Repos/streamify-data-engineering/databricks/dbt"

ALLOWED_SUBCOMMANDS = {"run", "seed", "test", "build", "deps", "debug"}
SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.*+\- ]+$")

if dbt_subcommand not in ALLOWED_SUBCOMMANDS:
    raise ValueError(f"Invalid dbt subcommand: {dbt_subcommand}")
if dbt_target and not re.match(r"^[a-zA-Z0-9_]+$", dbt_target):
    raise ValueError(f"Invalid target name: {dbt_target}")
if dbt_select and not SAFE_IDENTIFIER.match(dbt_select):
    raise ValueError(f"Invalid selector: {dbt_select}")


def run_dbt(args):
    """Run a dbt command and stream output."""
    print(f"Running: dbt {' '.join(args)}")
    result = subprocess.run(
        ["dbt"] + args,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {args[0]} failed with exit code {result.returncode}")


# Run dbt deps first if requested
if run_deps_first:
    run_dbt(["deps"])

# Build the main command
cmd_args = [dbt_subcommand, "--profiles-dir", ".", "--target", dbt_target]
if dbt_select:
    cmd_args.extend(["--select", dbt_select])

# deps and debug don't need --profiles-dir/--target
if dbt_subcommand in ("deps", "debug"):
    cmd_args = [dbt_subcommand]

run_dbt(cmd_args)
print("dbt command completed successfully.")

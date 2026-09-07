# =========================================================
# Runtime configuration
# =========================================================
#
# Example config file contents:
# {
#     "workspaceId": "00000000-0000-0000-0000-000000000000",
#     "sourceLakehouseId": "11111111-1111-1111-1111-111111111111",
#     "targetLakehouseId": "22222222-2222-2222-2222-222222222222"
# }
#
# =========================================================

import sys
import json

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

LH_SCHEMA = "dbo"

BRONZE_TABLE = "bronze_order"

HUB_TABLE = "order_hub"
DI_TABLE = "order_di"
SAT_TABLE = "order_sat"

WATERMARK_TABLE = "pipeline_watermark"
PIPELINE_NAME = "order_bronze_to_silver"


def _build_abfss_base(workspace_id, lakehouse_id):
    return f"abfss://{workspace_id}@{ONELAKE_HOST}/{lakehouse_id}"


def _read_config_text(spark, config_file_path):
    """
    Reads the raw text of the config file. Supports both a locally
    mounted path (e.g. the default attached Lakehouse Files mount,
    such as /lakehouse/default/Files/job_config.json) and a direct
    ABFSS/OneLake path, which is read through Spark.
    """
    if config_file_path.startswith("abfss://") or config_file_path.startswith("wasbs://"):
        return "\n".join(
            row["value"] for row in spark.read.text(config_file_path).collect()
        )

    with open(config_file_path, "r") as f:
        return f.read()


def load_runtime_config(spark):
    """
    Reads the config JSON file path from sys.argv[1] and returns a dict
    with the workspace/lakehouse IDs plus every ABFSS path required by
    the pipeline (the sample data Files path, plus one Tables path per
    Delta table).
    """
    if len(sys.argv) < 2:
        raise ValueError(
            "Missing required command-line argument: path to the config "
            "JSON file.\nUsage: main.py <config_file_path>"
        )

    config_file_path = sys.argv[1]

    raw_config = json.loads(_read_config_text(spark, config_file_path))

    required_keys = ["workspaceId", "sourceLakehouseId", "targetLakehouseId"]
    missing_keys = [k for k in required_keys if k not in raw_config]
    if missing_keys:
        raise ValueError(
            f"Config file '{config_file_path}' is missing required "
            f"key(s): {missing_keys}"
        )

    workspace_id = raw_config["workspaceId"]
    source_lakehouse_id = raw_config["sourceLakehouseId"]
    target_lakehouse_id = raw_config["targetLakehouseId"]

    source_lakehouse_abfss = _build_abfss_base(workspace_id, source_lakehouse_id)
    target_lakehouse_abfss = _build_abfss_base(workspace_id, target_lakehouse_id)

    return {
        "WORKSPACE_ID": workspace_id,
        "SOURCE_LAKEHOUSE_ID": source_lakehouse_id,
        "TARGET_LAKEHOUSE_ID": target_lakehouse_id,

        "SOURCE_LAKEHOUSE_ABFSS": source_lakehouse_abfss,
        "TARGET_LAKEHOUSE_ABFSS": target_lakehouse_abfss,

        "SAMPLE_DATA_PATH": f"{source_lakehouse_abfss}/Files/sample_data",

        "BRONZE_TABLE_PATH": f"{target_lakehouse_abfss}/Tables/{LH_SCHEMA}/{BRONZE_TABLE}",
        "HUB_TABLE_PATH": f"{target_lakehouse_abfss}/Tables/{LH_SCHEMA}/{HUB_TABLE}",
        "DI_TABLE_PATH": f"{target_lakehouse_abfss}/Tables/{LH_SCHEMA}/{DI_TABLE}",
        "SAT_TABLE_PATH": f"{target_lakehouse_abfss}/Tables/{LH_SCHEMA}/{SAT_TABLE}",
        "WATERMARK_TABLE_PATH": f"{target_lakehouse_abfss}/Tables/{LH_SCHEMA}/{WATERMARK_TABLE}",
    }

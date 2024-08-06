import os
import yaml

import snowflake.connector

from titan import Blueprint
from titan.blueprint import RunMode, print_plan
from titan.enums import ResourceType
from titan.gitops import collect_resources_from_config


def crawl(path: str):
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yield os.path.join(root, file)


def collect_resources(path: str):
    resources = []
    files_read = 0

    if os.path.isfile(path):
        # Handle single file
        if path.endswith((".yaml", ".yml")):
            print(f"Reading config file: {path}")
            with open(path, "r") as f:
                config = yaml.safe_load(f)
                if config:
                    resources.extend(collect_resources_from_config(config))
                    files_read += 1
                else:
                    print(f"Skipping empty config file: {path}")
    elif os.path.isdir(path):
        # Handle directory
        for file in crawl(path):
            with open(file, "r") as f:
                print(f"Reading config file: {file}")
                config = yaml.safe_load(f)
                if config:
                    resources.extend(collect_resources_from_config(config))
                    files_read += 1
                else:
                    print(f"Skipping empty config file: {file}")
    else:
        raise ValueError(f"Invalid path: {path}. Must be a file or directory.")

    if files_read == 0:
        raise ValueError(f"No valid YAML files were read from the given path: {path}")

    return resources


def str_to_bool(s: str) -> bool:
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")
    return s == "true"


def main():
    # Bootstrap environment
    try:
        connection_params = {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USERNAME"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "role": os.environ["SNOWFLAKE_ROLE"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        }
        workspace = os.environ["GITHUB_WORKSPACE"]

        # Inputs
        run_mode = os.environ["INPUT_RUN-MODE"]
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        resource_path = os.environ["INPUT_RESOURCE-PATH"]
        allowlist = os.environ.get("INPUT_ALLOWLIST", "all")
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e

    # Parse inputs
    run_mode = RunMode(run_mode)

    if allowlist == "all":
        allowlist = []
    else:
        allowlist = [ResourceType(r) for r in allowlist.split(",")]

    # Print config
    print("Config\n------")
    print(f"\t run_mode: {run_mode}")
    print(f"\t allowlist: {allowlist}")
    print(f"\t dry_run: {os.environ['INPUT_DRY-RUN']} => {dry_run}")
    print(f"\t resource_path: {resource_path}")
    print(f"\t workspace: {workspace}")

    resources = collect_resources(os.path.join(workspace, resource_path))

    blueprint = Blueprint(
        name="snowflake-gitops",
        resources=resources,
        run_mode=run_mode,
        dry_run=dry_run,
        allowlist=allowlist,
    )
    print(resources)
    conn = snowflake.connector.connect(**connection_params)
    plan = blueprint.plan(conn)
    print_plan(plan)
    blueprint.apply(conn, plan)


if __name__ == "__main__":
    main()

import os
import yaml

import snowflake.connector

from titan import Blueprint
from titan.blueprint import print_plan
from titan.gitops import collect_resources_from_config


def crawl(path: str):
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yield os.path.join(root, file)


def collect_resources(path: str):
    resources = []

    for file in crawl(path):
        with open(file, "r") as f:
            config = yaml.safe_load(f)
            resources.extend(collect_resources_from_config(config))

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
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        resource_path = os.environ["INPUT_RESOURCE-PATH"]
        allowed_resources = os.environ.get("INPUT_ALLOWED-RESOURCES", "all")
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e

    print("Config\n------")
    print(f"\t allowed_resources: {allowed_resources}")
    print(f"\t dry_run: {os.environ['INPUT_DRY-RUN']} => {dry_run}")
    print(f"\t resource_path: {resource_path}")
    print(f"\t workspace: {workspace}")

    resources = collect_resources(os.path.join(workspace, resource_path))
    blueprint = Blueprint(name="snowflake-gitops", resources=resources, dry_run=dry_run)
    print(resources)
    conn = snowflake.connector.connect(**connection_params)
    plan = blueprint.plan(conn)
    print_plan(plan)
    blueprint.apply(conn, plan)


if __name__ == "__main__":
    main()

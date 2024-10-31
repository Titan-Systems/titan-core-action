import json
import os
from typing import Optional

from titan import Blueprint
from titan.blueprint import print_plan
from titan.gitops import (
    collect_blueprint_config,
    collect_configs_from_path,
    collect_vars_from_environment,
    merge_configs,
    merge_vars,
)
from titan.operations.connector import connect


def str_to_bool(s: str) -> bool:
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")
    return s == "true"


def str_to_json(s: Optional[str]) -> Optional[dict]:
    if s is None or s == "" or s == "None":
        return None

    return json.loads(s)


def main():
    # Bootstrap environment
    try:
        workspace = os.environ["GITHUB_WORKSPACE"]

        # Inputs
        run_mode = os.environ["INPUT_RUN-MODE"]
        resource_path = os.environ["INPUT_RESOURCE-PATH"]
        allowlist = os.environ.get("INPUT_ALLOWLIST", "all")
        vars = str_to_json(os.environ.get("INPUT_VARS", None))
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        scope = os.environ.get("INPUT_SCOPE", None)
        database = os.environ.get("INPUT_DATABASE", None)
        schema = os.environ.get("INPUT_SCHEMA", None)
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e

    action_config = {
        "run_mode": run_mode,
        "resource_path": resource_path,
        "allowlist": allowlist,
        "vars": vars,
        "dry_run": dry_run,
        "scope": scope,
        "database": database,
        "schema": schema,
    }

    env_vars = collect_vars_from_environment()
    if env_vars:
        action_config["vars"] = merge_vars(action_config.get("vars", {}), env_vars)

    print("Configuration\n------")
    print(f"\t run_mode: {run_mode}")
    print(f"\t resource_path: {resource_path}")
    print(f"\t allowlist: {allowlist}")
    print(f"\t dry_run: {dry_run}")
    print(f"\t scope: {scope}")
    print(f"\t database: {database}")
    print(f"\t schema: {schema}")
    print(f"\t workspace: {workspace}")

    if action_config["vars"]:
        print("Vars\n------")
        for key in action_config["vars"].keys():
            print(f"\t {key}")

    configs = collect_configs_from_path(os.path.join(workspace, resource_path))
    yaml_config = {}

    print("Resource files\n------")
    for config in configs:
        print(f"\t{config[0]}")
        yaml_config = merge_configs(yaml_config, config[1])

    blueprint_config = collect_blueprint_config(yaml_config, action_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    plan = blueprint.plan(session)
    print_plan(plan)
    blueprint.apply(session, plan)


if __name__ == "__main__":
    main()

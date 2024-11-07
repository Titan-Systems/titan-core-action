import json
import os
import sys
from typing import Optional

from titan import Blueprint
from titan.blueprint import print_plan
from titan.gitops import (
    collect_blueprint_config,
    collect_configs_from_path,
    collect_vars_from_environment,
    merge_configs,
    merge_vars,
    parse_resources,
)
from titan.operations.connector import connect

sys.stdout.reconfigure(line_buffering=True)


def str_to_bool(s: str) -> bool:
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")
    return s == "true"


def str_to_json(s: Optional[str]) -> Optional[dict]:
    if s is None or s == "" or s == "None":
        return None

    return json.loads(s)


def to_str(s: Optional[str]) -> Optional[str]:
    if s is None or s == "None" or s == "":
        return None
    return s


def pretty_print_allowlist(allowlist: Optional[list]) -> str:
    if allowlist is None:
        return "all"
    return ", ".join([resource_type.value for resource_type in allowlist])


def main():
    # Bootstrap environment
    try:
        workspace = os.environ["GITHUB_WORKSPACE"]

        # Inputs
        run_mode = os.environ["INPUT_RUN-MODE"]
        resource_path = os.environ["INPUT_RESOURCE-PATH"]
        allowlist = parse_resources(os.environ.get("INPUT_ALLOWLIST", "all"))
        vars = str_to_json(os.environ.get("INPUT_VARS", None))
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        scope = to_str(os.environ.get("INPUT_SCOPE", None))
        database = to_str(os.environ.get("INPUT_DATABASE", None))
        schema = to_str(os.environ.get("INPUT_SCHEMA", None))
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
        action_config["vars"] = merge_vars(action_config["vars"] or {}, env_vars)

    print("\nConfiguration\n----------------")
    print(f"  run_mode:      {run_mode}")
    print(f"  resource_path: {resource_path}")
    print(f"  allowlist:     {pretty_print_allowlist(allowlist)}")
    print(f"  dry_run:       {dry_run}")
    print(f"  scope:         {scope}")
    print(f"  database:      {database or '---'}")
    print(f"  schema:        {schema or '---'}")
    print(f"  workspace:     {workspace}")

    if action_config["vars"]:
        print("\nVars\n----------------")
        for key in action_config["vars"].keys():
            print(f"  {key}")

    configs = collect_configs_from_path(os.path.join(workspace, resource_path))
    yaml_config = {}

    print("\nResource files\n----------------")
    for config in configs:
        file_path, config_dict = config
        print(f"  {file_path.lstrip(workspace).lstrip('/')}")
        yaml_config = merge_configs(yaml_config, config_dict)

    print("\n\n")

    blueprint_config = collect_blueprint_config(yaml_config, action_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    plan = blueprint.plan(session)
    print_plan(plan)
    blueprint.apply(session, plan)


if __name__ == "__main__":
    main()

import os
import yaml

import snowflake.connector

from titan import Blueprint
from titan.resources import Database, Warehouse, Role, User, RoleGrant


def crawl(path: str):
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yield os.path.join(root, file)


def collect_resources_from_config(config: dict):
    databases = config.get("databases", [])
    role_grants = config.get("role_grants", [])
    roles = config.get("roles", [])
    users = config.get("users", [])
    warehouses = config.get("warehouses", [])

    databases = [Database(**database) for database in databases]
    users = [User(**user) for user in users]
    roles = [Role(**role) for role in roles]
    # role_grants = [RoleGrant(**role_grant) for role_grant in role_grants]
    warehouses = [Warehouse(**warehouse) for warehouse in warehouses]

    return (
        *databases,
        *role_grants,
        *roles,
        *users,
        *warehouses,
    )


def collect_resources(path: str):
    resources = []

    for file in crawl(path):
        with open(file, "r") as f:
            config = yaml.safe_load(f)
            resources.extend(collect_resources_from_config(config))

    return resources


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
        resource_path = os.environ["INPUT_RESOURCE-PATH"]
        allowed_resources = os.environ.get("INPUT_ALLOWED-RESOURCES", "all")
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e

    print(f"Workspace: [{workspace}]")
    print(f"Resource Path: {resource_path}")
    print(f"Allowed Resources: {allowed_resources}")

    resources = collect_resources(os.path.join(workspace, resource_path))
    blueprint = Blueprint(name="snowflake-gitops", resources=resources)
    conn = snowflake.connector.connect(**connection_params)
    blueprint.plan(conn)
    blueprint.apply(conn)


if __name__ == "__main__":
    main()

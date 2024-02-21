import os

import snowflake.connector


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

    conn = snowflake.connector.connect(**connection_params)
    print(f"Hello, World! [{workspace}]:{conn.role}")
    print(f"Resource Path: {resource_path}")
    print(f"Allowed Resources: {allowed_resources}")


if __name__ == "__main__":
    main()

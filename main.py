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
        resource_path = os.environ["resource-path"]
        allowed_resources = os.environ.get("allowed-resources", "all")
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e

    conn = snowflake.connector.connect(**connection_params)
    print(f"Hello, World! [{workspace}]:{conn.role}")


if __name__ == "__main__":
    main()

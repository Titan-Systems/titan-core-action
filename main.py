import os

import snowflake.connector


connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USERNAME"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}


def main():
    workspace = os.environ.get("GITHUB_WORKSPACE")
    conn = snowflake.connector.connect(**connection_params)
    print(f"Hello, World! [{workspace}]:{conn.role}")


if __name__ == "__main__":
    main()

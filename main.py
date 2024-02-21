import os

import snowflake.connector


def main():
    workspace = os.environ.get("GITHUB_WORKSPACE")
    print(f"Hello, World! [{workspace}]")


if __name__ == "__main__":
    main()

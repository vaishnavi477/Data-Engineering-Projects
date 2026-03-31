from prefect import flow, task
from datetime import timedelta
import subprocess
import os
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
GENERATE_DATA_SCRIPT = os.getenv("GENERATE_DATA_SCRIPT")
DEID_SCRIPT = os.getenv("DEID_SCRIPT")
LOAD_PSQL_SCRIPT = os.getenv("LOAD_PSQL_SCRIPT")
GE_VALIDATE_SCRIPT = os.getenv("GE_VALIDATE_SCRIPT")
EXPORT_CURATED_SCRIPT = os.getenv("EXPORT_CURATED_SCRIPT")
DBT_DIR = os.getenv("DBT_DIR")

# ---------------------------
# Helper function
# ---------------------------
def run_command(command, cwd=None):
    """
    Run a shell command safely.
    Supports shell=True for string commands and cwd for working directory.
    """
    result = subprocess.run(
        command,
        shell=True,
        cwd=cwd,
        capture_output=True,
        text=True,
        executable="/bin/bash"
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Command failed: {command}")

# ---------------------------
# TASKS
# ---------------------------
@task(retries=2, retry_delay_seconds=10)
def generate_data():
    script_path = os.path.join(BASE_PATH, GENERATE_DATA_SCRIPT)
    run_command(f'python3 "{script_path}"')

@task(retries=2, retry_delay_seconds=10)
def deidentify_data():
    script_path = os.path.join(BASE_PATH, DEID_SCRIPT)
    run_command(f'python3 "{script_path}"')

@task(retries=2, retry_delay_seconds=10)
def load_to_postgres():
    script_path = os.path.join(BASE_PATH, LOAD_PSQL_SCRIPT)
    run_command(f'python3 "{script_path}"')

@task(retries=2, retry_delay_seconds=15)
def dbt_run():
    dbt_path = os.path.join(BASE_PATH, DBT_DIR)
    run_command("dbt run", cwd=dbt_path)

@task(retries=2, retry_delay_seconds=15)
def dbt_test():
    dbt_path = os.path.join(BASE_PATH, DBT_DIR)
    run_command("dbt test", cwd=dbt_path)

@task(retries=2, retry_delay_seconds=10)
def export_curated():
    script_path = os.path.join(BASE_PATH, EXPORT_CURATED_SCRIPT)
    run_command(f'python3 "{script_path}"')

@task(retries=2, retry_delay_seconds=10)
def ge_validate():
    script_path = os.path.join(BASE_PATH, GE_VALIDATE_SCRIPT)
    run_command(f'python3 "{script_path}"')

# ---------------------------
# FLOW
# ---------------------------
@flow(name="healthcare-data-pipeline")
def healthcare_pipeline():
    print("Starting Healthcare Data Pipeline...")

    generate_data()
    deidentify_data()
    load_to_postgres()

    dbt_run()
    dbt_test()

    export_curated()
    ge_validate()

    print("Pipeline completed successfully!")

# ---------------------------
# ENTRY POINT
# ---------------------------
if __name__ == "__main__":
    healthcare_pipeline()

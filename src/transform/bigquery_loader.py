import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import ClientError
from loguru import logger

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# --- Logging Configuration ---
# Configure Loguru for clear and informative logging
logger.add("logs/bigquery_loader.log", rotation="10 MB", level="DEBUG")
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
)


@logger.catch
def load_csv_to_bigquery(
    project_id: str, dataset_id: str, table_id: str, file_path: Path
) -> None:
    """
    Loads a CSV file into a BigQuery table.

    Args:
        project_id: The Google Cloud project ID.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.
        file_path: The path to the CSV file.
    """
    logger.info(
        f"Initiating load for {file_path.name} to table {project_id}.{dataset_id}.{table_id}"
    )

    # --- BigQuery Client Initialization ---
    try:
        client = bigquery.Client(project=project_id)
        logger.debug("BigQuery client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise

    # --- Job Configuration ---
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table if it exists
    )
    logger.debug(
        "Load job configured with schema auto-detection and write-truncate disposition."
    )

    # --- File Upload and Load Job ---
    try:
        with open(file_path, "rb") as source_file:
            logger.debug(f"Opening file {file_path.name} for loading.")
            load_job = client.load_table_from_file(
                source_file,
                f"{project_id}.{dataset_id}.{table_id}",
                job_config=job_config,
            )
            logger.info(
                f"Starting BigQuery load job {load_job.job_id} for {file_path.name}"
            )

        # --- Wait for Job Completion ---
        load_job.result()  # Waits for the job to complete.
        logger.success(
            f"Successfully loaded {file_path.name} to {project_id}.{dataset_id}.{table_id}"
        )

    except ClientError as e:
        logger.error(f"A client error occurred during the BigQuery load job: {e}")
        # Log detailed error information if available
        if e.errors:
            for error in e.errors:
                logger.error(f"Reason: {error['reason']}, Message: {error['message']}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise


def main():
    """
    Main function to find CSV files and load them into BigQuery.
    """
    logger.info("Starting the BigQuery data loading process.")

    # --- Get Configuration from Environment Variables ---
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")

    if not project_id or not dataset_id:
        logger.error(
            "GOOGLE_CLOUD_PROJECT_ID and BIGQUERY_DATASET_ID must be set in the .env file."
        )
        return

    # --- Define Data Directory ---
    # Assuming the script is run from the root of the project
    clean_data_dir = Path("data/clean")
    if not clean_data_dir.is_dir():
        logger.error(f"Directory not found: {clean_data_dir}")
        return

    # --- Find and Process CSV Files ---
    csv_files = list(clean_data_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {clean_data_dir}.")
        return

    logger.info(f"Found {len(csv_files)} CSV files to process.")

    for csv_file in csv_files:
        table_id = csv_file.stem  # Use the filename without extension as the table name
        try:
            load_csv_to_bigquery(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                file_path=csv_file,
            )
        except Exception as e:
            logger.error(f"Failed to process {csv_file.name}. Moving to the next file.")
            # Continue with the next file
            pass

    logger.info("BigQuery data loading process finished.")


if __name__ == "__main__":
    main()

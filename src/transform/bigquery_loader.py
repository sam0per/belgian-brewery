import os
import csv
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import ClientError
from loguru import logger

# --- Configuration (remains the same) ---
load_dotenv()
logger.add("logs/bigquery_loader.log", rotation="10 MB", level="DEBUG")
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
)


# --- NEW HELPER FUNCTION ---
@logger.catch
def get_schema_from_csv(file_path: Path) -> list[bigquery.SchemaField]:
    """
    Reads the header row of a CSV file and creates a BigQuery schema.

    Args:
        file_path: The path to the CSV file.

    Returns:
        A list of SchemaField objects for the BigQuery table.
    """
    logger.info(f"Generating schema from header of {file_path.name}...")
    try:
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader)

        if not header:
            raise ValueError("CSV header is empty.")

        # For simplicity and robustness, we'll default all columns to STRING type.
        # BigQuery is good at handling data in string format.
        schema = [bigquery.SchemaField(name, "STRING") for name in header]
        logger.success(
            f"Schema generated with {len(schema)} columns: {', '.join(header)}"
        )
        return schema

    except FileNotFoundError:
        logger.error(f"CSV file not found at {file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read or generate schema from CSV header: {e}")
        raise


@logger.catch
def load_csv_to_bigquery(
    project_id: str, dataset_id: str, table_id: str, file_path: Path
) -> None:
    """
    Loads a CSV file into a BigQuery table, using the CSV header for column names.
    """
    logger.info(
        f"Initiating load for {file_path.name} to table {project_id}.{dataset_id}.{table_id}"
    )

    try:
        # --- 1. Get the schema from the CSV header ---
        schema = get_schema_from_csv(file_path)

        # --- 2. Initialize BigQuery Client ---
        client = bigquery.Client(project=project_id)
        logger.debug("BigQuery client initialized successfully.")

        # --- 3. Configure the Load Job (UPDATED) ---
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table if it exists
        )
        logger.debug("Load job configured with explicit schema from CSV header.")

        # --- 4. Upload File and Run Load Job ---
        with open(file_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                f"{project_id}.{dataset_id}.{table_id}",
                job_config=job_config,
            )
            logger.info(
                f"Starting BigQuery load job {load_job.job_id} for {file_path.name}"
            )

        load_job.result()  # Waits for the job to complete.
        logger.success(
            f"Successfully loaded {file_path.name} to {project_id}.{dataset_id}.{table_id}"
        )

    except ClientError as e:
        logger.error(f"A client error occurred during the BigQuery load job: {e}")
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

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")

    if not project_id or not dataset_id:
        logger.error(
            "GOOGLE_CLOUD_PROJECT_ID and BIGQUERY_DATASET_ID must be set in the .env file."
        )
        return

    clean_data_dir = Path("data/clean")
    if not clean_data_dir.is_dir():
        logger.error(f"Directory not found: {clean_data_dir}")
        return

    csv_files = list(clean_data_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {clean_data_dir}.")
        return

    logger.info(f"Found {len(csv_files)} CSV files to process.")

    for csv_file in csv_files:
        table_id = csv_file.stem
        try:
            load_csv_to_bigquery(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                file_path=csv_file,
            )
        except Exception as e:
            logger.error(f"Failed to process {csv_file.name}. Moving to the next file.")
            pass

    logger.info("BigQuery data loading process finished.")


if __name__ == "__main__":
    main()

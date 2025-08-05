from pathlib import Path

import ollama
import pandas as pd
from loguru import logger
from tqdm import tqdm

# --- Configuration ---
logger.add("logs/llm_geocoder.log", rotation="10 MB", level="DEBUG")
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
    level="INFO",
)

# --- LLM and File Configuration ---
OLLAMA_MODEL = "wizardlm2:7b"
INPUT_FILE = Path("data/clean/wiki_be_brewery_addresses.csv")

# Ensure the input file exists from the geodata_catcher step
if not INPUT_FILE.exists():
    logger.error(
        f"Input file not found at {INPUT_FILE}. Please run the geodata_catcher script first."
    )
    exit()


@logger.catch
def get_address_with_llm(brewery_name: str) -> str | None:
    """
    Uses a local LLM via Ollama to find the address of a Belgian brewery
    and return it in a specific, structured format.

    Args:
        brewery_name: The name of the brewery.

    Returns:
        The address in "municipality, postcode, province" format, or None.
    """
    logger.info(
        f"Attempting to find address for '{brewery_name}' using LLM ({OLLAMA_MODEL})..."
    )

    # --- Prompt ---
    # This prompt is highly specific about the required output format.
    prompt = f"""
    You are a data extraction assistant. Your task is to find the address of a Belgian brewery and then extract ONLY its municipality, postcode, and province.

    The input brewery is: "{brewery_name}".

    Your output MUST be as short as possible and in the EXACT format: `municipality, postcode, province`

    AVOID AT ALL COSTS:
    - your own commentary
    - any additional information
    - any explanatory notes
    - the street names
    - street number
    - country
    - brewery name

    Example of correct output:
    If you find the address for 'Brouwerij De Halve Maan' is 'Walplein 26, 8000 Brugge, West-Vlaanderen, Belgium', you must return ONLY:
    `Brugge, 8000, West-Vlaanderen`

    If you cannot find the municipality, postcode, and province, return the exact string "Not Found".
    """

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL, messages=[{"role": "user", "content": prompt}]
        )

        address = response["message"]["content"].strip()

        # Check for failure cases
        if "not found" in address.lower() or not address or len(address.split(",")) < 3:
            logger.warning(
                f"LLM could not find a valid structured address for '{brewery_name}'. Response: '{address}'"
            )
            return None

        # Remove any potential quotes the LLM might add around the output
        address = address.strip('"')

        logger.success(
            f"LLM extracted structured address for '{brewery_name}': {address}"
        )
        return address

    except Exception as e:
        logger.error(
            f"An error occurred while contacting Ollama for '{brewery_name}': {e}"
        )
        logger.error(
            "Please ensure the Ollama application is running and the model is downloaded."
        )
        return None


def main():
    """
    Main function to find and fill missing brewery addresses using an LLM.
    """
    logger.info("Starting LLM geocoding process for missing addresses.")

    # --- 1. Load Data ---
    try:
        df = pd.read_csv(INPUT_FILE)
        logger.info(f"Loaded {len(df)} rows from {INPUT_FILE}.")
    except Exception as e:
        logger.error(f"Could not read the input file: {e}")
        return

    # --- 2. Find Breweries with Missing Addresses ---
    missing_address_df = df[df["full_address"].isnull() | (df["full_address"] == "")]

    if missing_address_df.empty:
        logger.info("No breweries with missing addresses found. Nothing to do.")
        return

    breweries_to_find = missing_address_df["brewery_name"].unique()
    logger.info(
        f"Found {len(breweries_to_find)} unique breweries with missing addresses."
    )

    # --- 3. Process the First 3 Breweries ---
    logger.info("Processing the first 3 breweries as a test run...")

    # Use tqdm to show a progress bar, which is useful for long-running tasks
    for brewery_name in tqdm(breweries_to_find[:3], desc="Querying LLM"):
        # The get_address_with_llm function handles its own logging for success/failure
        get_address_with_llm(brewery_name)
        print("-" * 20)  # Separator for clarity

    logger.info("LLM geocoding test run complete.")


if __name__ == "__main__":
    main()

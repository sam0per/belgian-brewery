import re
from pathlib import Path

import pandas as pd
from loguru import logger

# --- Configuration (remains the same) ---
logger.add("logs/wiki_brewery_cleaner.log", rotation="10 MB", level="DEBUG")
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
    level="INFO",
)

RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")
INPUT_FILE = RAW_DATA_DIR / "wiki_be_beers_breweries_provinces.csv"


def clean_brewery_name(name: str) -> str:
    """
    Applies a series of cleaning rules to a single brewery name.
    The order of these rules is important.
    """
    if not isinstance(name, str):
        return name

    cleaned_name = name

    # --- Pre-processing ---
    # Rule 1: Remove leading/trailing quotes and whitespace
    cleaned_name = cleaned_name.strip().strip('"')

    # --- RULE for Alken-Maes ---
    # Rule 2: Handle specific, high-priority standardizations
    # This now catches "Alken Maes" and "Alken-Maes"
    if re.search(r"\bAlken[- ]Maes\b", cleaned_name, re.IGNORECASE):
        return "Alken-Maes"
    cleaned_name = re.sub(
        r"\b(ab-?inbev|inbev)\b", "AB InBev", cleaned_name, flags=re.IGNORECASE
    )

    # Rule 3: Handle comma-separated lists and collaborations
    if "collaboration brew" in cleaned_name.lower():
        parts = cleaned_name.split(",")
        if len(parts) > 1:
            cleaned_name = parts[1]
    elif "," in cleaned_name:
        cleaned_name = cleaned_name.split(",")[0]

    # Rule 4: Handle separator phrases to remove annotations
    separator_pattern = re.compile(
        r"\s*\(?vroeger|\s*inopdracht van|\s+in\s+De Proefbrouwerij|\s+voor|\s+brewed for|\s+gebrouwen|\s+in opdracht van|\s+bij|\s+nu|\s+later|\s+door",
        re.IGNORECASE,
    )
    match = separator_pattern.search(cleaned_name)
    if match:
        cleaned_name = cleaned_name[: match.start()]

    # --- Post-processing and refinement ---
    # Rule 5: Remove any remaining text in parentheses
    cleaned_name = re.sub(r"\s*\([^)]*\)?", "", cleaned_name)

    # Rule 6: Remove consecutive duplicate words
    cleaned_name = re.sub(r"\b(\w+)\s+\1\b", r"\1", cleaned_name, flags=re.IGNORECASE)

    # Rule 7: Clean up leading and trailing characters
    cleaned_name = cleaned_name.strip().rstrip(")-.,")

    if cleaned_name.lower().startswith("'t ") or cleaned_name.lower().startswith("‘t "):
        cleaned_name = cleaned_name[1:]

    # Final cleanup
    cleaned_name = " ".join(cleaned_name.split())

    return cleaned_name.strip()


def main():
    """
    Main function to clean brewery names, enforce data integrity, standardize
    capitalization, and split the data into two separate files.
    """
    logger.info("Starting brewery name cleaning and data splitting process...")

    # --- 1. Load Data ---
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    try:
        df = pd.read_csv(INPUT_FILE)
        logger.info(f"Successfully loaded {len(df)} rows from {INPUT_FILE}.")
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        return

    # --- 2. Clean Brewery Names (Content) ---
    logger.info("Applying content cleaning rules to 'brewery_name' column...")
    df["brewery_name"] = df["brewery_name"].apply(clean_brewery_name)
    logger.success("Content cleaning complete.")

    # --- NEW STEP: Enforce data integrity for specific breweries ---
    logger.info("Enforcing data integrity rules...")
    df.loc[df["brewery_name"] == "Alken-Maes", "province"] = "Limburg"
    logger.success("Data integrity rules applied.")

    # --- 4. Standardize Brewery Name Capitalization ---
    logger.info("Standardizing brewery name capitalization...")
    df["brewery_name"] = df.groupby(df["brewery_name"].str.lower())[
        "brewery_name"
    ].transform("first")
    logger.success("Capitalization standardized.")

    # Ensure the output directory exists
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # --- 5. Create and Save Beer Data File ---
    try:
        beer_columns = ["beer_name", "style_name", "abv_pct", "brewery_name"]
        beer_df = df[beer_columns]
        beer_output_file = CLEAN_DATA_DIR / "wiki_be_beers.csv"
        beer_df.to_csv(beer_output_file, index=False)
        logger.success(
            f"Successfully saved beer data ({len(beer_df)} rows) to {beer_output_file}"
        )
    except KeyError as e:
        logger.error(
            f"Failed to create beer file. Missing column: {e}. Please check the input file."
        )

    # --- 6. Create and Save Brewery Data File ---
    try:
        brewery_columns = ["brewery_name", "province"]
        brewery_df = df[brewery_columns]

        brewery_df = brewery_df.dropna(subset=["brewery_name"])
        brewery_df = brewery_df[brewery_df["brewery_name"] != ""]

        unique_breweries_df = brewery_df.drop_duplicates(
            subset=["brewery_name"]
        ).sort_values("brewery_name")

        brewery_output_file = CLEAN_DATA_DIR / "wiki_be_breweries.csv"
        unique_breweries_df.to_csv(brewery_output_file, index=False)
        logger.success(
            f"Successfully saved unique brewery data ({len(unique_breweries_df)} rows) to {brewery_output_file}"
        )
    except KeyError as e:
        logger.error(
            f"Failed to create brewery file. Missing column: {e}. Please check the input file."
        )


if __name__ == "__main__":
    main()

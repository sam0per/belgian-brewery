import time
from pathlib import Path

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.location import Location
from loguru import logger
from tqdm import tqdm

# --- Configuration ---
logger.add("logs/geodata_catcher.log", rotation="10 MB", level="DEBUG")
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
    level="INFO",
)

INPUT_FILE = Path("data/clean/wiki_be_beers_breweries_provinces.csv")
OUTPUT_FILE = Path(
    "data/clean/be_brewery_addresses.csv"
)


@logger.catch
def get_brewery_location(geolocator: Nominatim, brewery_name: str) -> Location | None:
    """
    Geocodes a brewery name to get its full location object.

    Args:
        geolocator: An instance of a geopy geolocator.
        brewery_name: The name of the brewery to find.

    Returns:
        The full geopy Location object, or None if not found.
    """
    try:
        query = f"{brewery_name}, Belgium"
        location = geolocator.geocode(query, addressdetails=True)
        location = location if isinstance(location, Location) or location is None else None

        time.sleep(1)  # Respect Nominatim's usage policy

        if location:
            logger.debug(f"Found location for '{brewery_name}'")
            return location
        else:
            logger.warning(f"Could not find location for '{brewery_name}'")
            return None
    except Exception as e:
        logger.error(f"An error occurred while geocoding '{brewery_name}': {e}")
        return None


def parse_location_data(brewery_name: str, location: Location | None) -> dict:
    """
    Parses a geopy Location object to extract structured address components.

    Args:
        brewery_name: The name of the brewery (for context).
        location: The geopy Location object.

    Returns:
        A dictionary containing structured address information.
    """
    # Start with default values for all desired columns
    parsed_data = {
        "brewery_name": brewery_name,
        "full_address": None,
        "street": None,
        "number": None,
        "municipality": None,
        "postcode": None,
        "province": None,
        "latitude": None,
        "longitude": None,
    }

    if not location or not location.raw or "address" not in location.raw:
        logger.warning(f"No detailed address data found for {brewery_name}")
        return parsed_data

    # Safely update the dictionary with available data
    parsed_data.update(
        {
            "full_address": location.address,
            "latitude": location.latitude,
            "longitude": location.longitude,
        }
    )

    address_parts = location.raw["address"]

    # Extract components using .get() to gracefully handle missing keys
    parsed_data.update(
        {
            "street": address_parts.get("road"),
            "number": address_parts.get("house_number"),
            # Sometimes it's city, town, or even village
            "municipality": address_parts.get("city")
            or address_parts.get("town")
            or address_parts.get("village"),
            "postcode": address_parts.get("postcode"),
            # In Belgium, Nominatim often calls the province 'state'
            "province": address_parts.get("state"),
        }
    )

    return parsed_data


def main():
    """
    Main function to read brewery data, get structured addresses, and save the results.
    """
    logger.info("Starting structured brewery address fetching process.")

    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    unique_breweries = df["brewery_name"].dropna().unique()
    logger.info(f"Found {len(unique_breweries)} unique breweries to process.")

    geolocator = Nominatim(user_agent="belgian_brewery_mapper_v2")
    logger.info("Initialized Nominatim geolocator.")

    results = []
    for brewery in tqdm(unique_breweries, desc="Geocoding Breweries"):
        location = get_brewery_location(geolocator, brewery)
        # The parsing now happens here, for each location object
        structured_data = parse_location_data(brewery, location)
        results.append(structured_data)

    logger.info("Finished geocoding all unique breweries.")

    if not results:
        logger.warning("No results to write. Exiting.")
        return

    results_df = pd.DataFrame(results)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(OUTPUT_FILE, index=False)
    logger.success(
        f"Successfully saved {len(results_df)} structured brewery addresses to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()

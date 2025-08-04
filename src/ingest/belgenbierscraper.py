"""
Belgian Beer Scraper
A modular scraper to extract beer data from Belgenbier.be's beer listing pages.
"""

import requests
from bs4 import BeautifulSoup, Tag
from bs4.element import PageElement, NavigableString
import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from loguru import logger
import time
from urllib.parse import urljoin


class BelgenBierScraper:
    """
    A class to scrape beer data from the Belgenbier.be website.
    """

    def __init__(
        self, base_url: str = "https://www.belgenbier.be/bieren/bieren.php"
    ) -> None:
        """
        Initialize the Belgian beer scraper.

        Args:
            base_url: Base URL for the Belgian beer database.
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

        self._setup_directories()
        logger.info(f"Initialized Belgian beer scraper for: {self.base_url}")

    def _setup_directories(self) -> None:
        """Setup data and log directories relative to the script's location."""
        project_root = Path(__file__).parent.parent.parent
        self.data_dir = project_root / "data" / "raw" / "belgenbier"
        self.log_dir = project_root / "logs"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Log directory: {self.log_dir}")

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a specific page from the Belgian beer database.

        Args:
            url: The URL of the page to fetch.

        Returns:
            BeautifulSoup object of the parsed page, or None if it failed.
        """
        logger.info(f"Fetching page: {url}")
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            logger.success(
                f"Successfully parsed page with {len(soup.find_all())} HTML elements"
            )
            return soup

        except requests.RequestException as e:
            logger.error(f"Failed to fetch page {url}: {e}")
            return None

    def find_main_beer_table(self, soup: BeautifulSoup) -> Optional[Tag]:
        """
        Find the main table containing beer data.

        Args:
            soup: The BeautifulSoup object of the page.

        Returns:
            The BeautifulSoup Tag of the table, or None if not found.
        """
        # The main content table seems to be the first one with a specific bgcolor
        tables = soup.find_all("table", {"bgcolor": "#E8E8E8"})

        if tables:
            first_table = tables[0]
            if isinstance(first_table, Tag):
                logger.success("Identified main beer table based on bgcolor attribute.")
                return first_table

        logger.warning(
            "Could not find the primary table. Falling back to keyword search."
        )

        # Fallback if the bgcolor changes
        for i, table in enumerate(soup.find_all("table")):
            if not isinstance(table, Tag):
                continue

            rows = table.find_all("tr")
            if len(rows) > 10:  # Assume the main table has a good number of rows
                table_text = table.get_text(strip=True).lower()
                if any(
                    indicator in table_text
                    for indicator in ["bier", "brouwerij", "uit productie"]
                ):
                    logger.success(
                        f"Identified main beer table via fallback: Table {i}"
                    )
                    return table

        logger.error("No suitable beer table found on the page.")
        return None

    def extract_beer_data_from_cell(self, cell_text: str) -> Optional[Dict[str, Any]]:
        """
        Extract beer information from a single cell text.

        Args:
            cell_text: The raw text from a table cell.

        Returns:
            A dictionary with beer data or None if not a valid beer entry.
        """
        if not cell_text or len(cell_text.strip()) < 3:
            return None

        # Pattern to skip common non-beer text
        skip_patterns = r"^(page \d+|\[.*\]|vorige pagina|next page|zoek|search|naam bier|er zijn momenteel|webmaster)"
        if re.match(skip_patterns, cell_text.lower()):
            return None

        # Pattern to extract: Beer Name (Brewery) - Optional Status (Optional Info)
        pattern = r"^(.+?)\s*\(([^)]+)\)(.*)$"
        match = re.match(pattern, cell_text.strip())

        if not match:
            return None

        beer_name = match.group(1).strip()
        brewery = match.group(2).strip()
        remainder = match.group(3).strip()

        production_status = "In productie"
        if "uit productie" in remainder.lower():
            production_status = "Uit productie"
            # Clean the status from the remainder
            remainder = re.sub(
                r"\s*-\s*uit productie", "", remainder, flags=re.IGNORECASE
            ).strip()

        return {
            "beer_name": beer_name,
            "brewery": brewery,
            "production_status": production_status,
            "notes": remainder.strip(),
            "raw_text": cell_text,
        }

    def extract_beers_from_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extracts all unique beer data from a single parsed page.

        Args:
            soup: The BeautifulSoup object for the page.

        Returns:
            A list of dictionaries, where each dictionary is a unique beer.
        """
        main_table = self.find_main_beer_table(soup)
        if not main_table:
            return []

        beers: List[Dict[str, Any]] = []
        seen_beers: set[tuple[str, str]] = set()  # Track (beer_name, brewery)

        for cell in main_table.find_all("td"):
            cell_text = cell.get_text(strip=True)
            beer_data = self.extract_beer_data_from_cell(cell_text)

            if beer_data:
                beer_key = (
                    beer_data["beer_name"].lower(),
                    beer_data["brewery"].lower(),
                )

                if beer_key not in seen_beers:
                    seen_beers.add(beer_key)
                    beers.append(beer_data)

        logger.info(f"Extracted {len(beers)} unique beers from the page.")
        return beers

    def get_pagination_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract pagination information to find the next page URL.

        Args:
            soup: The BeautifulSoup object of the page.

        Returns:
            A dictionary with pagination details.
        """
        pagination_info = {"has_next": False, "next_url": None, "total_pages": 1}

        # Find total pages from text like "Pagina 1 van 237"
        page_text_match = re.search(r"Pagina\s+\d+\s+van\s+(\d+)", soup.get_text())
        if page_text_match:
            pagination_info["total_pages"] = int(page_text_match.group(1))

        # Find the "Next page" link
        next_link_tag = soup.find("a", string=re.compile(r"Next page", re.IGNORECASE))
        if next_link_tag and isinstance(next_link_tag, Tag):
            href = next_link_tag.get("href")
            if href:
                next_href = str(href)
                # Construct absolute URL if it's relative
                if next_href.startswith("http"):
                    pagination_info["next_url"] = next_href
                else:
                    # Handle relative URLs properly
                    pagination_info["next_url"] = urljoin(self.base_url, next_href)

                pagination_info["has_next"] = True
                logger.info(f"Next page found: {pagination_info['next_url']}")
            else:
                logger.info("No more pages found.")
        else:
            logger.info("No more pages found.")

        return pagination_info

    def scrape_all_pages(
        self, max_pages: Optional[int] = None, delay: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Scrape multiple pages by following the 'Next page' link.

        Args:
            max_pages: Maximum number of pages to scrape. Scrapes all if None.
            delay: Seconds to wait between page requests.

        Returns:
            A list of all unique beer data found across all pages.
        """
        all_beers: List[Dict[str, Any]] = []
        seen_beers: set[tuple[str, str]] = set()

        current_url: Optional[str] = self.base_url
        page_count = 0

        while current_url:
            page_count += 1
            if max_pages and page_count > max_pages:
                logger.info(f"Reached max page limit of {max_pages}.")
                break

            logger.info(f"--- Scraping Page {page_count} ---")
            soup = self.fetch_page(current_url)
            if not soup:
                break

            page_beers = self.extract_beers_from_page(soup)

            # Add new, unique beers to the master list
            new_beers_found = 0
            for beer in page_beers:
                beer_key = (beer["beer_name"].lower(), beer["brewery"].lower())
                if beer_key not in seen_beers:
                    seen_beers.add(beer_key)
                    beer["beer_id"] = len(all_beers) + 1  # Assign a unique ID
                    beer["source_page"] = page_count
                    all_beers.append(beer)
                    new_beers_found += 1

            logger.info(
                f"Added {new_beers_found} new unique beers. Total unique: {len(all_beers)}"
            )

            pagination = self.get_pagination_info(soup)
            current_url = pagination["next_url"]

            if current_url and (not max_pages or page_count < max_pages):
                time.sleep(delay)

        logger.success(
            f"Scraping complete. Found {len(all_beers)} total unique beers from {page_count} pages."
        )
        return all_beers

    def save_to_csv(self, beers: List[Dict[str, Any]], filename: str) -> None:
        """
        Save beer data to a CSV file.

        Args:
            beers: The list of beer dictionaries to save.
            filename: The name of the output CSV file.
        """
        if not beers:
            logger.warning("No beer data provided to save.")
            return

        filepath = self.data_dir / filename
        df = pd.DataFrame(beers)

        # Define a logical column order
        column_order = [
            "beer_id",
            "beer_name",
            "brewery",
            "production_status",
            "notes",
            "source_page",
            "raw_text",
        ]
        # Get list of columns that are actually in the DataFrame
        final_columns = [col for col in column_order if col in df.columns]

        df = df[final_columns]
        df.to_csv(filepath, index=False)
        logger.success(f"Successfully saved {len(df)} beers to {filepath}")


# Add a global flag to prevent multiple executions
_main_executed = False

def main() -> None:
    """Main function to run the Belgian beer scraper."""
    global _main_executed
    if _main_executed:
        return
    _main_executed = True
    
    # Configure logging
    log_file = Path(__file__).parent.parent.parent / "logs" / "belgenbier_scraper.log"
    logger.add(log_file, rotation="1 MB", retention="10 days", level="DEBUG")

    logger.info("--- Starting Belgian Beer Scraper ---")

    try:
        scraper = BelgenBierScraper()

        # To scrape a small sample (e.g., first 3 pages)
        all_beers = scraper.scrape_all_pages(max_pages=3)

        # To scrape the entire website
        # all_beers = scraper.scrape_all_pages()

        if all_beers:
            # Save all collected data to a single file
            scraper.save_to_csv(all_beers, "belgian_beers_complete.csv")

            # --- Final Summary ---
            logger.info("\n--- FINAL SUMMARY ---")
            df = pd.DataFrame(all_beers)
            logger.info(f"Total unique beers collected: {len(df)}")

            if "brewery" in df.columns:
                logger.info(f"Total unique breweries found: {df['brewery'].nunique()}")
                top_10_breweries = df["brewery"].value_counts().nlargest(10)
                logger.info("Top 10 breweries by beer count:\n" + str(top_10_breweries))

            if "production_status" in df.columns:
                status_counts = df["production_status"].value_counts()
                logger.info("Production status summary:\n" + str(status_counts))

        logger.success("--- Belgian beer scraping completed successfully! ---")

    except Exception as e:
        logger.opt(exception=True).error(
            f"A critical error occurred in the main execution: {e}"
        )


if __name__ == "__main__":
    main()

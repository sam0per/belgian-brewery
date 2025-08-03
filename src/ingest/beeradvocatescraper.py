"""
BeerAdvocate Top Rated Beers Scraper
A modular scraper to extract beer data from BeerAdvocate's top-rated page.
"""

import requests
from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString
import pandas as pd
import time
import re
from typing import Optional, Dict, Any, List, Tuple, Union
from loguru import logger


class BeerAdvocateScraper:
    """Main scraper class for BeerAdvocate top-rated beers."""

    def __init__(self, base_url: str = "https://www.beeradvocate.com/beer/top-rated/"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
        logger.info(f"Initialized scraper for: {base_url}")

    def fetch_page(self, url: Optional[str] = None) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page.

        Args:
            url: URL to fetch (defaults to base_url)

        Returns:
            BeautifulSoup object or None if failed
        """
        target_url = url or self.base_url

        try:
            logger.info(f"Fetching page: {target_url}")
            response = self.session.get(target_url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            logger.success(
                f"Successfully parsed page with {len(soup.find_all())} HTML elements"
            )

            return soup

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page: {e}")
            return None

    def analyze_page_structure(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Analyze the basic structure of the page.

        Args:
            soup: BeautifulSoup object

        Returns:
            Dictionary with page structure information
        """
        if not soup:
            logger.warning("No soup object provided for analysis")
            return {}

        # Basic page analysis
        title_text = soup.title.string if soup.title and soup.title.string else "No title found"
        analysis = {
            "title": title_text,
            "total_elements": len(soup.find_all()),
            "tables_count": len(soup.find_all("table")),
            "divs_count": len(soup.find_all("div")),
            "links_count": len(soup.find_all("a")),
        }

        # Look for common table structures
        tables = soup.find_all("table")
        if tables:
            analysis["table_info"] = []
            for i, table in enumerate(tables):
                if isinstance(table, Tag):
                    rows = table.find_all("tr")
                    th_element = table.find("th")
                    analysis["table_info"].append(
                        {
                            "table_index": i,
                            "row_count": len(rows),
                            "has_header": bool(th_element),
                            "first_few_rows_text": [
                                row.get_text()[:100] for row in rows[:3] if isinstance(row, Tag)
                            ],
                        }
                    )

        logger.info(f"Page analysis complete: {analysis['title']}")
        return analysis

    def find_beer_data_containers(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Identify potential containers holding beer data.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of potential data containers
        """
        if not soup:
            return []

        # Look for common patterns in beer listing pages
        potential_containers: List[Dict[str, Any]] = []

        # Check for tables with beer-related content
        tables = soup.find_all("table")
        for table in tables:
            if isinstance(table, Tag):  # Type check for pylance
                text_content = table.get_text().lower()
                if any(
                    keyword in text_content
                    for keyword in ["beer", "brewery", "rating", "stout", "ipa"]
                ):
                    row_count = len(table.find_all("tr"))
                    potential_containers.append(
                        {
                            "type": "table",
                            "element": table,
                            "row_count": row_count,
                            "preview": text_content[:200],
                        }
                    )

        # Check for div containers with structured data
        divs = soup.find_all("div", class_=True)
        for div in divs:
            if isinstance(div, Tag):
                class_list = div.get("class")
                if class_list and isinstance(class_list, list) and any(
                    "list" in cls.lower() or "beer" in cls.lower()
                    for cls in class_list
                ):
                    potential_containers.append(
                        {
                            "type": "div",
                            "element": div,
                            "classes": class_list,
                            "preview": div.get_text()[:200],
                        }
                    )

        logger.info(f"Found {len(potential_containers)} potential beer data containers")
        return potential_containers

    def explore_page(self) -> Dict[str, Any]:
        """
        Main exploration method to understand the page structure.

        Returns:
            Complete analysis of the page
        """
        logger.info("Starting page exploration...")

        soup = self.fetch_page()
        if not soup:
            return {"error": "Failed to fetch page"}

        # Perform analysis
        structure_info = self.analyze_page_structure(soup)
        beer_containers = self.find_beer_data_containers(soup)

        exploration_results = {
            "page_structure": structure_info,
            "potential_beer_containers": len(beer_containers),
            "container_details": [
                {"type": container["type"], "preview": container["preview"]}
                for container in beer_containers[:5]  # Show first 5
            ],
        }

        logger.success("Page exploration completed")
        return exploration_results

    def find_main_beer_table(self, soup: BeautifulSoup) -> Optional[Tag]:
        """
        Locate the main table containing beer rankings.

        Args:
            soup: BeautifulSoup object

        Returns:
            The main beer ranking table or None
        """
        if not soup:
            return None

        tables = soup.find_all("table")
        logger.info(f"Found {len(tables)} tables on the page")

        # Look for table with ranking structure
        for i, table in enumerate(tables):
            if isinstance(table, Tag):  # Type check for pylance
                rows = table.find_all("tr")
                if len(rows) > 10:  # Main table should have many rows
                    # Check if it contains beer-related headers
                    header_elements = table.find_all("th")
                    header_text = " ".join([
                        th.get_text() for th in header_elements 
                        if isinstance(th, Tag)
                    ])
                    
                    first_row = rows[0] if rows else None
                    first_row_text = (
                        first_row.get_text().lower() 
                        if first_row and isinstance(first_row, Tag) 
                        else ""
                    )

                    logger.info(f"Table {i}: {len(rows)} rows, header: '{header_text[:100]}'")

                    if any(keyword in (header_text + first_row_text).lower()
                           for keyword in ["rating", "beer", "brewery", "avg"]):
                        logger.success(f"Identified main beer table: Table {i}")
                        return table

        return None

    def analyze_table_structure(self, table: Optional[Tag]) -> Dict[str, Any]:
        """
        Analyze the structure of the beer ranking table.

        Args:
            table: BeautifulSoup table element

        Returns:
            Dictionary with table structure analysis
        """
        if not table or not isinstance(table, Tag):
            return {}

        rows = table.find_all("tr")
        headers = table.find_all("th")

        analysis = {
            "total_rows": len(rows),
            "has_headers": len(headers) > 0,
            "header_count": len(headers),
            "header_texts": [
                th.get_text().strip() for th in headers 
                if isinstance(th, Tag)
            ],
            "sample_rows": []
        }

        # Analyze first few data rows (skip header if present)
        start_idx = 1 if headers else 0
        for i in range(start_idx, min(start_idx + 5, len(rows))):
            if i < len(rows):
                row = rows[i]
                if isinstance(row, Tag):
                    cells = row.find_all(["td", "th"])
                    cell_data = {
                        "row_index": i,
                        "cell_count": len(cells),
                        "cell_texts": [
                            cell.get_text().strip()[:50] for cell in cells 
                            if isinstance(cell, Tag)
                        ],
                        "cell_html_preview": [
                            str(cell)[:100] for cell in cells[:3] 
                            if isinstance(cell, Tag)
                        ]
                    }
                    analysis["sample_rows"].append(cell_data)

        logger.info(f"Table analysis: {analysis['total_rows']} rows, {analysis['header_count']} headers")
        return analysis

    def extract_beer_row_data(self, row: Union[Tag, Any], row_number: int) -> Optional[Dict[str, Any]]:
        """
        Extract beer data from a single table row.

        Args:
            row: BeautifulSoup row element
            row_number: Row number for reference

        Returns:
            Dictionary with extracted beer data or None
        """
        if not isinstance(row, Tag):
            return None
            
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:  # Need at least rank, beer info, ratings, avg
            return None

        try:
            # Extract basic cell texts
            cell_texts = [
                cell.get_text().strip() for cell in cells 
                if isinstance(cell, Tag)
            ]

            # Skip header row
            if not cell_texts[0].isdigit():
                return None

            # Look for patterns in the data
            beer_data: Dict[str, Any] = {
                "row_number": row_number,
                "raw_cells": cell_texts,
                "cell_count": len(cells)
            }

            # Extract rank (first column)
            if cell_texts and cell_texts[0].isdigit():
                beer_data["rank"] = int(cell_texts[0])

            # Extract beer info from HTML structure (second column)
            if len(cells) > 1 and isinstance(cells[1], Tag):
                beer_cell = cells[1]
                if isinstance(beer_cell, Tag):
                    parsed_info = self._parse_beer_cell_html(beer_cell)
                    beer_data.update(parsed_info)

            # Extract number of ratings (third column)
            if len(cell_texts) > 2:
                ratings_text = cell_texts[2]
                clean_ratings = ratings_text.replace(',', '')
                if clean_ratings.isdigit():
                    beer_data["num_ratings"] = int(clean_ratings)
                    beer_data["num_ratings_display"] = ratings_text

            # Extract average rating (fourth column)
            if len(cell_texts) > 3:
                avg_text = cell_texts[3]
                if re.match(r'^\d+\.\d+$', avg_text):
                    beer_data["avg_rating"] = float(avg_text)

            return beer_data

        except (ValueError, AttributeError) as e:
            logger.warning(f"Error extracting data from row {row_number}: {e}")
            return None

    def _parse_beer_cell_html(self, beer_cell: Tag) -> Dict[str, str]:
        """
        Parse the beer info cell HTML to extract name, brewery, style, and ABV.
        
        The HTML structure typically contains:
        - A link with the beer name
        - Line breaks separating components
        - Brewery name
        - Style name
        - ABV percentage after |
        
        Args:
            beer_cell: BeautifulSoup cell element containing beer info
            
        Returns:
            Dictionary with parsed beer information
        """
        parsed = {}
        
        try:
            # Get the raw HTML content
            cell_html = str(beer_cell)
            
            # Extract beer name from the link
            beer_link = beer_cell.find('a')
            if beer_link and isinstance(beer_link, Tag):
                beer_name = beer_link.get_text().strip()
                parsed["beer_name"] = beer_name
            
            # Get all text content and split by line breaks
            full_text = beer_cell.get_text()
            
            # Split by | to separate main info from ABV
            if ' | ' in full_text:
                main_part, abv_part = full_text.split(' | ', 1)
                # Extract ABV
                abv_match = re.search(r'(\d+(?:\.\d+)?%)', abv_part)
                if abv_match:
                    parsed["abv"] = abv_match.group(1)
            else:
                main_part = full_text
            
            # Parse the main part using HTML structure
            # Look for <br> tags to identify separate components
            html_parts = str(beer_cell).split('<br/>')
            
            if len(html_parts) >= 3:
                # First part contains beer name (with link)
                beer_name_part = BeautifulSoup(html_parts[0], 'html.parser').get_text().strip()
                if beer_link:
                    parsed["beer_name"] = beer_link.get_text().strip()
                else:
                    parsed["beer_name"] = beer_name_part
                
                # Second part contains brewery
                brewery_part = BeautifulSoup(html_parts[1], 'html.parser').get_text().strip()
                parsed["brewery"] = brewery_part
                
                # Third part contains style (may have ABV after |)
                style_part = BeautifulSoup(html_parts[2], 'html.parser').get_text().strip()
                if ' | ' in style_part:
                    style_only = style_part.split(' | ')[0].strip()
                    parsed["style"] = style_only
                else:
                    parsed["style"] = style_part
            else:
                # Fallback: try to parse from text using the old method
                fallback_parsed = self._parse_beer_info_fallback(main_part)
                parsed.update(fallback_parsed)
                
        except Exception as e:
            logger.warning(f"Error parsing beer cell HTML: {e}")
            # Fallback to basic text extraction
            parsed["beer_name"] = beer_cell.get_text().strip()
            
        return parsed

    def _parse_beer_info_fallback(self, beer_info: str) -> Dict[str, str]:
        """
        Fallback method to parse beer info from text when HTML parsing fails.
        
        Args:
            beer_info: Raw beer information string
            
        Returns:
            Dictionary with parsed beer information
        """
        parsed = {}
        
        try:
            # Remove extra whitespace and normalize
            clean_info = re.sub(r'\s+', ' ', beer_info).strip()
            
            # Common patterns for brewery names
            brewery_patterns = [
                r'(.*?)(Toppling Goliath Brewing Company)',
                r'(.*?)(3 Floyds Brewing Co\.)',
                r'(.*?)(Perennial Artisan Ales)',
                r'(.*?)(Cigar City Brewing)',
                r'(.*?)(The Alchemist)',
                r'(.*?)(Tree House Brewing Company)',
                r'(.*?)(\w+\s+Brewing\s+(?:Company|Co\.?))',
                r'(.*?)(\w+\s+Brewery)',
                r'(.*?)(Brasserie\s+\w+)',
                r'(.*?)(Brouwerij\s+\w+)',
            ]
            
            # Try to match brewery patterns
            for pattern in brewery_patterns:
                match = re.search(pattern, clean_info, re.IGNORECASE)
                if match:
                    beer_name = match.group(1).strip()
                    brewery = match.group(2).strip()
                    remaining = clean_info[match.end():].strip()
                    
                    parsed["beer_name"] = beer_name
                    parsed["brewery"] = brewery
                    parsed["style"] = remaining
                    return parsed
            
            # If no pattern matches, try generic approach
            words = clean_info.split()
            if len(words) > 4:
                # Assume first few words are beer name
                parsed["beer_name"] = ' '.join(words[:4])
                parsed["brewery"] = ' '.join(words[4:7]) if len(words) > 7 else ' '.join(words[4:])
                parsed["style"] = ' '.join(words[7:]) if len(words) > 7 else ""
            else:
                parsed["beer_name"] = clean_info
                parsed["brewery"] = ""
                parsed["style"] = ""
                
        except Exception as e:
            logger.warning(f"Error in fallback parsing: {e}")
            parsed["beer_name"] = beer_info
            parsed["brewery"] = ""
            parsed["style"] = ""
            
        return parsed

    def _parse_beer_info(self, beer_info: str) -> Dict[str, str]:
        """
        Parse the beer info string to extract name, brewery, style, and ABV.
        
        This method is deprecated in favor of HTML structure parsing.
        """
        # Use the fallback method
        return self._parse_beer_info_fallback(beer_info)
        
    def _extract_beer_components(self, main_info: str) -> Dict[str, str]:
        """
        Extract beer name, brewery, and style from concatenated text.
        
        This method is deprecated in favor of HTML structure parsing.
        """
        # Use the fallback method
        return self._parse_beer_info_fallback(main_info)

    def extract_all_beers(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract all 250 beers from the rankings table.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of all beer data dictionaries
        """
        table = self.find_main_beer_table(soup)
        if not table or not isinstance(table, Tag):
            logger.error("Could not find main beer table")
            return []

        rows = table.find_all("tr")
        all_beers = []
        
        logger.info(f"Processing {len(rows)} rows from beer table")

        for i, row in enumerate(rows):
            if isinstance(row, Tag):
                beer_data = self.extract_beer_row_data(row, i)
                if beer_data and "rank" in beer_data:  # Only include valid beer rows
                    all_beers.append(beer_data)
                    
        logger.success(f"Successfully extracted {len(all_beers)} beers")
        return all_beers

    def scrape_top_beers(self, save_to_file: bool = True) -> List[Dict[str, Any]]:
        """
        Main method to scrape all top-rated beers.
        
        Args:
            save_to_file: Whether to save results to CSV
            
        Returns:
            List of all beer data
        """
        logger.info("Starting complete beer data extraction...")
        
        soup = self.fetch_page()
        if not soup:
            logger.error("Failed to fetch page")
            return []
            
        # Extract all beers
        all_beers = self.extract_all_beers(soup)
        
        if save_to_file and all_beers:
            self._save_to_csv(all_beers)
            
        logger.success(f"Scraping completed. Extracted {len(all_beers)} beers")
        return all_beers
        
    def _save_to_csv(self, beer_data: List[Dict[str, Any]], filename: str = "top_250_beers.csv") -> None:
        """
        Save beer data to CSV file.
        
        Args:
            beer_data: List of beer dictionaries
            filename: Output filename
        """
        try:
            df = pd.DataFrame(beer_data)
            
            # Select and order columns for output
            output_columns = [
                "rank", "beer_name", "brewery", "style", "abv", 
                "num_ratings", "avg_rating"
            ]
            
            # Only include columns that exist
            available_columns = [col for col in output_columns if col in df.columns]
            df_output = df[available_columns]
            
            df_output.to_csv(filename, index=False)
            logger.success(f"Saved {len(beer_data)} beers to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def comprehensive_page_analysis(self) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of the BeerAdvocate page.

        Returns:
            Complete analysis results
        """
        logger.info("Starting comprehensive page analysis...")

        soup = self.fetch_page()
        if not soup:
            return {"error": "Failed to fetch page"}

        # Basic structure analysis
        structure = self.analyze_page_structure(soup)

        # Find and analyze main table
        main_table = self.find_main_beer_table(soup)
        table_analysis = self.analyze_table_structure(main_table) if main_table else {}

        # Extract sample data
        sample_beers = self.extract_all_beers(soup)[:15]  # Get first 15 beers as samples

        results = {
            "page_structure": structure,
            "table_analysis": table_analysis,
            "sample_beer_count": len(sample_beers),
            "sample_beers": sample_beers[:5],  # Show first 5 for review
            "data_patterns": self._analyze_data_patterns(sample_beers)
        }

        logger.success("Comprehensive analysis completed")
        return results

    def _analyze_data_patterns(self, sample_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze patterns in the extracted sample data.

        Args:
            sample_data: List of sample beer data

        Returns:
            Pattern analysis results
        """
        if not sample_data:
            return {}

        patterns: Dict[str, Any] = {
            "consistent_fields": [],
            "field_types": {},
            "data_quality": {}
        }

        # Check which fields appear consistently
        all_fields = set()
        for beer in sample_data:
            all_fields.update(beer.keys())

        for field in all_fields:
            count = sum(
                1 for beer in sample_data 
                if field in beer and beer[field] is not None and beer[field] != ""
            )
            patterns["field_types"][field] = count
            if count >= len(sample_data) * 0.8:  # 80% consistency
                patterns["consistent_fields"].append(field)

        # Check data quality
        patterns["data_quality"]["has_rankings"] = any(
            "rank" in beer and beer.get("rank") is not None 
            for beer in sample_data
        )
        patterns["data_quality"]["has_ratings"] = any(
            "rating" in beer and beer.get("rating") is not None 
            for beer in sample_data
        )
        patterns["data_quality"]["has_beer_names"] = any(
            "beer_name" in beer and beer.get("beer_name") is not None 
            for beer in sample_data
        )

        return patterns


def main():
    """Example usage of the scraper for complete data extraction."""
    logger.add("scraper_analysis.log", rotation="1 MB")

    scraper = BeerAdvocateScraper()
    
    # First, run analysis to understand structure
    logger.info("=== RUNNING PAGE ANALYSIS ===")
    analysis_results = scraper.comprehensive_page_analysis()
    
    if "error" in analysis_results:
        logger.error(f"Analysis failed: {analysis_results['error']}")
        return
        
    # Display key findings
    logger.info(f"Found {analysis_results['sample_beer_count']} sample beers")
    logger.info(f"Data quality: {analysis_results['data_patterns']['data_quality']}")
    
    # Now extract all beers
    logger.info("\n=== EXTRACTING ALL BEERS ===")
    all_beers = scraper.scrape_top_beers(save_to_file=True)
    
    # Display first few beers as examples
    logger.info("\n=== SAMPLE EXTRACTED BEERS ===")
    for i, beer in enumerate(all_beers[:5]):
        logger.info(f"#{beer.get('rank', 'N/A')}: {beer.get('beer_name', 'N/A')} - "
                   f"{beer.get('brewery', 'N/A')} ({beer.get('style', 'N/A')}) - "
                   f"{beer.get('abv', 'N/A')} - Rating: {beer.get('avg_rating', 'N/A')}")


if __name__ == "__main__":
    main()

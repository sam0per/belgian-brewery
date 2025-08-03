"""
Kaggle Beer Dataset Scraper
A modular scraper to download and analyze beer-related datasets from Kaggle.
"""

import kaggle
import pandas as pd
import os
import zipfile
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from loguru import logger
import time


class KaggleBeerScraper:
    """Main scraper class for Kaggle beer datasets."""

    def __init__(self):
        # Set up directories relative to project root
        # Get the project root (2 levels up from src/ingest/)
        project_root = Path(__file__).parent.parent.parent
        self.data_dir = project_root / "data" / "raw" / "kaggle"
        self.log_dir = project_root / "logs"
        self.metadata_dir = self.data_dir / "metadata"
        self._setup_directories()
        
        # Setup logging
        self._setup_logging()
        
        # Initialize Kaggle API
        self._authenticate_kaggle()
        
        logger.info("Initialized Kaggle Beer Scraper")

    def _setup_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data directory: {self.data_dir.absolute()}")
        logger.info(f"Log directory: {self.log_dir.absolute()}")
        logger.info(f"Metadata directory: {self.metadata_dir.absolute()}")

    def _setup_logging(self) -> None:
        """Setup logging for Kaggle scraper."""
        log_file = self.log_dir / "kaggle_scraper.log"
        logger.add(log_file, rotation="1 MB", retention="10 days")

    def _authenticate_kaggle(self) -> None:
        """Authenticate with Kaggle API."""
        try:
            kaggle.api.authenticate()
            logger.success("Successfully authenticated with Kaggle API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Kaggle API: {e}")
            raise

    def search_beer_datasets(self, search_terms: Optional[List[str]] = None, max_results: int = 20) -> List[Dict[str, Any]]:
        """
        Search for beer-related datasets on Kaggle.
        
        Args:
            search_terms: List of search terms to use
            max_results: Maximum number of results to return
            
        Returns:
            List of dataset information dictionaries
        """
        if search_terms is None:
            search_terms = ['beer', 'brewery', 'brewing', 'alcohol', 'craft beer']
        
        all_datasets = []
        
        for term in search_terms:
            logger.info(f"Searching for datasets with term: '{term}'")
            
            try:
                datasets = kaggle.api.dataset_list(
                    search=term, 
                    sort_by='hottest'
                )
                
                if datasets:
                    logger.info(f"Found {len(datasets)} datasets for term '{term}'")
                    for dataset in datasets:
                        if dataset:
                            # Check if dataset is actually beer-related
                            title_lower = dataset.title.lower()
                            subtitle_lower = getattr(dataset, 'subtitle', '').lower()
                            
                            beer_keywords = ['beer', 'brewery', 'brewing', 'ale', 'ipa', 'stout', 'lager', 'hops']
                            is_beer_related = any(keyword in title_lower or keyword in subtitle_lower 
                                                for keyword in beer_keywords)
                            
                            if is_beer_related or term == 'beer':  # Include all if searching specifically for beer
                                dataset_info = {
                                    'title': dataset.title,
                                    'ref': dataset.ref,
                                    'subtitle': getattr(dataset, 'subtitle', ''),
                                    'description': getattr(dataset, 'description', ''),
                                    'size': getattr(dataset, 'size', 0),
                                    'download_count': getattr(dataset, 'downloadCount', 0),
                                    'vote_count': getattr(dataset, 'voteCount', 0),
                                    'usability_rating': getattr(dataset, 'usabilityRating', 0),
                                    'last_updated': str(getattr(dataset, 'lastUpdated', '')),
                                    'search_term': term,
                                    'is_beer_related': is_beer_related
                                }
                                all_datasets.append(dataset_info)
                            else:
                                logger.debug(f"Skipping non-beer dataset: {dataset.title}")
                else:
                    logger.warning(f"No datasets found for term '{term}'")
                    
            except Exception as e:
                logger.error(f"Error searching for term '{term}': {e}")
                continue
                
        # Remove duplicates based on ref
        unique_datasets = []
        seen_refs = set()
        for dataset in all_datasets:
            if dataset['ref'] not in seen_refs:
                unique_datasets.append(dataset)
                seen_refs.add(dataset['ref'])
                
        # Sort by relevance (beer-related first, then by download count)
        unique_datasets.sort(key=lambda x: (not x['is_beer_related'], -x['download_count']))
                
        logger.success(f"Found {len(unique_datasets)} unique beer-related datasets")
        return unique_datasets

    def download_dataset(self, dataset_ref: str, force_download: bool = False) -> Dict[str, Any]:
        """
        Download a dataset and perform initial quality checks.
        
        Args:
            dataset_ref: Kaggle dataset reference
            force_download: Whether to re-download if already exists
            
        Returns:
            Dictionary with download results and quality assessment
        """
        try:
            logger.info(f"Starting download of dataset: {dataset_ref}")
            
            # First, download and analyze metadata
            metadata_analysis = self.analyze_dataset_metadata(dataset_ref)
            
            # Create dataset-specific directory
            dataset_dir = self.data_dir / dataset_ref.replace('/', '_')
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            # Check if already downloaded
            if not force_download and any(dataset_dir.iterdir()):
                logger.info(f"Dataset {dataset_ref} already exists. Skipping download.")
                existing_files = list(dataset_dir.glob('*.csv'))
                if existing_files:
                    analysis = self._analyze_downloaded_files(dataset_dir, dataset_ref)
                    # Add metadata to analysis
                    analysis['metadata'] = metadata_analysis
                    return analysis
            
            # Download the dataset
            logger.info(f"Downloading files to: {dataset_dir}")
            kaggle.api.dataset_download_files(
                dataset_ref, 
                path=str(dataset_dir), 
                unzip=True
            )
            
            logger.success(f"Successfully downloaded dataset: {dataset_ref}")
            
            # Analyze downloaded files
            analysis = self._analyze_downloaded_files(dataset_dir, dataset_ref)
            # Add metadata to analysis
            analysis['metadata'] = metadata_analysis
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error downloading dataset {dataset_ref}: {e}")
            return {
                'dataset_ref': dataset_ref,
                'success': False,
                'error': str(e),
                'metadata': {}
            }

    def analyze_dataset_metadata(self, dataset_ref: str) -> Dict[str, Any]:
        """
        Download and analyze metadata for a specific dataset.
        
        Args:
            dataset_ref: Kaggle dataset reference (e.g., 'user/dataset-name')
            
        Returns:
            Dictionary with metadata analysis
        """
        try:
            logger.info(f"Downloading metadata for dataset: {dataset_ref}")
            
            # Download metadata
            metadata_path = self.metadata_dir / dataset_ref.replace('/', '_')
            metadata_path.mkdir(parents=True, exist_ok=True)
            
            kaggle.api.dataset_metadata(dataset_ref, path=str(metadata_path))
            
            # Read and parse metadata
            metadata_file = metadata_path / "dataset-metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    
                analysis = {
                    'dataset_ref': dataset_ref,
                    'title': metadata.get('title', ''),
                    'description': metadata.get('description', ''),
                    'keywords': metadata.get('keywords', []),
                    'licenses': metadata.get('licenses', []),
                    'collaborators': metadata.get('collaborators', []),
                    'data_files': len(metadata.get('resources', [])),
                    'file_info': [],
                    'metadata_saved': True,
                    'metadata_path': str(metadata_file)
                }
                
                # Analyze file information
                for resource in metadata.get('resources', []):
                    file_info = {
                        'name': resource.get('name', ''),
                        'description': resource.get('description', ''),
                        'columns': len(resource.get('schema', {}).get('fields', [])),
                        'schema': resource.get('schema', {}).get('fields', [])
                    }
                    analysis['file_info'].append(file_info)
                    
                logger.success(f"Successfully analyzed metadata for {dataset_ref}")
                logger.info(f"Metadata saved to: {metadata_file}")
                return analysis
                
            else:
                logger.error(f"Metadata file not found for {dataset_ref}")
                return {'dataset_ref': dataset_ref, 'metadata_saved': False, 'error': 'Metadata file not found'}
                
        except Exception as e:
            logger.error(f"Error analyzing metadata for {dataset_ref}: {e}")
            return {'dataset_ref': dataset_ref, 'metadata_saved': False, 'error': str(e)}

    def _analyze_downloaded_files(self, dataset_dir: Path, dataset_ref: str) -> Dict[str, Any]:
        """
        Analyze downloaded files for quality and structure.
        
        Args:
            dataset_dir: Directory containing downloaded files
            dataset_ref: Dataset reference
            
        Returns:
            Analysis results
        """
        analysis = {
            'dataset_ref': dataset_ref,
            'success': True,
            'download_path': str(dataset_dir),
            'files': [],
            'total_size_mb': 0,
            'csv_files': 0,
            'data_quality': {}
        }
        
        try:
            # Analyze all files in the directory
            for file_path in dataset_dir.iterdir():
                if file_path.is_file():
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    analysis['total_size_mb'] += file_size_mb
                    
                    file_info = {
                        'name': file_path.name,
                        'size_mb': round(file_size_mb, 2),
                        'extension': file_path.suffix.lower()
                    }
                    
                    # Analyze CSV files specifically
                    if file_path.suffix.lower() == '.csv':
                        analysis['csv_files'] += 1
                        csv_analysis = self._analyze_csv_file(file_path)
                        file_info.update(csv_analysis)
                        
                    analysis['files'].append(file_info)
            
            # Overall data quality assessment
            analysis['data_quality'] = self._assess_data_quality(analysis)
            
            logger.info(f"File analysis complete for {dataset_ref}")
            logger.info(f"Total files: {len(analysis['files'])}, CSV files: {analysis['csv_files']}")
            logger.info(f"Total size: {analysis['total_size_mb']:.2f} MB")
            
        except Exception as e:
            logger.error(f"Error analyzing files for {dataset_ref}: {e}")
            analysis['success'] = False
            analysis['error'] = str(e)
            
        return analysis

    def _analyze_csv_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Perform detailed analysis of a CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            CSV analysis results
        """
        try:
            # Read CSV with error handling
            df = pd.read_csv(file_path, nrows=1000)  # Sample first 1000 rows for analysis
            
            analysis = {
                'rows_sampled': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'beer_related_columns': [],
                'quality_score': 0
            }
            
            # Check for beer-related columns
            beer_keywords = [
                'beer', 'brewery', 'brew', 'alcohol', 'abv', 'ibu', 'rating', 'review',
                'style', 'hops', 'malt', 'yeast', 'flavor', 'aroma', 'appearance'
            ]
            
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in beer_keywords):
                    analysis['beer_related_columns'].append(col)
            
            # Calculate quality score
            quality_score = 0
            quality_score += min(len(analysis['beer_related_columns']) * 20, 60)  # Beer relevance
            quality_score += min(analysis['columns'] * 2, 20)  # Column richness
            quality_score += 20 if analysis['rows_sampled'] > 100 else 10  # Data volume
            
            analysis['quality_score'] = quality_score
            
            logger.debug(f"CSV analysis complete for {file_path.name}: "
                        f"{analysis['columns']} cols, {analysis['rows_sampled']} rows, "
                        f"quality: {quality_score}")
            
            return analysis
            
        except Exception as e:
            logger.warning(f"Error analyzing CSV file {file_path.name}: {e}")
            return {
                'error': str(e),
                'quality_score': 0
            }

    def _assess_data_quality(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess overall data quality of the dataset.
        
        Args:
            analysis: File analysis results
            
        Returns:
            Quality assessment
        """
        quality = {
            'overall_score': 0,
            'has_csv_files': analysis['csv_files'] > 0,
            'file_count': len(analysis['files']),
            'total_size_mb': analysis['total_size_mb'],
            'beer_relevance': 'unknown'
        }
        
        # Calculate overall score
        score = 0
        
        # File format scoring
        if quality['has_csv_files']:
            score += 30
            
        # Size scoring
        if quality['total_size_mb'] > 1:
            score += 20
        elif quality['total_size_mb'] > 0.1:
            score += 10
            
        # Beer relevance scoring
        beer_related_files = 0
        total_beer_columns = 0
        
        for file_info in analysis['files']:
            if 'beer_related_columns' in file_info:
                if file_info['beer_related_columns']:
                    beer_related_files += 1
                    total_beer_columns += len(file_info['beer_related_columns'])
        
        if beer_related_files > 0:
            score += min(beer_related_files * 20, 40)
            score += min(total_beer_columns * 2, 10)
            quality['beer_relevance'] = 'high' if total_beer_columns > 5 else 'medium'
        else:
            quality['beer_relevance'] = 'low'
            
        quality['overall_score'] = min(score, 100)
        
        return quality

    def download_recommended_datasets(self, min_quality_score: int = 50) -> Dict[str, Any]:
        """
        Download and analyze recommended beer datasets.
        
        Args:
            min_quality_score: Minimum quality score to consider downloading
            
        Returns:
            Results summary
        """
        # Updated list of verified beer datasets
        recommended_datasets = [
            "abhaysharma38/beer-rating-reviews",
            "rdoume/beerreviews", 
            "ruthgn/beer-profile-and-ratings-data-set",
            "stephenpoletto/top-beer-information",
            # "ehallmar/beers-breweries-and-beer-reviews"
        ]
        
        results = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'datasets': [],
            'summary': {}
        }
        
        logger.info(f"Starting download of {len(recommended_datasets)} recommended datasets")
        
        for dataset_ref in recommended_datasets:
            results['attempted'] += 1
            logger.info(f"Processing dataset {results['attempted']}/{len(recommended_datasets)}: {dataset_ref}")
            
            try:
                # Download and analyze
                download_result = self.download_dataset(dataset_ref)
                
                if download_result.get('success', False):
                    quality_score = download_result.get('data_quality', {}).get('overall_score', 0)
                    
                    if quality_score >= min_quality_score:
                        results['successful'] += 1
                        download_result['recommended'] = True
                        logger.success(f"Successfully downloaded high-quality dataset: {dataset_ref} "
                                     f"(Quality: {quality_score})")
                    else:
                        logger.warning(f"Dataset quality below threshold: {dataset_ref} "
                                     f"(Quality: {quality_score} < {min_quality_score})")
                        download_result['recommended'] = False
                        # Still count as successful download, just low quality
                        results['successful'] += 1
                else:
                    results['failed'] += 1
                    logger.error(f"Failed to download dataset: {dataset_ref}")
                    
                results['datasets'].append(download_result)
                
                # Brief pause between downloads
                time.sleep(1)
                
            except Exception as e:
                results['failed'] += 1
                logger.error(f"Error processing dataset {dataset_ref}: {e}")
                results['datasets'].append({
                    'dataset_ref': dataset_ref,
                    'success': False,
                    'error': str(e),
                    'metadata': {}
                })
        
        # Generate summary
        results['summary'] = self._generate_download_summary(results)
        
        logger.info(f"Download process completed: {results['successful']} successful, "
                   f"{results['failed']} failed out of {results['attempted']} attempted")
        
        return results

    def _generate_download_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate summary of download results.
        
        Args:
            results: Download results
            
        Returns:
            Summary statistics
        """
        summary = {
            'total_datasets': results['attempted'],
            'successful_downloads': results['successful'],
            'failed_downloads': results['failed'],
            'total_size_mb': 0,
            'total_csv_files': 0,
            'high_quality_datasets': 0,
            'recommended_datasets': []
        }
        
        for dataset in results['datasets']:
            if dataset.get('success', False):
                summary['total_size_mb'] += dataset.get('total_size_mb', 0)
                summary['total_csv_files'] += dataset.get('csv_files', 0)
                
                quality_score = dataset.get('data_quality', {}).get('overall_score', 0)
                if quality_score >= 70:
                    summary['high_quality_datasets'] += 1
                    
                if dataset.get('recommended', False):
                    summary['recommended_datasets'].append({
                        'ref': dataset['dataset_ref'],
                        'quality_score': quality_score,
                        'csv_files': dataset.get('csv_files', 0),
                        'size_mb': dataset.get('total_size_mb', 0)
                    })
        
        return summary


def main():
    """Main function to run Kaggle beer dataset scraper."""
    logger.info("Starting Kaggle Beer Dataset Scraper")
    
    try:
        # Initialize scraper
        scraper = KaggleBeerScraper()
        
        # Search for beer datasets with improved search
        logger.info("=== SEARCHING FOR BEER DATASETS ===")
        datasets = scraper.search_beer_datasets(
            search_terms=['beer', 'brewery', 'brewing'], 
            max_results=15
        )
        
        logger.info(f"Found {len(datasets)} unique beer datasets")
        for i, dataset in enumerate(datasets[:5]):
            beer_indicator = "🍺" if dataset['is_beer_related'] else "❓"
            logger.info(f"{i+1}. {beer_indicator} {dataset['title']} ({dataset['ref']}) - "
                       f"Downloads: {dataset['download_count']}, Rating: {dataset['usability_rating']}")
        
        # Download recommended datasets
        logger.info("\n=== DOWNLOADING RECOMMENDED DATASETS ===")
        results = scraper.download_recommended_datasets(min_quality_score=40)
        
        # Display summary
        logger.info("\n=== DOWNLOAD SUMMARY ===")
        summary = results['summary']
        logger.info(f"Successfully downloaded: {summary['successful_downloads']} datasets")
        logger.info(f"Total data size: {summary['total_size_mb']:.2f} MB")
        logger.info(f"Total CSV files: {summary['total_csv_files']}")
        logger.info(f"High quality datasets: {summary['high_quality_datasets']}")
        
        if summary['recommended_datasets']:
            logger.info("\nRecommended datasets for analysis:")
            for dataset in summary['recommended_datasets']:
                logger.info(f"  - {dataset['ref']} (Quality: {dataset['quality_score']}, "
                           f"Size: {dataset['size_mb']:.1f}MB, CSVs: {dataset['csv_files']})")
        
        # Display metadata information
        successful_with_metadata = [d for d in results['datasets'] 
                                   if d.get('success', False) and d.get('metadata', {}).get('metadata_saved', False)]
        logger.info(f"\nMetadata saved for {len(successful_with_metadata)} datasets")
        
        logger.success("Kaggle scraping completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()

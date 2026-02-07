"""
SEC Edgar 10-Q/10-K JSON Downloader (Refactored)

This script downloads 10Q and 10K submissions from the SEC Edgar system.
A list of tickers and CIK numbers are read from an input file.

Author: Jeff Jones
Refactored: 2026
"""

import re
import sys
import os
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Configuration for the downloader."""
    base_dir: Path = Path.cwd()
    output_dir: str = "OutputXML"
    min_year: int = 2020  # Changed from hard-coded 2025
    rate_limit_seconds: float = 1.0
    user_agent: str = os.getenv(
        'SEC_USER_AGENT',
        'Financial Inference Technology jeffjones@fitsolutionsusa.com'
    )
    retry_attempts: int = 3
    retry_backoff: float = 0.5


# Constants for SEC filing identification
INSTANCE_MARKERS = ['INSTANCE DOCUMENT', 'EX-101.INS', 'INSTANCE FILE', 'EXHIBIT 101.INS']
PRESENTATION_MARKERS = ['PRESENTATION LINKBASE', 'EX-101.PRE', 'EXHIBIT 101.PRE']
VALID_FORM_TYPES = ['10-K', '10-Q']


class SECDownloader:
    """Handles downloading of SEC XBRL filings."""
    
    def __init__(self, config: Config = None):
        """Initialize the downloader with configuration.
        
        Args:
            config: Configuration object. If None, uses default config.
        """
        self.config = config or Config()
        self.session = self._create_session()
        self.xml_file_count = 0
        
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.
        
        Returns:
            Configured requests Session object.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers['User-Agent'] = self.config.user_agent
        return session
    
    def _rate_limit(self):
        """Apply rate limiting to avoid overwhelming SEC servers."""
        logger.debug(f"Rate limiting: sleeping {self.config.rate_limit_seconds}s")
        time.sleep(self.config.rate_limit_seconds)
    
    @staticmethod
    def restore_windows_1252_characters(restore_string: str) -> str:
        """Replace C1 control characters with Windows-1252 equivalents.
        
        Args:
            restore_string: String potentially containing control characters.
            
        Returns:
            String with Windows-1252 characters restored.
        """
        def to_windows_1252(match):
            try:
                return bytes([ord(match.group(0))]).decode('windows-1252')
            except UnicodeDecodeError:
                return ''
        
        return re.sub(r'[\u0080-\u0099]', to_windows_1252, restore_string)
    
    def extract_xml_file_urls(self, input_url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract XML file URLs from SEC filing page.
        
        Args:
            input_url: URL of the SEC filing index page.
            
        Returns:
            Tuple of (instance_url, presentation_url). Returns (None, None) on failure.
        """
        self._rate_limit()
        
        try:
            response = self.session.get(input_url)
            response.raise_for_status()
            logger.info(f"Successfully requested: {input_url}")
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve {input_url}: {e}")
            return None, None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        row_list = soup.find_all("tr")
        
        instance_url = None
        presentation_url = None
        
        for row in row_list:
            row_string = row.get_text()
            
            # Check for instance document
            if any(marker in row_string for marker in INSTANCE_MARKERS):
                tags_with_href = row.find(href=True)
                if tags_with_href:
                    instance_url = tags_with_href.attrs['href']
            
            # Check for presentation linkbase
            if any(marker in row_string for marker in PRESENTATION_MARKERS):
                tags_with_href = row.find(href=True)
                if tags_with_href:
                    presentation_url = tags_with_href.attrs['href']
        
        if instance_url and presentation_url:
            return instance_url, presentation_url
        else:
            logger.warning(f"Could not find both XML files at {input_url}")
            return None, None
    
    @staticmethod
    def create_json_request(ticker: str, cik: int) -> Optional[str]:
        """Create SEC JSON API request URL.
        
        Args:
            ticker: Stock ticker symbol.
            cik: Central Index Key (CIK) number.
            
        Returns:
            JSON API URL or None if CIK is invalid.
        """
        cik_string = str(cik)
        if len(cik_string) < 4:
            logger.error(f"Invalid CIK length for {ticker}: {cik_string}")
            return None
        
        padded_cik = cik_string.zfill(10)
        return f'https://data.sec.gov/submissions/CIK{padded_cik}.json'
    
    def get_json_object(self, json_request: str) -> Optional[dict]:
        """Retrieve and parse JSON from SEC API.
        
        Args:
            json_request: URL of the JSON API endpoint.
            
        Returns:
            Parsed JSON object or None on failure.
        """
        self._rate_limit()
        
        try:
            response = self.session.get(json_request)
            response.raise_for_status()
            logger.info(f"Successfully requested: {json_request}")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve JSON from {json_request}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {json_request}: {e}")
            return None
    
    def get_submission_file_urls(
        self,
        ticker: str,
        cik: int
    ) -> Optional[Dict[str, List]]:
        """Get submission file URLs for a ticker/CIK.
        
        Args:
            ticker: Stock ticker symbol.
            cik: Central Index Key.
            
        Returns:
            Dictionary mapping report dates to filing information, or None on failure.
        """
        json_request = self.create_json_request(ticker, cik)
        if not json_request:
            return None
        
        json_object = self.get_json_object(json_request)
        if not json_object:
            logger.error(f"No JSON data retrieved for {ticker} (CIK: {cik})")
            return None
        
        try:
            filings = json_object['filings']
            recent_filings = filings['recent']
            forms = recent_filings['form']
        except KeyError as e:
            logger.error(f"Unexpected JSON structure for {ticker}: missing {e}")
            return None
        
        submissions = {}
        
        for i in range(len(forms)):
            form_type = forms[i]
            is_xbrl = recent_filings.get('isXBRL', [None] * len(forms))[i]
            
            if form_type in VALID_FORM_TYPES and is_xbrl == 1:
                accession_num = recent_filings['accessionNumber'][i]
                reporting_date = recent_filings['reportDate'][i]
                filing_date = recent_filings['filingDate'][i]
                
                # Build URL
                accession_no_dash = accession_num.replace("-", "")
                url = (
                    f'https://www.sec.gov/Archives/edgar/data/'
                    f'{cik}/{accession_no_dash}/{accession_num}-index.htm'
                )
                
                # Store only the latest filing for each reporting date
                if reporting_date not in submissions:
                    submissions[reporting_date] = [
                        filing_date,
                        reporting_date,
                        url,
                        form_type
                    ]
                    logger.debug(f"Added {form_type} for {ticker} on {reporting_date}")
                else:
                    logger.debug(
                        f"Skipping duplicate filing for {ticker} on {reporting_date}"
                    )
        
        if submissions:
            logger.info(f"Found {len(submissions)} submissions for {ticker}")
            return submissions
        else:
            logger.warning(f"No valid submissions found for {ticker}")
            return None
    
    def get_ticker_directory(self, ticker: str) -> Path:
        """Get or create directory for ticker data.
        
        Args:
            ticker: Stock ticker symbol.
            
        Returns:
            Path to ticker directory.
        """
        ticker_dir = self.config.base_dir / self.config.output_dir / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        return ticker_dir
    
    @staticmethod
    def format_date_for_filename(date_str: str) -> str:
        """Convert YYYY-MM-DD to YYYYMMDD format.
        
        Args:
            date_str: Date string in YYYY-MM-DD format.
            
        Returns:
            Date string in YYYYMMDD format.
        """
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%Y%m%d')
    
    def build_filename(
        self,
        report_date: str,
        ticker: str,
        cik: int,
        report_type: str,
        is_presentation: bool = False
    ) -> str:
        """Build standardized filename for XML file.
        
        Args:
            report_date: Report date in YYYY-MM-DD format.
            ticker: Stock ticker.
            cik: CIK number.
            report_type: Form type (10-K or 10-Q).
            is_presentation: Whether this is a presentation file.
            
        Returns:
            Formatted filename.
        """
        date_str = self.format_date_for_filename(report_date)
        clean_type = report_type.replace('/', '')
        suffix = '_Pre.xml' if is_presentation else '.xml'
        return f'{date_str}_{ticker}_{cik}_{clean_type}{suffix}'
    
    def retrieve_and_store_xml_files(
        self,
        ticker: str,
        cik: int,
        report_type: str,
        report_date: str,
        xml_url: str,
        pre_xml_url: str
    ) -> bool:
        """Download and save XML files for a filing.
        
        Args:
            ticker: Stock ticker.
            cik: CIK number.
            report_type: Form type (10-K or 10-Q).
            report_date: Report date in YYYY-MM-DD format.
            xml_url: URL for instance document.
            pre_xml_url: URL for presentation linkbase.
            
        Returns:
            True if files were downloaded, False otherwise.
        """
        ticker_dir = self.get_ticker_directory(ticker)
        date_obj = datetime.strptime(report_date, '%Y-%m-%d')
        
        # Skip if year is before minimum configured year
        if date_obj.year < self.config.min_year:
            logger.info(
                f"Skipping {ticker} {report_date} (before {self.config.min_year})"
            )
            return False
        
        # Download instance document
        instance_filename = self.build_filename(
            report_date, ticker, cik, report_type, is_presentation=False
        )
        instance_path = ticker_dir / instance_filename
        
        if instance_path.exists():
            logger.info(f"Instance file already exists: {instance_filename}")
        else:
            if not self._download_file(xml_url, instance_path):
                return False
        
        # Download presentation linkbase
        pres_filename = self.build_filename(
            report_date, ticker, cik, report_type, is_presentation=True
        )
        pres_path = ticker_dir / pres_filename
        
        if pres_path.exists():
            logger.info(f"Presentation file already exists: {pres_filename}")
        else:
            if not self._download_file(pre_xml_url, pres_path):
                return False
        
        return True
    
    def _download_file(self, url: str, filepath: Path) -> bool:
        """Download a file from URL and save to disk.
        
        Args:
            url: URL to download from.
            filepath: Path where file should be saved.
            
        Returns:
            True if successful, False otherwise.
        """
        self._rate_limit()
        full_url = f'https://www.sec.gov{url}'
        
        try:
            response = self.session.get(full_url)
            response.raise_for_status()
            
            filepath.write_bytes(response.content)
            self.xml_file_count += 1
            logger.info(f"Downloaded: {filepath.name}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Failed to download {full_url}: {e}")
            return False
        except IOError as e:
            logger.error(f"Failed to write file {filepath}: {e}")
            return False
    
    def process_ticker(self, ticker: str, cik: int) -> int:
        """Process all filings for a single ticker.
        
        Args:
            ticker: Stock ticker.
            cik: CIK number.
            
        Returns:
            Number of filings processed.
        """
        logger.info(f"Processing {ticker} (CIK: {cik})")
        
        # Get list of submissions
        submissions = self.get_submission_file_urls(ticker, cik)
        if not submissions:
            logger.warning(f"No submissions found for {ticker}")
            return 0
        
        files_processed = 0
        
        # Process each submission
        for report_date, filing_info in submissions.items():
            filing_date, report_date, filing_url, report_type = filing_info
            
            # Extract XML URLs from filing page
            xml_url, pre_xml_url = self.extract_xml_file_urls(filing_url)
            
            if not xml_url or not pre_xml_url:
                logger.warning(
                    f"No XBRL files found for {ticker} {report_type} on {report_date}"
                )
                continue
            
            # Download the files
            if self.retrieve_and_store_xml_files(
                ticker, cik, report_type, report_date, xml_url, pre_xml_url
            ):
                files_processed += 1
        
        return files_processed
    
    def process_ticker_list(self, input_file: Path) -> int:
        """Process all tickers from input CSV file.
        
        Args:
            input_file: Path to CSV file with Ticker and CIK columns.
            
        Returns:
            Total number of XML files downloaded.
        """
        try:
            ticker_cik_df = pd.read_csv(input_file)
        except Exception as e:
            logger.error(f"Failed to read input file {input_file}: {e}")
            return 0
        
        required_columns = ['Ticker', 'CIK']
        if not all(col in ticker_cik_df.columns for col in required_columns):
            logger.error(
                f"Input file must contain {required_columns} columns"
            )
            return 0
        
        total_files = 0
        
        for _, row in ticker_cik_df.iterrows():
            ticker = row['Ticker']
            cik = row['CIK']
            
            try:
                files_processed = self.process_ticker(ticker, cik)
                total_files += files_processed
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}", exc_info=True)
                continue
        
        logger.info(f"Total XML files downloaded: {self.xml_file_count}")
        return self.xml_file_count


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Download 10-Q and 10-K XBRL filings from SEC Edgar'
    )
    parser.add_argument(
        '-i', '--input',
        default='Ticker_CIK_List.csv',
        help='Input CSV file with Ticker and CIK columns'
    )
    parser.add_argument(
        '--min-year',
        type=int,
        default=2020,
        help='Minimum year to download (default: 2020)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create config
    config = Config(min_year=args.min_year)
    
    # Create downloader and process files
    downloader = SECDownloader(config)
    input_path = Path(args.input)
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    
    files_downloaded = downloader.process_ticker_list(input_path)
    
    if files_downloaded > 0:
        logger.info(f"Successfully downloaded {files_downloaded} XML files")
    else:
        logger.warning("No XML files were downloaded")


if __name__ == "__main__":
    main()

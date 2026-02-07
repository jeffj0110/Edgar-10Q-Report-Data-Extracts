"""
SEC Edgar XBRL XML Data Parser (Refactored)

This script parses XBRL Instance XML and XBRL Presentation XML files
from SEC Edgar filings and outputs structured CSV data.

Author: Jeff Jones (Original - July 2021)
Refactored: 2026
"""

import sys
import os
import csv
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field

import pandas as pd
import xml.etree.ElementTree as ET


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Constants
EXCLUSION_TAGS = [
    '}xbrl', 'html', '}loc', '}footnote', '}schemaRef',
    '}context', '}unit', '}entity', '}identifier', '}period',
    '}startDate', '}endDate', '}segment', '}instant',
    '}measure', '}divide', 'explicitMember', 'dimension'
]

CSV_HEADERS = [
    'Ticker', 'Report_Date', 'Field_Date', 'Section', 'Sub_Section',
    'Dimension', 'Member', 'FactDesc', 'Fact', 'Value_Rounding'
]


@dataclass
class XBRLData:
    """Container for parsed XBRL data."""
    tag: str
    string_value: str
    context_ref: str = 'null'
    unit_ref: str = 'null'
    decimals: str = 'null'


@dataclass
class ContextData:
    """Container for XBRL context information."""
    id: str
    text: str = ''
    asofdate: Optional[str] = None
    dimension: str = ''
    attributes: Dict[str, str] = field(default_factory=dict)


class XBRLParser:
    """Parser for SEC XBRL filings."""
    
    def __init__(self):
        """Initialize the parser."""
        self.storage_gaap: Dict[str, XBRLData] = {}
        self.context_dictionary: Dict[str, ContextData] = {}
        self.id_counter = 1
        
    def _exclusion_found(self, search_string: str) -> bool:
        """Check if tag should be excluded from parsing.
        
        Args:
            search_string: XML tag to check.
            
        Returns:
            True if tag should be excluded, False otherwise.
        """
        return any(tag in search_string for tag in EXCLUSION_TAGS)
    
    def _create_id(self) -> str:
        """Generate unique ID for XBRL elements without IDs.
        
        Returns:
            String ID.
        """
        id_str = str(self.id_counter)
        self.id_counter += 1
        return id_str
    
    @staticmethod
    def _reverse_find(
        start_index: int,
        input_string: str,
        char1: str,
        char2: str
    ) -> int:
        """Search backwards from index for one of two characters.
        
        Args:
            start_index: Index to start searching from.
            input_string: String to search in.
            char1: First character to search for.
            char2: Second character to search for.
            
        Returns:
            Index of found character, or -1 if not found.
        """
        if not input_string or start_index > len(input_string) or start_index < 0:
            return -1
        if not char1 or not char2:
            return -1
            
        for i in range(start_index - 1, -1, -1):
            if input_string[i] in (char1, char2):
                return i
        return -1
    
    @staticmethod
    def _convert_period_to_years(input_string: str) -> float:
        """Convert period string (P##Y##M##D format) to years.
        
        Supports formats like:
        - P10Y (10 years)
        - P6M (0.5 years)
        - P90D (0.25 years)
        - P5Y6M15D (5.542 years)
        
        Args:
            input_string: Period string in ISO 8601 duration format.
            
        Returns:
            Float representing years (with fractional component).
        """
        if not input_string or not input_string.startswith('P'):
            return 0.0
        
        years_str = ''
        months_str = ''
        days_str = ''
        
        # Extract years
        if 'Y' in input_string:
            y_pos = input_string.find('Y') - 1
            for i in range(y_pos, 0, -1):
                if input_string[i].isdigit():
                    years_str = input_string[i] + years_str
                else:
                    break
        
        # Extract months
        if 'M' in input_string:
            m_pos = input_string.find('M') - 1
            for i in range(m_pos, 0, -1):
                if input_string[i].isdigit():
                    months_str = input_string[i] + months_str
                else:
                    break
        
        # Extract days
        if 'D' in input_string:
            d_pos = input_string.find('D') - 1
            for i in range(d_pos, 0, -1):
                if input_string[i].isdigit():
                    days_str = input_string[i] + days_str
                else:
                    break
        
        years = float(years_str) if years_str else 0.0
        months = float(months_str) if months_str else 0.0
        days = float(days_str) if days_str else 0.0
        
        return years + (months / 12) + (days / 360)
    
    def _process_contexts(self, tree: ET.ElementTree) -> None:
        """Extract context information from XBRL file.
        
        Args:
            tree: Parsed XML tree.
        """
        context_list = tree.iter(tag='{http://www.xbrl.org/2003/instance}context')
        
        for cnx_item in context_list:
            ctx_id = cnx_item.attrib.get('id')
            if not ctx_id:
                continue
                
            ctx_data = ContextData(id=ctx_id)
            
            # Process period information
            for cnx_child in cnx_item.iter():
                if cnx_child.tag.endswith('}period'):
                    for date_child in cnx_child.iter():
                        if date_child.tag.endswith('}endDate'):
                            ctx_data.asofdate = date_child.text
                        elif date_child.tag.endswith('}instant'):
                            ctx_data.asofdate = date_child.text
            
            # Process other context elements
            for context_child in cnx_item.iter():
                for att, val in context_child.attrib.items():
                    if not val:
                        continue
                        
                    if att == 'dimension':
                        # Handle dimension attributes specially
                        if context_child.text and context_child.text.strip():
                            dim_text = context_child.text.strip()
                            
                            # Concatenate multiple dimensions
                            if ctx_data.dimension:
                                ctx_data.dimension = f"{val} / {ctx_data.dimension}"
                            else:
                                ctx_data.dimension = val
                            
                            # Concatenate dimension text
                            if ctx_data.text:
                                ctx_data.text = f"{dim_text} / {ctx_data.text}"
                            else:
                                ctx_data.text = dim_text
                    else:
                        # Store other attributes
                        ctx_data.attributes[att] = val
            
            # Convert to dictionary format for compatibility
            self.context_dictionary[ctx_id] = {
                'text': ctx_data.text,
                'asofdate': ctx_data.asofdate,
                'dimension': ctx_data.dimension,
                **ctx_data.attributes
            }
    
    def parse_xml(self, xml_file: Path) -> bool:
        """Parse XBRL instance XML file.
        
        Args:
            xml_file: Path to XML file.
            
        Returns:
            True if parsing successful, False otherwise.
        """
        try:
            tree = ET.parse(xml_file)
        except ET.ParseError as e:
            logger.error(f"XML parse error in {xml_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error reading {xml_file}: {e}")
            return False
        
        # Process context elements
        self._process_contexts(tree)
        
        # Process all other elements
        for element in tree.iter():
            if self._exclusion_found(element.tag):
                continue
            
            # Skip context tags (already processed)
            if element.tag.endswith('context'):
                continue
            
            # Get or create ID
            elem_id = element.attrib.get('id')
            if not elem_id:
                elem_id = self._create_id()
            
            # Extract XBRL data
            xbrl_data = XBRLData(
                tag=element.tag,
                string_value=element.text if element.text else '',
                context_ref=element.attrib.get('contextRef', 'null'),
                unit_ref=element.attrib.get('unitRef', 'null'),
                decimals=element.attrib.get('decimals', 'null')
            )
            
            # Convert to dictionary for compatibility
            self.storage_gaap[elem_id] = {
                'tag': xbrl_data.tag,
                'StringValue': xbrl_data.string_value,
                'contextRef': xbrl_data.context_ref,
                'unitRef': xbrl_data.unit_ref,
                'decimals': xbrl_data.decimals
            }
        
        logger.info(
            f"Parsed {len(self.context_dictionary)} contexts, "
            f"{len(self.storage_gaap)} data points"
        )
        return True
    
    def parse_presentation_xml(self, file_name: str) -> pd.DataFrame:
        """Parse XBRL presentation XML to understand field relationships.
        
        Args:
            file_name: Base filename (presentation file derived from this).
            
        Returns:
            DataFrame with presentation arc information.
        """
        pres_file_name = file_name.replace(".csv", "_pre.xml")
        file_path = Path.cwd() / pres_file_name
        
        if not file_path.exists():
            logger.warning(f"Presentation XML not found: {pres_file_name}")
            return pd.DataFrame()
        
        try:
            tree = ET.parse(file_path)
        except ET.ParseError as e:
            logger.error(f"Failed to parse presentation XML {file_path}: {e}")
            return pd.DataFrame()
        
        arc_list = []
        elements = tree.findall('{http://www.xbrl.org/2003/linkbase}presentationLink')
        
        for element in elements:
            # Get presentation section from role attribute
            presentation_section = ''
            for key, value in element.attrib.items():
                if '}' in key:
                    key_name = key.split('}')[1]
                    if key_name == 'role':
                        presentation_section = value.rsplit('/', 1)[1]
            
            # Process presentation arcs
            for child_element in element.iter():
                element_split = child_element.tag.split('}')
                if len(element_split) < 2:
                    continue
                    
                label = element_split[1]
                
                if label == 'presentationArc':
                    arc_data = {'Pres_Sect': presentation_section}
                    
                    for key, value in child_element.attrib.items():
                        if '}' not in key:
                            continue
                            
                        new_key = key.split('}')[1]
                        
                        if new_key == 'from':
                            subsection = (
                                value.rsplit('_', 2)[1] if '_' in value else value
                            )
                            arc_data['Subsection'] = subsection
                        elif new_key == 'to':
                            field_name = (
                                value.rsplit('_', 2)[1] if '_' in value else value
                            )
                            arc_data['to'] = field_name
                    
                    if 'to' in arc_data and 'Subsection' in arc_data:
                        arc_list.append([
                            'presentation',
                            arc_data['Pres_Sect'],
                            arc_data['Subsection'],
                            arc_data['to']
                        ])
        
        if not arc_list:
            logger.warning(f"No presentation arcs found in {pres_file_name}")
            return pd.DataFrame()
        
        df = pd.DataFrame(
            arc_list,
            columns=['FILE', 'Section_Value', 'Sub-Section_VALUE', 'Field_Name']
        )
        logger.info(f"Loaded {len(df)} presentation arcs")
        return df
    
    def _search_arc_list(
        self,
        arc_df: pd.DataFrame,
        column_name: str,
        row_string: str,
        dimension_string: str
    ) -> Tuple[str, str]:
        """Search presentation arcs to find section/subsection for a field.
        
        Args:
            arc_df: DataFrame with presentation arcs.
            column_name: Field name to search for.
            row_string: Context text to match.
            dimension_string: Dimension to match.
            
        Returns:
            Tuple of (section, subsection) strings.
        """
        if arc_df.empty:
            return '', ''
        
        # Handle table member searches
        lower_row = row_string.lower()
        if 'member' in lower_row and lower_row.endswith('member'):
            if ':' in row_string:
                table_member_name = row_string.rsplit(':', 1)[1]
            else:
                table_member_name = row_string
            filtered_df = arc_df.loc[arc_df['Field_Name'] == table_member_name]
        else:
            filtered_df = arc_df.loc[arc_df['Field_Name'] == column_name]
        
        if filtered_df.empty:
            return '', ''
        
        # Simple case: single match
        if len(filtered_df) == 1:
            return (
                filtered_df.iloc[0]['Section_Value'],
                filtered_df.iloc[0]['Sub-Section_VALUE']
            )
        
        # Multiple matches: try to find best match
        # For now, return first match (could be enhanced)
        return (
            filtered_df.iloc[0]['Section_Value'],
            filtered_df.iloc[0]['Sub-Section_VALUE']
        )
    
    def _get_report_date(self, file_name: str) -> str:
        """Extract report date from parsed data or filename.
        
        Args:
            file_name: XML filename.
            
        Returns:
            Report date in YYYYMMDD format.
        """
        # Try to find DocumentPeriodEndDate in parsed data
        for key, data in self.storage_gaap.items():
            if 'DocumentPeriodEndDate' in data['tag']:
                date_str = data['StringValue']
                if date_str and '-' in date_str:
                    try:
                        return datetime.strptime(
                            date_str, "%Y-%m-%d"
                        ).strftime("%Y%m%d")
                    except ValueError:
                        logger.warning(f"Invalid date format: {date_str}")
        
        # Fall back to filename
        logger.warning("Using date from filename")
        return file_name[:8] if len(file_name) >= 8 else ''
    
    def write_csv(self, file_name: str, ticker: str) -> None:
        """Write parsed XBRL data to CSV file.
        
        Args:
            file_name: Output CSV filename.
            ticker: Stock ticker symbol.
        """
        output_path = Path.cwd() / file_name
        
        try:
            with open(output_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                writer.writerow(CSV_HEADERS)
                
                # Load presentation data
                arc_df = self.parse_presentation_xml(file_name)
                has_presentation = not arc_df.empty
                
                # Get report date
                report_date = self._get_report_date(file_name)
                
                # Write each data point
                for key, data in self.storage_gaap.items():
                    # Get context information
                    date_string = report_date
                    dimension_string = ''
                    row_string = ''
                    
                    context_ref = data.get('contextRef', 'null')
                    if context_ref != 'null' and context_ref in self.context_dictionary:
                        context = self.context_dictionary[context_ref]
                        
                        # Get date
                        if 'asofdate' in context and context['asofdate']:
                            date_str = context['asofdate']
                            if '-' in date_str:
                                clean_date = date_str.strip()
                                if 'T' in clean_date:
                                    clean_date = clean_date.split('T')[0]
                                try:
                                    date_string = datetime.strptime(
                                        clean_date, "%Y-%m-%d"
                                    ).strftime("%Y%m%d")
                                except ValueError:
                                    date_string = report_date
                        
                        # Get dimension and text
                        dimension_string = context.get('dimension', '')
                        row_string = context.get('text', '')
                    
                    # Extract column name from tag
                    raw_tag = data['tag']
                    if '}' in raw_tag:
                        column_name = raw_tag[raw_tag.rindex('}') + 1:]
                    else:
                        column_name = raw_tag
                    
                    # Process value
                    string_value = data['StringValue']
                    
                    if string_value.isnumeric():
                        output_value = string_value
                    elif string_value.startswith('P'):
                        # Period conversion
                        if len(string_value) > 1 and string_value[1].isdigit():
                            if any(c in string_value for c in ['Y', 'M', 'D']):
                                converted = self._convert_period_to_years(string_value)
                                output_value = str(converted)
                            else:
                                output_value = string_value
                        else:
                            output_value = string_value
                    else:
                        output_value = string_value
                    
                    # Suppress font definitions in text blocks
                    if any(
                        font in output_value.lower()
                        for font in ['font-', 'font:']
                    ):
                        output_value = 'Suppressed'
                    
                    # Find section/subsection from presentation
                    if has_presentation:
                        section, subsection = self._search_arc_list(
                            arc_df, column_name, row_string, dimension_string
                        )
                    else:
                        section = ''
                        subsection = ''
                    
                    # Write row
                    decimals = data.get('decimals', '')
                    row = [
                        ticker,
                        report_date,
                        date_string,
                        section,
                        subsection,
                        dimension_string,
                        row_string,
                        column_name,
                        output_value,
                        decimals
                    ]
                    writer.writerow(row)
            
            logger.info(f"Wrote CSV: {file_name}")
            
        except IOError as e:
            logger.error(f"Failed to write CSV {output_path}: {e}")
            raise


def process_multiple_files(input_file_name: str) -> int:
    """Process multiple XML files based on ticker list.
    
    Args:
        input_file_name: CSV file with Ticker and CIK columns.
        
    Returns:
        Number of files processed.
    """
    base_dir = Path.cwd()
    input_path = base_dir / input_file_name
    
    try:
        ticker_cik_df = pd.read_csv(input_path)
    except Exception as e:
        logger.error(f"Failed to read {input_path}: {e}")
        return 0
    
    if 'Ticker' not in ticker_cik_df.columns or 'CIK' not in ticker_cik_df.columns:
        logger.error("Input CSV must have 'Ticker' and 'CIK' columns")
        return 0
    
    file_counter = 0
    output_xml_dir = base_dir / "OutputXML"
    
    for _, row in ticker_cik_df.iterrows():
        ticker = row['Ticker']
        cik = row['CIK']
        
        ticker_dir = output_xml_dir / ticker
        if not ticker_dir.exists():
            logger.warning(f"Directory not found: {ticker_dir}")
            continue
        
        logger.info(f"Processing files for {ticker}")
        
        # Get all XML files (excluding presentation files)
        xml_files = [
            f for f in ticker_dir.glob('*.xml')
            if '_pre.xml' not in f.name.lower()
        ]
        
        for xml_file in xml_files:
            logger.info(f"Processing: {xml_file.name}")
            
            # Parse the XML file
            parser = XBRLParser()
            
            if not parser.parse_xml(xml_file):
                logger.error(f"Failed to parse {xml_file.name}")
                continue
            
            # Write CSV output
            csv_filename = xml_file.name.replace('.xml', '.csv')
            output_dir = ticker_dir
            
            try:
                os.chdir(output_dir)
                parser.write_csv(csv_filename, ticker)
                logger.info(f"Successfully processed {xml_file.name}")
                file_counter += 1
            except Exception as e:
                logger.error(f"Error writing CSV for {xml_file.name}: {e}")
            finally:
                os.chdir(base_dir)
    
    # Concatenate CSV files per ticker
    concatenate_csv_files(input_file_name, base_dir)
    
    return file_counter


def concatenate_csv_files(input_file_name: str, base_directory: Path) -> None:
    """Combine all CSV files for each ticker into one file.
    
    Args:
        input_file_name: CSV file with ticker list.
        base_directory: Base working directory.
    """
    try:
        ticker_cik_df = pd.read_csv(base_directory / input_file_name)
    except Exception as e:
        logger.error(f"Failed to read {input_file_name}: {e}")
        return
    
    output_xml_dir = base_directory / "OutputXML"
    
    for _, row in ticker_cik_df.iterrows():
        ticker = row['Ticker']
        ticker_dir = output_xml_dir / ticker
        
        if not ticker_dir.exists():
            continue
        
        # Get all CSV files (excluding combined file)
        csv_files = [
            f for f in ticker_dir.glob('*.csv')
            if '_combined.csv' not in f.name.lower()
        ]
        
        if not csv_files:
            logger.warning(f"No CSV files found for {ticker}")
            continue
        
        try:
            # Combine all CSVs
            combined_df = pd.concat(
                [pd.read_csv(f, low_memory=False) for f in csv_files],
                ignore_index=True
            )
            
            # Write combined file
            combined_file = ticker_dir / f"{ticker}_Combined.csv"
            combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
            logger.info(f"Created combined CSV for {ticker}")
            
        except Exception as e:
            logger.error(f"Error combining CSV files for {ticker}: {e}")


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Parse SEC XBRL XML files to CSV'
    )
    parser.add_argument(
        '-i', '--input',
        default='Ticker_CIK_List.csv',
        help='Input CSV file with Ticker and CIK columns'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_file = args.input
    logger.info(f"Input file: {input_file}")
    
    file_count = process_multiple_files(input_file)
    
    if file_count > 0:
        logger.info(f"Successfully processed {file_count} XML files")
    else:
        logger.warning("No XML files were processed")


if __name__ == "__main__":
    main()

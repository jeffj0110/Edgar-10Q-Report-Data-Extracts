# Edgar-10Q-Report-Data-Extracts
There are two python3 scripts included. 

    10Q_10K_JSON_Downloader.py - This script will take as input a CSV based list of tickers/CIK's.  It will download all the 10Q/10K filings from the SEC website back until around 2011 and store each quarterly filings in a file with a naming convention of 'YYYYMMDD_Ticker_CIK_ReportTYpe.XML'.
    
    10Q_10K_XML_Parser_v3.py - This is the latest XBRL parser which processes the Instance and Presentation XBRL XML files for a 10Q or 10K report.  It will generate a CSV file containing all the data points which are defined in the report.
    
    Both scripts recently upgraded to Python v3.12

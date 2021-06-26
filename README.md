# Edgar-10Q-Report-Data-Extracts
There are two python3 scripts included. 

    10Q_10K_Downloader.py - This script will take as input a CSV based list of tickers/CIK's.  It will download all the 10Q/10K filings from the SEC website back until around 2011 and store each quarterly filings in a file with a naming convention of 'YYYYMMDD_Ticker_CIK_ReportTYpe.XML'.
    
    10Q_10K_XML_Parser.py - This script parses all the data points from the files which were downloaded by the script above and creates a CSV file with the parsed datapoints.
    
    

# Reads a list of json files which are downloaded from the SEC website.
# and  parses the lines with 10k/10Q filings associated with them.
# The Accession and values are then used to submit a request to download the associated files for
# that filing.
#
# import our libraries
#import ref
import requests
import time
import xlrd
import sys, getopt, os
import os.path
from os import path
import pathlib
import pandas as pd
from datetime import datetime
import unicodedata
from bs4 import BeautifulSoup
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

global CWD, XML_File_Count


def restore_windows_1252_characters(restore_string):
    """
        Replace C1 control characters in the Unicode string s by the
        characters at the corresponding code points in Windows-1252,
        where possible.
    """

    def to_windows_1252(match):
        try:
            return bytes([ord(match.group(0))]).decode('windows-1252')
        except UnicodeDecodeError:
            # No character at the corresponding code point: remove it.
            return ''

    return re.sub(r'[\u0080-\u0099]', to_windows_1252, restore_string)

def Extract_XML_File_URLs(Input_URL):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers['User-Agent'] = 'Financial Inference Technology jeffjones@fitsolutionsusa.com'
#    print(session.headers.keys())
# This is to avoid overruning the SEC Website.  Too many requests and they will disable your IP for a period of time.
    print("Sleep 1")
    time.sleep(1)
    new_html_text = Input_URL

    response = session.get(new_html_text)

    return_status = response.status_code
    if return_status == 200 :
        print("Successful request to SEC Site : ", new_html_text)
    else:
        print("Unsuccessful request to SEC Site")
        print("Status Code = ", return_status)
        print(new_html_text)
        return('none', 'none')

    # pass it through the html parser,
    soup = BeautifulSoup(response.content, 'html.parser')
    # Just looking for table rows in the html page
    row_list = soup.find_all("tr")
    instance_url_string = ''
    pres_url_string = ''
    XML_File_Names = []
    row_count = len(row_list)
    # print(row_count)
    for x in range(row_count):   #    for att in tag.attrib :
        row = row_list[x]
        row_string = row.get_text()
        if ('INSTANCE DOCUMENT' in row_string) or ('EX-101.INS' in row_string) or ('INSTANCE FILE' in row_string) or ('EXHIBIT 101.INS' in row_string) :
#        print("Row ", x, "Text ", row_string)
            tags_with_href = row.find(href=True)
            if tags_with_href != None :
                instance_url_string = tags_with_href.attrs['href']
            else:
                return 'none', 'none'
        if ('PRESENTATION LINKBASE' in row_string) or ('EX-101.PRE' in row_string) or ('EXHIBIT 101.PRE' in row_string):
            #        print("Row ", x, "Text ", row_string)
            tags_with_href = row.find(href=True)
            if tags_with_href != None:
                pres_url_string = tags_with_href.attrs['href']
            else:
                return 'none', 'none'

    #if we reach here, both urls should be populated
    if instance_url_string != '' and pres_url_string != '' :
        return instance_url_string, pres_url_string
    else :
        return 'none', 'none'


# Data extraction for filings when a request is submitted in the form -
# https://data.sec.gov/submissions/CIK##########.json
def Create_json_request(tck, cik) :
    base_request = 'https://data.sec.gov/submissions/CIK##########.json'
    # Need zero padded cik
    cik_string = str(cik)
    if (len(cik_string) < 4)  :
        return ""
    padded_cik = cik_string.rjust(10,'0')
    request_string = base_request.replace('##########',padded_cik)
    return request_string


def Get_json_object(json_request) :
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers['User-Agent'] = 'Financial Inference Technology jeffjones@fitsolutionsusa.com'
    print("Sleeping for 1 second")
    time.sleep(1)
    response = session.get(json_request)
    return_status = response.status_code
    if return_status == 200:
        print("Successful request to SEC Site : ", json_request)
    else :
        print("Unsuccessful request to SEC Site")
        print("Status Code = ", return_status, json_request)
        return ""

    return response.content

# Open JSON file that has submissions to SEC website for given ticker and CIK.
#
def Get_Submission_File_URLs(ticker: str, CIK: str, Submissions: dict) :
    #Read list of tickers / CIK for companies to be processed
    global CWD
    os.chdir(CWD)
    base_directory = pathlib.Path.cwd()
    num_submissions = 0
    json_request = Create_json_request(ticker, CIK)
    if len(json_request) > 0:
        json_obj = Get_json_object(json_request)
        json_object = json.loads(json_obj)
    else :
        print("No JSON request created for ", ticker, " ", CIK)
        return False

    filings = json_object['filings']
    recent_filings = filings['recent']
    forms = recent_filings['form']
    # For every XBRL submission, add a row to the pandas dataframe
    Submission_List = []
    for i in range(len(forms)):
        if (forms[i] == '10-K' or forms[i] == '10-Q') and (recent_filings['isXBRL'][i] == 1) :
#            print("Found ", forms[i], " for ", recent_filings['reportDate'][i], " Is This XBRL? ", recent_filings['isXBRL'][i])
            Form_Type = forms[i]
            AccessionNums = recent_filings['accessionNumber'][i]
            Reporting_Date = recent_filings['reportDate'][i]
            Filing_Date = recent_filings['filingDate'][i]
            Form_Description = recent_filings['primaryDocDescription'][i]
            Filing_Number = recent_filings['fileNumber'][i]
            base_url = 'https://www.sec.gov/Archives/edgar/data/CIK/AccessionNumberNoDashes/accessionNumber-index.htm'
            new_url = base_url.replace("accessionNumber", AccessionNums)
            AccessionWO_Dash = AccessionNums.replace("-","")
            url_submission = new_url.replace("AccessionNumberNoDashes", AccessionWO_Dash)
            new_row = [Form_Type,
                       Form_Description,
                       Filing_Date,
                       Reporting_Date,
                       Filing_Number,
                       AccessionNums,
                       url_submission]
            Submission_List.append(new_row)

    num_submissions = len(Submission_List)

    if num_submissions < 1 :
        print(f"No SEC File Submissions Found for  {ticker}")
        return False
    else:
        Submission_DF = pd.DataFrame(Submission_List, columns=['Form type', 'Form description', 'Filing date', 'Reporting date', 'File numbers', 'Accession number', 'Filings URL'])

    for i in range(num_submissions) :
        row = Submission_DF.iloc[i]
        #determine if a row is associated with a 10-Q or 10-K
        #if so, then extract the Form type, filing date and reporting date along with the URL.
        #we only want the latest filings, so don't keep older filings
        form_type = str(row['Form type'])
        if (('10-K' in form_type) or ('10-Q' in form_type)) and ('A' not in form_type) :
            #extract the url if it a more current filing that one we already have
            file_date = row['Filing date']
            report_date = row['Reporting date']
            filing_url = row['Filings URL']
            if len(Submissions) == 0 :
                print("Initializing submissions list")
                Submissions[report_date] = [file_date, report_date, filing_url, form_type]
            else:
                if Submissions.get(report_date) != None :
                    print("Taking latest amended filing", Submissions.get(report_date)[0])
                else:
#                    print("adding to submissions list")
#                    print(row)
                    Submissions[report_date] = [file_date, report_date, filing_url, form_type]
    if len(Submissions) > 0 :
        return True
    else:
        return False

def  Report_Exists(filenamestring) :

    if path.exists(filenamestring) :
        return True
    else:
        return False

def Retrieve_Store_XML_Files(tck, cik, Report_Type, Report_Date, XML_Download_URL, PRE_XML_URL) :
    global CWD
    global XML_File_Count
    os.chdir(CWD)
    os.makedirs('OutputXML', exist_ok=True)
    os.chdir('OutputXML')
    os.makedirs(tck, exist_ok=True)
    os.chdir(tck)
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers['User-Agent'] = 'Financial Inference Technology jeffjones@fitsolutionsusa.com'
#    print(session.headers.keys())

    # Need to convert the date string to YYYYMMDD
    date_time_obj = datetime.strptime(Report_Date, '%Y-%m-%d')
    new_date_string = str(date_time_obj.year) + str(date_time_obj.month).zfill(2) + str(date_time_obj.day).zfill(2)
    # Get rid of any \ or / characters in the Report_Type
    Rep_Type = Report_Type.replace('/', '')
    filenamestring = new_date_string + '_' + tck + '_' + str(cik) + '_' + Rep_Type + '.xml'
    #if file exists, don't process it again.
    if Report_Exists(filenamestring) == False :
        # This is to avoid overruning the SEC Website.  Too many requests and they will disable your IP for a period of time.
        print("Sleep 1")
        time.sleep(1)
        new_html_text = 'https://www.sec.gov' + XML_Download_URL
        response = session.get(new_html_text)
        return_status = response.status_code
        if return_status == 200:
            print("Successful request to SEC Site : ", new_html_text)
            open(filenamestring, 'wb').write(response.content)
            XML_File_Count = XML_File_Count + 1
        else:
            print("Unsuccessful request to SEC Site")
            print("Status Code = ", return_status)
            print(new_html_text)
            return False
    else:
        print(f"Already processed {filenamestring}")

    # Retrieve and write the Presentation xml schema file
    filenamestring = new_date_string + '_' + tck + '_' + str(cik) + '_' + Rep_Type + '_Pre' + '.xml'
    # if file exists, don't process it again.
    if Report_Exists(filenamestring) == False:
        # This is to avoid overrunning the SEC Website.  Too many requests and they will disable your IP for a period of time.
        print("Sleep 1")
        time.sleep(1)
        new_html_text = 'https://www.sec.gov' + PRE_XML_URL
        response = session.get(new_html_text)
        return_status = response.status_code
    else:
        print(f"Already processed {filenamestring}")
        return False

    if return_status == 200:
        print("Successful request to SEC Site : ", new_html_text)
    else:
        print("Unsuccessful request to SEC Site")
        print("Status Code = ", return_status)
        print(new_html_text)
        return False


    open(filenamestring, 'wb').write(response.content)
    XML_File_Count = XML_File_Count + 1

    return True

def get_10k_10Q(Input_File_Name) :
    # Read Excel input and extract the list of 10K's and 10Q's which have been filed.
    # Then request the page which has the XML file name on it (if available)
    # If no XML file name available for a filed 10k/q, then skip that one

    #Read list of tickers / CIK for companies to be processed
    global CWD
    base_directory = pathlib.Path.cwd()
    fname = base_directory.joinpath(Input_File_Name).resolve()
    Ticker_CIK = pd.read_csv(fname)
    num_tickers = len(Ticker_CIK)

    # Read in input Excel file with all the filings (downloading Excel files has to be done manually right now)
    for row in range(num_tickers) :
        Filing_URL_List = {}
        row_data = Ticker_CIK.iloc[row]
        tck = row_data['Ticker']
        cik = row_data['CIK']
        if Get_Submission_File_URLs(tck, cik, Filing_URL_List) :
            print(f"Submissions Found For {tck}, {cik} ", len(Filing_URL_List))
        else:
            print(f"submissions Not Found For {tck}, {cik}")
            return False
        # Obtain a list of URL's to download 10Q/10K reports for given ticker and CIK
        Num_Filings = len(Filing_URL_List)
        if Num_Filings > 0 :
            for rep_date in Filing_URL_List :
                Report_Date = Filing_URL_List.get(rep_date)[1]
                date_time_obj = datetime.strptime(Report_Date, '%Y-%m-%d')
                #Convert to date object
                #Don't request to retrieve anything if the file already has been processed.
                # Need to convert the date string to YYYYMMDD
                new_date_string = str(date_time_obj.year) + str(date_time_obj.month).zfill(2) + str(date_time_obj.day).zfill(2)
                # Get rid of any \ or / characters in the Report_Type
                Report_Type = Filing_URL_List.get(rep_date)[3]
                Rep_Type = Report_Type.replace('/', '')
                filenamestring = new_date_string + '_' + tck + '_' + str(cik) + '_' + Rep_Type + '.xml'
                pres_filenamestring = new_date_string + '_' + tck + '_' + str(cik) + '_' + Rep_Type + '_Pre' + '.xml'
                if date_time_obj.year > 2010 :
                    Input_URL = Filing_URL_List.get(rep_date)[2]
                    os.chdir(CWD)
                    if (Report_Exists('OutputXML\\' + tck + '\\' + filenamestring) == False) and (Report_Exists('OutputXML\\' + tck + '\\' + pres_filenamestring) == False):
                        XML_Download_URL, Pre_XML_URL  = Extract_XML_File_URLs(Input_URL)
                        if XML_Download_URL != 'none' :
                            Retrieve_Store_XML_Files(tck, cik, Report_Type, Report_Date, XML_Download_URL, Pre_XML_URL)
                        else:
                            print(f'No XBRL Instance URL Found For : {tck}, {Report_Type}, {Report_Date}')
                    else:
                        print(f"Already Retrieved Files For {new_date_string} {tck} {Rep_Type}")
    return True

# Check to see if this file is being executed as the "Main" python
# script instead of being used as a module by some other python script
# This allows us to use the module which ever way we want.
def main(argv):
   global CWD
   global XML_File_Count
   CWD = os.getcwd()
   XML_File_Count = 0
   inputfile = 'Ticker_CIK_List.csv'
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print('10Q_10K_Downloader -i <inputfile>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('10Q_10K_Downloader -i <inputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg


   if 'xxxxxx' in inputfile :
       return_code = 0
   else:
       return_code = get_10k_10Q(inputfile)

   if return_code > 0 :
       print("Number Filings Retrieved = ", XML_File_Count)
   else:
       print("No XML Files Retrieved For Tickers/CIK's in the list in : ",  inputfile)
   return

if __name__ == "__main__":
   main(sys.argv[1:])


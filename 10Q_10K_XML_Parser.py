# Example script for Edgar data extraction
# June 2021 : Jeff Jones
# import our libraries
import sys, getopt, os
import glob
import re
from datetime import datetime
from typing import Generator
from xml.etree.ElementTree import Element
import pandas as pd
import requests
import unicodedata
import lxml
from bs4 import BeautifulSoup
import csv
import pprint
import pathlib
import collections
import xml.etree.ElementTree as ET

Tag_Ig_Count = int(0)


ExclusionTags = [
                 '}xbrl',
                 'html',
#                 '}loc',
                 '}footnote',
#                 'footnoteArc',
#                 'footnoteLink',
#                 '}schemaRef',
                 '}context',
                 '}unit',
                 '}entity',
#                 'CIK',
#                 '}identifier',
                '}period',
                '}startDate',
                '}endDate',
#                'scheme',
                '}segment',
#                 '}instant',
#                 'measure',
#                 'unitDenominator',
#                 'unitNumerator',
#                 'divide',
#                 'table',
#                 '}tr',
#                 '}td',
#                 '}span',
#                 '}div',
#                 '}p',
#                 '}br',
                 'explicitMember',
                 'dimension',
#                 'TextBlock'
 ]

def ExclusionFound(Search_String):
    for str in ExclusionTags:
        if str in Search_String:
#            print("excluding ", Search_String)
            return True
    return False


# Find matching contextRef for a field
def Find_contextRef(id):
    if id in context_dictionary.keys():
        return context_dictionary[id].items()
    else:
        return None


#    PARSE THE HTML FILE.
def Process_Contexts(tree) :
    context_list = tree.iter(tag='{http://www.xbrl.org/2003/instance}context')
    for cnx_item in context_list:
        context_dictionary[cnx_item.attrib['id']] = {}
        context_dictionary[cnx_item.attrib['id']]['text'] = ''
#        print('Iterating through context', cnx_item.tag)
#        print("Context Element ID", cnx_item.attrib.get('id', 'null'))
#        context_children = cnx_item.getchildren()
        for cnx_child in cnx_item.iter():
            #extract as of dates from context
#            print(cnx_child.tag)
            if cnx_child.tag.endswith('}period') :
#                period_objects = cnx_child.getchildren()
                for date_child in cnx_child.iter() :
#                    print(cnx_child.tag, date_child.tag)
                    if date_child.tag.endswith('}endDate') :
                        context_dictionary[cnx_item.attrib['id']]['asofdate'] = date_child.text
                    elif date_child.tag.endswith('}instant') :
                        context_dictionary[cnx_item.attrib['id']]['asofdate'] = date_child.text

        for context_child in cnx_item.iter():
#            print("Iterating through levels of context", context_child.tag)
            for att in context_child.attrib:
                if att:
#                    print("Attribute", att)
                    context_dictionary[cnx_item.attrib['id']][att] = context_child.attrib[att]
                    if not context_child.text is None:
                        if context_child.text.strip() != '':
                            if att == 'dimension':
                                if 'text' in context_dictionary[cnx_item.attrib['id']].keys():
                                    if context_dictionary[cnx_item.attrib['id']]['text'] != '':
                                        context_dictionary[cnx_item.attrib['id']]['text'] = context_child.text.strip() + " / " + \
                                                  context_dictionary[cnx_item.attrib['id']]['text']
                                    else:
                                        context_dictionary[cnx_item.attrib['id']]['text'] = context_child.text.strip()
                                else:
                                    context_dictionary[cnx_item.attrib['id']]['text'] = context_child.text.strip()
                            else:
                                context_dictionary[cnx_item.attrib['id']][att] = context_child.text.strip()
                    else:
                        context_dictionary[cnx_item.attrib['id']]['text'] = ''


    return

def Parse_XML(xml_file):
    # Initalize storage units, one will store all the context fields, and one will store all GAAP info.

    # Load the HTML file.
    tree = ET.parse(xml_file)
    Process_Contexts(tree)
    # pprint.pprint(context_dictionary)
    # loop through all the other elements in the HTML file.
    for element in tree.iter():
#        print("Processing Tag", (element.tag))
        # for element_item in element.iter():
# The iterator will still pick up context objects, so we filter them out
        if ExclusionFound(element.tag) != True:
            # We should be excluding all context objects and their children, since we already processed them.
            if element.tag.endswith('context'):
                print('We should never pass this point - there is a problem', element.tag)
                return False
            else:
                # Grab the required values
                if (element.attrib.get('id', 'null') != 'null') :
                    storage_gaap[element.attrib['id']] = {}
                    storage_gaap[element.attrib['id']]['tag'] = element.tag
                    storage_gaap[element.attrib['id']]['decimals'] = element.attrib.get('decimals', 'null')
#                    if element.attrib.get('contextRef', 'null') != 'null':
                    storage_gaap[element.attrib['id']]['contextRef'] = element.attrib.get('contextRef', 'null')
#                    else:
#                        pprint.pprint(element)
                    storage_gaap[element.attrib['id']]['unitRef'] = element.attrib.get('unitRef', 'null')
                    storage_gaap[element.attrib['id']]['decimals'] = element.attrib.get('decimals', 'null')
                    if not element.text is None:
                        storage_gaap[element.attrib['id']]['StringValue'] = element.text
                    else:
                        storage_gaap[element.attrib['id']]['StringValue'] = ''
#                else:
#                    print("Ignoring this tag", element.tag)

    print(f"Number of Context Entries : {len(context_dictionary)}")
    print(f"Number of Table Values : {len(storage_gaap)}")
    return True

def Write_CSV(file_name, ticker) :
    # open the file in the current working directory
    with open(file_name, mode='w', newline='', encoding='utf-8') as sec_file:
        # create the writer.
        writer = csv.writer(sec_file, quoting=csv.QUOTE_ALL)

        # write the header.
        writer.writerow(['Ticker', 'Report_Date', 'Field_Date', 'Table Row', 'Column', 'Value', 'Value_Rounding'])
        # Need DocumentPeriodEndDate field for the report date in each record
        Report_Date = ''
        for storage_1 in storage_gaap:
            if 'DocumentPeriodEndDate' in storage_gaap[storage_1]['tag']:
                Report_Date = storage_gaap[storage_1]['StringValue']
                break

        # If we do not find the DocumentPeriodEndDate, print error to console
        if Report_Date == '' :
            print("No DocumentPeriodEndDate found in XML submission For ", file_name)
            print("Using date from file name")
            Report_Date = file_name[0:8]
        else:
            #Convert string to YYYYMMDD
            Report_Date = datetime.strptime(Report_Date, "%Y-%m-%d").strftime("%Y%m%d")

        # start at level 1
        for storage_1 in storage_gaap:
            # Cont_Ref = Find_contextRef(storage_gaap[storage_1]['contextRef'])
            # print(Cont_Ref)
            # RowStr2 = dict(Cont_Ref['text'])
            if 'contextRef' in storage_gaap[storage_1].keys() :
                id = storage_gaap[storage_1]['contextRef']
                if id != 'null' :
                    length = len(id)
                    if 'asofdate' in context_dictionary[id].keys() :
                        date_string = context_dictionary[id]['asofdate']
                        if '-' in date_string :
                            stripped_date_string = date_string.strip('\n')
                            date_string = datetime.strptime(stripped_date_string, "%Y-%m-%d").strftime("%Y%m%d")
                        else:
                            date_string = Report_Date
                    else:
                        date_string = Report_Date

                    CombinedText = context_dictionary[id]['text']
                    Row_String = CombinedText
                else:
                    Row_String = ""

            RawTag = storage_gaap[storage_1]['tag']
            ColumnName = RawTag[RawTag.rindex('}') + 1:]
# The values should not be adjusted by the 'decimals' value.  This is just a number to indicate this value was rounded.
            if 'decimals' in storage_gaap[storage_1].keys() :
                dec_value = storage_gaap[storage_1]['decimals']
            else:
                dec_value = ""

            if storage_gaap[storage_1]['StringValue'].isnumeric() :
                original_xml_number = float(storage_gaap[storage_1]['StringValue'])
                converted_number = float(storage_gaap[storage_1]['StringValue'])
                output_converted_number = str(converted_number)
            else:
                output_converted_number = storage_gaap[storage_1]['StringValue']

            if 'font-' in output_converted_number or 'FONT-' in output_converted_number:
                output_converted_number = 'Suppressed'

            output_line = [ticker, Report_Date, date_string, Row_String, ColumnName, output_converted_number, dec_value]
            writer.writerow(output_line)

        sec_file.close()
    return

def SecParse_MultipleFiles(InputFileName) :
    CWD = os.getcwd()
    base_directory = pathlib.Path.cwd()
    fname = base_directory.joinpath(InputFileName).resolve()
    Ticker_CIK = pd.read_csv(fname)
    num_tickers = len(Ticker_CIK)
    file_counter = 0
    # Process all XML Files For A List of Tickers / CIK's
    # Process one file at a time and write the results
    #Read list of tickers / CIK for companies to be processed

    base_directory = pathlib.Path.cwd()
    # Read in input Excel file with all the filings (downloading Excel files has to be done manually right now)
    for row in range(num_tickers):
        os.chdir(CWD)
        os.chdir("OutputXML")
        row_data = Ticker_CIK.iloc[row]
        tck = row_data['Ticker']
        cik = row_data['CIK']
        # Change to the directory with each list of files for the ticker
        os.chdir(tck)
        # Get list of files
        base_directory = pathlib.Path.cwd()
        print("Working in Directory : ", base_directory)
        file_list = glob.glob('*.xml')
        for file_name in file_list :
            print(file_name)
            file_htm = base_directory.joinpath(file_name).resolve()
            print("Processing File : ", file_htm)
            global storage_gaap
            global context_dictionary
            storage_gaap = {}
            context_dictionary = {}
            Tag_Ig_Count = 0
            # Check response code
            if (Parse_XML(file_htm)):
                print(f"Successful extraction for {file_name}, Ignored {Tag_Ig_Count}")
                Tag_Ignore_Count = 0
                working_file_name = file_name.replace('xml', 'csv')
                Write_CSV(working_file_name, tck)
                print("Results Are Written To ", working_file_name)
                file_counter += 1
            else:
                print(f"Error Retrieving Data For {file_name}")

    os.chdir(CWD)
    Concatenate_CSV_Files(InputFileName, CWD)
    return file_counter

def Concatenate_CSV_Files(InputFileName, CurrentDirectory) :
    os.chdir(CurrentDirectory)
    base_directory = pathlib.Path.cwd()
    fname = base_directory.joinpath(InputFileName).resolve()
    Ticker_CIK = pd.read_csv(fname)
    num_tickers = len(Ticker_CIK)
    file_counter = 0
    for row in range(num_tickers):
        os.chdir(CurrentDirectory)
        os.chdir("OutputXML")
        row_data = Ticker_CIK.iloc[row]
        tck = row_data['Ticker']
        cik = row_data['CIK']
        os.chdir(tck)
        extension = 'csv'
        all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        # export to csv
        Combined_File_Name = tck + '_Combined.csv'
        combined_csv.to_csv(Combined_File_Name, index=False, encoding='utf-8-sig')
        print("Combined CSV Files For ", tck)
    return

# Check to see if this file is being executed as the "Main" python
# script instead of being used as a module by some other python script
# This allows us to use the module which ever way we want.
def main(argv):
   inputfile = 'Ticker_CIK_List.csv'
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print('get_10k_10Q -i <inputfile>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('get_10k_10Q -i <inputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg

   print('Input file is :', inputfile)
   return_code = SecParse_MultipleFiles(inputfile)

   if return_code > 0 :
       print("Number XML Files Processed = ", return_code)
   else:
       print("No XML Files Retrieved For ",inputfile)
       return False

if __name__ == "__main__":
   main(sys.argv[1:])
# Example script for Edgar data extraction
# June 2021 : Jeff Jones
# import our libraries
import os
import re
from datetime import datetime
from typing import Generator
from xml.etree.ElementTree import Element

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

Ticker_CIK_List = {
    "AGNC": [1423689, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1423689/000142368921000037/agnc-20210331.htm"],
    "NLY": [1043219, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1043219/000162828021009417/nly-20210331.htm"],
    "DX": [826675, "https://www.sec.gov/ix?doc=/Archives/edgar/data/826675/000082667521000033/dx-20210331.htm"],
    "TWO": [1465740, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1465740/000146574021000054/two-20210331.htm"],
    "CMO": [766701, "https://www.sec.gov/ix?doc=/Archives/edgar/data/766701/000156459021023425/cmo-10q_20210331.htm"],
    "IVR": [1437071, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1437071/000143707121000010/ivr-20210331.htm"],
    "CIM": [1409493, "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001409493/000162828021009312/cim-20210331.htm"],
    "PMT": [1464423, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1464423/000156459021025494/pmt-10q_20210331.htm"],
    "PFSI": [1745916,
             "https://www.sec.gov/ix?doc=/Archives/edgar/data/1745916/000155837021006277/pfsi-20210331x10q.htm"],
    "MFA": [1055160, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1055160/000105516021000007/mfa-20210331.htm"],
    "ARR": [1428205, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1428205/000142820521000134/arr-20210331.htm"],
    "EARN": [1560672,
             "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001560672/000156067221000041/earn-20210331.htm"],
    "EFC": [1411342, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1411342/000141134221000049/efc-20210331.htm"],
    "NYMT": [1273685,
             "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001273685/000127368521000053/nymt-20210331.htm"],
    "MITT": [1514281,
             "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001514281/000151428121000067/mitt-20210331.htm"],
    "NRZ": [1556593, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1556593/000155659321000010/nrz-20210331.htm"],
    "RWT": [930236, "https://www.sec.gov/ix?doc=/Archives/edgar/data/930236/000093023621000019/rwt-20210331.htm"],
    "AAIC": [1209028, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1209028/000156459021026020/ai-10q_20210331.htm"],
    "ORC": [1518621, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1518621/000151862121000079/orc10q20210331.htm"],
    "CHMI": [1571776,
             "https://www.sec.gov/ix?doc=/Archives/edgar/data/1571776/000114036121016620/brhc10024045_10q.htm"],
    "WMC": [1465885, "https://www.sec.gov/ix?doc=/Archives/edgar/data/1465885/000162828021009332/wmc-20210331.htm"]
}

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
                                context_dictionary[cnx_item.attrib['id']][att] = cnx_item.text.strip()
                    else:
                        context_dictionary[cnx_item.attrib['id']]['text'] = ''


    return

def Parse_XML(xml_file):
    # Initalize storage units, one will store all the context fields, and one will store all GAAP info.

    # Load the HTML file.
    tree = ET.parse(xml_file)
    Process_Contexts(tree)
    # Remove context items so we don't process them again
#    x=0
#    for element in tree.iter():
#        x += 1
#    print("Nodes before delete = ", x)
#    root_node = tree.getroot()
#    xbrl_child = root_node[0]
#    for xbrl_child in xbrl_child.iter():
#        print(xbrl_child.tag)
#        if (xbrl_child.tag.endswith('}xbrl')):
#            xbrli_starting_node = xbrl_child
#        elif (xbrl_child.tag.endswith('}context')):
#            high_level_context = xbrl_child
#    root_node.remove(high_level_context)
#    context_list = xbrl_child.iter(tag='{http://www.xbrl.org/2003/instance}context')
#    for cnx_item in context_list:
#            xbrl_child.remove(cnx_item)

#    root_node.remove(high_level_context)
#    x = 0
#    for element in tree.iter():
#        x += 1
#    print("Nodes after delete = ", x)
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
    # open the file.
    with open(file_name, mode='w', newline='') as sec_file:
        # create the writer.
        writer = csv.writer(sec_file, quoting=csv.QUOTE_ALL)

        # write the header.
        writer.writerow(['Ticker', 'Report_Date', 'Field_Date', 'Table Row', 'Column', 'VALUE'])
        # Need DocumentPeriodEndDate field for the report date in each record
        Report_Date = ''
        for storage_1 in storage_gaap:
            if 'DocumentPeriodEndDate' in storage_gaap[storage_1]['tag']:
                Report_Date = storage_gaap[storage_1]['StringValue']
                break

        # If we do not find the DocumentPeriodEndDate, print error to console
        if Report_Date == '' :
            print("Potential Error : No DocumentPeriodEndDate found in XML submission For ", file_name)
            print("Using todays date for report date")
            Report_Date = datetime.today().strftime('%Y%m%d')
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
                            date_string = datetime.strptime(date_string, "%Y-%m-%d").strftime("%Y%m%d")
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
            # The values in the statements need to be adjusted by the decimals value
            if 'decimals' in storage_gaap[storage_1].keys() :
                if storage_gaap[storage_1]['decimals'] != 'INF':
                    if storage_gaap[storage_1]['decimals'] != 'null' :
                        dec_value = float(storage_gaap[storage_1]['decimals'])
                    else:
                        dec_value = 0
            else:
                dec_value = 0

            if storage_gaap[storage_1]['StringValue'].isnumeric() :
                original_xml_number = float(storage_gaap[storage_1]['StringValue'])
                converted_number = original_xml_number * (10 ** dec_value)
                output_converted_number = str(converted_number)
            else:
                output_converted_number = storage_gaap[storage_1]['StringValue']

            if 'font-' in output_converted_number:
                output_converted_number = 'Suppressed'

            output_line = [ticker, Report_Date, date_string, Row_String, ColumnName, output_converted_number]
            writer.writerow(output_line)

        sec_file.close()
    return

def SecParse_MultipleFiles() :
    report_type = '10-Q'
    # The dateb variable is used as a prefix in the XML file names being opened
    dateb = '20210331'
    os.chdir("C:/Users/jeffj/FITSolutionsProjects/FreeLancer/Edgar Data Parsing")
    base_directory = pathlib.Path.cwd()
    print("Working in Directory : ", base_directory)

    for item in Ticker_CIK_List.items():
        ticker = list(item)[0]
        cik = list(list(item)[1])[0]
        url = list(list(item)[1])[1]
        sec_base_file_name = 'newfilename.xml'
        filenamestring = dateb + '_' + ticker + '_' + str(cik) + '_' + report_type + '.xml'
        working_file_name = filenamestring.replace('newfilename', filenamestring)
        file_htm = base_directory.joinpath("data").joinpath(working_file_name).resolve()
        print("Processing File : ", file_htm)
        global storage_gaap
        global context_dictionary
        storage_gaap = {}
        context_dictionary = {}
        Tag_Ig_Count = 0
        # Check response code
        if (Parse_XML(file_htm)):
            print(f"Successful extraction for {ticker} {cik}, Ignored {Tag_Ig_Count}")
            Tag_Ignore_Count = 0
            working_file_name = filenamestring.replace('xml', 'csv')
            Write_CSV(working_file_name, ticker)
            print("Results Are Written To ..\\", working_file_name)
        else:
            print(f"Error Retrieving Data For {ticker} {cik}")
    return

# Check to see if this file is being executed as the "Main" python
# script instead of being used as a module by some other python script
# This allows us to use the module which ever way we want.
if __name__ == '__main__':
    SecParse_MultipleFiles()

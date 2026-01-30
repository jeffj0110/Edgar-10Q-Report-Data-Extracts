# SEC Edgar XBRL XML Data Parser
# July 2021 : Jeff Jones
#
# The script will pull in files from a base directory which are organized by Ticker
#  Base Dir -> Symbol > *.xml files
# The XBRL Instance XML and XBRL Presentation XML files are used.
#
import sys, getopt, os
import glob
from datetime import datetime
import pandas as pd
import csv
import pprint
import pathlib
import collections
import xml.etree.ElementTree as ET

Tag_Ig_Count = int(0)

ExclusionTags = [
                 '}xbrl',
                 'html',
                 '}loc',
                 '}footnote',
#                 'footnoteArc',
#                 'footnoteLink',
                 '}schemaRef',
                 '}context',
                 '}unit',
                 '}entity',
#                 'CIK',
                 '}identifier',
                '}period',
                '}startDate',
                '}endDate',
#                'scheme',
                '}segment',
                 '}instant',
                 '}measure',
#                 'unitDenominator',
#                 'unitNumerator',
                 '}divide',
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

#This function will start at an index and look to the left for a given character
def reverseFind(startIndex, inputString, inputChar1, inputChar2) :
    IndexCnter = -1
    InputLen = len(inputString)
    if InputLen == 0 :
        return -1
    if (startIndex > InputLen) or (startIndex < 0):
        return -1
    if inputChar1 == '' or inputChar2 == '':
        return -1
    for i in range(startIndex - 1, -1, -1) :
        if inputString[i] == inputChar1 or inputString[i] == inputChar2 :
            IndexCnter = i
            break
    return IndexCnter

global ID_Counter
def CreateID() :
    global ID_Counter
    ID_Counter = ID_Counter + 1
    return str(ID_Counter)

# Read in Presentation XML file for a report and parse the results
# into a Pandas dataframe by organizing the relationships between the
# fields and the various locations it is used in the report.
# The Presentation XML file needs to follow the same naming conventions as the Instance
# file name except that it has a _pre on the end of the file pre-extension
# Updated July of 2021 : Added special dictionary for handling table entries
#
def Pres_XML_Parse(file_name) :

    # Initalize storage units
    storage_list = []
    # Labels come in two forms, those I want and those I don't want.
    avoids = ['linkbase']
    # parse = ['roleRef','label','labelLink','labelArc','loc','definitionLink','definitionArc','calculationArc', 'presentationLink', 'order']
    parse = ['roleRef', 'loc', 'presentationLink', 'presentationArc']
    # parse = ['roleRef','presentationArc']
    ArcList_DF = pd.DataFrame()
    # part of the process is matching up keys, to do that we will store some keys as we parse them.
    list_count = 0
    # loop through each file.

    Preso_file_name = file_name.replace(".csv", "_pre.xml")
    working_directory = pathlib.Path.cwd()
    file_pre = working_directory.joinpath(Preso_file_name).resolve()
    if os.path.exists(file_pre) == False :
        print(f"Presentation XML File Not Found {Preso_file_name}")
        return ArcList_DF
    tree = ET.parse(file_pre)
    # This will find all the 'presentationLink' objects in the file
    elements = tree.findall('{http://www.xbrl.org/2003/linkbase}presentationLink')
    for element in elements:
        # if the element has children we need to loop through those.
        elem_keys = element.keys()
        for element_key in elem_keys:
#            if 'Table' in element_key :
#                print (element_key)
            if '}' in element_key:
                key_name = element_key.split('}')[1]
                if key_name == 'role':
                    Presentation_Section = element.attrib[element_key].rsplit('/', 1)[1]

        for child_element in element.iter():
            # split the label to remove the namespace component, this will return a list.
            element_split_label = child_element.tag.split('}')
            # The first element is the namespace, and the second element is a label.
            # namespace = element_split_label[0]
            label = element_split_label[1]
            #            print(namespace, "-", label)
            # if it's a PresentationArc, we want to capture the
            # parent and field name to understand where the data point
            # is being presented in the report.
            if label == 'presentationArc':
                # define the item type label
                element_type_label = label
                # initalize a smaller dictionary that will house all the content from that element.
                dict_storage = {}
                #                dict_storage['item_type'] = element_type_label
                # grab the attribute keys
                cal_keys = child_element.keys()
                # for each key.
                for key in cal_keys:
                    if '}' in key:
                        # add the new key to the dictionary and grab the old value.
                        new_key = key.split('}')[1]
                        if new_key == 'from':
                            if "_" in child_element.attrib[key] :
                                subsection = child_element.attrib[key].rsplit('_', 2)[1]
                            else :
                                subsection = child_element.attrib[key]
                            dict_storage['Subsection'] = subsection
                        if new_key == 'to':
                            if "_" in child_element.attrib[key]:
                                dei_field_name = child_element.attrib[key].rsplit('_', 2)[1]
                            else :
                                dei_field_name = child_element.attrib[key]
                            dict_storage[new_key] = dei_field_name
                dict_storage['Pres_Sect'] = Presentation_Section
                storage_list.append(['presentationArc', dict_storage])
                list_count += 1
# Store the extracted Presentation Arcs in a dataframe
    Arc_List = []
    for dict_cont in storage_list:
        field_value = ''
        Sect_value = ''
        SubSection_Value = ''
        list_of_items = dict_cont[1].items()
        for item in list_of_items:
            if item[0] == 'to':
                field_value = item[1]
            if item[0] == 'Pres_Sect':
                Sect_value = item[1]
            if item[0] == 'Subsection':
                SubSection_Value = item[1]

        new_row = ['presentation',
                       Sect_value,
                       SubSection_Value,
                       field_value]
        Arc_List.append(new_row)

    num_arcs = len(Arc_List)
#    print("Size of arcs ", num_arcs)
    if num_arcs < 1 :
        print(f"No Presentation Link Entries for  {Preso_file_name}")
        return ArcList_DF
    else:
        ArcList_DF = pd.DataFrame(Arc_List, columns=['FILE', 'Section_Value', 'Sub-Section_VALUE', 'Field_Name'])
        return ArcList_DF

def Find_Table_Name(ArcList_DF, input_Row_String, input_dimension_string, Section, Sub_Section, Field_Name):
    clean_Row_String = input_Row_String
    clean_dimension_string = input_dimension_string
    if (Section in clean_Row_String ) or (Sub_Section in clean_Row_String) :
        target_Section_Value = Section
        target_Sub_Section_Value = Sub_Section
        return target_Section_Value, target_Sub_Section_Value, True
    if (Section in clean_dimension_string) or (Sub_Section in clean_dimension_string) :
        target_Section_Value = Section
        target_Sub_Section_Value = Sub_Section
        return target_Section_Value, target_Sub_Section_Value, True
    Search_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == Sub_Section]
    search_results = len(Search_df)
    while search_results > 0 :
        for i in range(search_results) :
            working_Section_Value = Search_df.iloc[i, 1]
            working_subSection_Value = Search_df.iloc[i, 2]
            if ('table' in working_Section_Value.lower()) or ('table' in working_subSection_Value.lower()):
                target_Section_Value = working_Section_Value
                target_Sub_Section_Value = working_subSection_Value
                return target_Section_Value, target_Sub_Section_Value, True
        Search_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == working_subSection_Value]
        search_results = len(Search_df)
    return '', '', False

def Find_Linked_Entries(ArcList_DF, input_Row_String, input_dimension_string, Section, Sub_Section, Field_Name) :
    # If strings start with us-gaap:, we need to truncate us-gaap:
#    if ':' in input_Row_String :
#        clean_Row_String = input_Row_String.rsplit(':',1)[1]
#    else:
    clean_Row_String = input_Row_String
#    if ':' in input_dimension_string :
#        clean_dimension_string = input_dimension_string.rsplit(':',1)[1]
#    else :
    clean_dimension_string = input_dimension_string

    if (Section in clean_Row_String ) or (Sub_Section in clean_Row_String) :
        target_Section_Value = Section
        target_Sub_Section_Value = Sub_Section
        return target_Section_Value, target_Sub_Section_Value, True
    if (Section in clean_dimension_string) or (Sub_Section in clean_dimension_string) :
        target_Section_Value = Section
        target_Sub_Section_Value = Sub_Section
        return target_Section_Value, target_Sub_Section_Value, True
    # if length of this search > 0, then we look to see if
    # the clean dimension or  clean row string are in the resulting row values
    # if we find them, we stop and return.  If we don't find them, we keep going until
    # the search returns no results
    Search_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == Sub_Section]
    search_results = len(Search_df)
    while search_results > 0 :
        for i in range(search_results) :
            working_Section_Value = Search_df.iloc[i, 1]
            working_subSection_Value = Search_df.iloc[i, 2]
            if (working_Section_Value in clean_Row_String) or (working_subSection_Value in clean_Row_String) :
                target_Section_Value = working_Section_Value
                target_Sub_Section_Value = working_subSection_Value
                return target_Section_Value, target_Sub_Section_Value, True
            if (working_Section_Value in clean_dimension_string) or (working_subSection_Value in clean_dimension_string) :
                target_Section_Value = working_Section_Value
                target_Sub_Section_Value = working_subSection_Value
                return target_Section_Value, target_Sub_Section_Value, True
        Search_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == working_subSection_Value]
        search_results = len(Search_df)
    return '', '', False

def Search_ArcList_DF(ArcList_DF, input_ColumnName, input_Row_String, input_dimension_string) :
    return_Section_Value = ''
    return_Sub_Section_Value = ''
    Table_Search = False
    lower_case_rowstr = input_Row_String.lower()
    if 'member' in lower_case_rowstr :
        if lower_case_rowstr.endswith('member'):  # special processing for tables, extract the member name and look it up
            if ':' in input_Row_String:
                Table_Member_Name = input_Row_String.rsplit(':', 1)[1]
            else:
                Table_Member_Name = input_Row_String
            filtered_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == Table_Member_Name]
            Table_Search = True
        else:          # Find the member name near the end of the string if it doesn't end with 'member'
            if lower_case_rowstr.find('member') != -1 :
                indexref = lower_case_rowstr.rfind('member')  #extract the name by taking the string before the ':'
                if reverseFind(indexref, lower_case_rowstr, ':', ' ') != -1 :
                    rindexref = reverseFind(indexref, lower_case_rowstr, ':', ' ')
                    Table_Member_Name = input_Row_String[rindexref+1:(indexref+6)]
                else :
                    Table_Member_Name = input_Row_String
                filtered_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == Table_Member_Name]
                Table_Search = True
            else:
                Table_Search = False
    else :
        filtered_df = ArcList_DF.loc[ArcList_DF['Field_Name'] == input_ColumnName]

    len_filtereddf = len(filtered_df)
    if len_filtereddf == 0 :
        return return_Section_Value, return_Sub_Section_Value
    if (len_filtereddf == 1) and (Table_Search == False):
        return_Section_Value = filtered_df.iloc[0, 1]
        return_Sub_Section_Value = filtered_df.iloc[0, 2]
    else :
        # We search for presentation arcs that match input_dimension_string, unless we are searching for a table
        if ((len(input_Row_String) == 0) and (len(input_dimension_string) == 0)) or (input_Row_String.isspace() and input_dimension_string.isspace()) :
            return_Section_Value = filtered_df.iloc[0, 1]
            return_Sub_Section_Value = filtered_df.iloc[0, 2]
        else:
            if Table_Search :
                for i in range(len_filtereddf) :
                    return_Section_Value, return_Sub_Section_Value, Success_Found = Find_Table_Name(ArcList_DF,
                                                                                                    input_Row_String,
                                                                                                    input_dimension_string,
                                                                                                    filtered_df.iloc[i, 1],
                                                                                                    filtered_df.iloc[0, 2],
                                                                                                    Table_Member_Name)
                    if Success_Found :
                        return return_Section_Value, return_Sub_Section_Value
                #if we don't find a table, then we do another table search using the input_ColumnName
                return_Section_Value, return_Sub_Section_Value, Success_Found = Find_Table_Name(ArcList_DF,
                                                                                                    input_Row_String,
                                                                                                    input_dimension_string,
                                                                                                    filtered_df.iloc[i, 1],
                                                                                                    filtered_df.iloc[0, 2],
                                                                                                    input_ColumnName)
            else:
                for i in range(len_filtereddf) :
                    return_Section_Value, return_Sub_Section_Value, Success_Found = Find_Linked_Entries(ArcList_DF,
                                                                                                        input_Row_String,
                                                                                                        input_dimension_string,
                                                                                                        filtered_df.iloc[i, 1],
                                                                                                        filtered_df.iloc[0, 2],
                                                                                                        input_ColumnName)
                    if Success_Found :
                        return return_Section_Value, return_Sub_Section_Value

    return return_Section_Value, return_Sub_Section_Value

#This function will convert a string formatted with any of the following
#Patterns to a float variable of years (whole years plus fractional)
#The following patterns are supported
# P##Y##M##D  : Converts to Years+Month/12+Days/360
# P##Y : Converts to Years
# P##M : Converts to zero years plus Months/12
# P##D: Converts to zero years, zero months plus Days/360
# P##Y##M     ...
# P##M##D     ...
# P##Y##D     ...
def Convert_Period_To_Years(Input_String) :
    year_total = float(0.0)
# Verify valid incoming format
    if len(Input_String) == 0 :
        return 0.0
    if Input_String.startswith('P') :
        Years_Numbers = ''
        Months_Numbers = ''
        Days_Numbers = ''
        if 'Y' in Input_String :
            Y_Position = Input_String.find('Y') - 1
            for i in range(Y_Position, 0, -1) :
                if Input_String[i].isdigit() :
                    Years_Numbers = Input_String[i] + Years_Numbers
                else :
                    break    # Need to break out of for loop if we hit a non-numeric
        if 'M' in Input_String :
            M_Position = Input_String.find('M') -1
            for i in range(M_Position, 0, -1):
                if Input_String[i].isdigit():
                    Months_Numbers = Input_String[i] + Months_Numbers
                else:
                    break  # Need to break out of for loop if we hit a non-numeric
        if 'D' in Input_String :
            D_Position = Input_String.find('D') - 1
            for i in range(D_Position, 0, -1):
                if Input_String[i].isdigit():
                    Days_Numbers = Input_String[i] + Days_Numbers
                else:
                    break  # Need to break out of for loop if we hit a non-numeric
    else :
        return 0.0

    if Years_Numbers == '' :
        Year_Float = 0.0
    else :
        Year_Float = float(Years_Numbers)

    if (Months_Numbers == '') :
        Month_Float = 0.0
    else :
        Month_Float = float(Months_Numbers)

    if Days_Numbers == '' :
        Days_Float = 0.0
    else :
        Days_Float = float(Days_Numbers)

    year_total = Year_Float + (Month_Float/12) + (Days_Float/360)
    return year_total

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
            if cnx_child.tag.endswith('}period') :
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
                    if att != 'dimension' :
                        context_dictionary[cnx_item.attrib['id']][att] = context_child.attrib[att]
                    if not context_child.text is None:
                        if context_child.text.strip() != '':
                            if att == 'dimension':
                                if 'dimension' in context_dictionary[cnx_item.attrib['id']].keys():
                                    context_dictionary[cnx_item.attrib['id']]['dimension'] = context_child.attrib[att] + " / " + context_dictionary[cnx_item.attrib['id']]['dimension']

                                else :
                                    context_dictionary[cnx_item.attrib['id']]['dimension'] = context_child.attrib[att]
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
                    storage_gaap[element.attrib['id']]['contextRef'] = element.attrib.get('contextRef', 'null')
                    storage_gaap[element.attrib['id']]['unitRef'] = element.attrib.get('unitRef', 'null')
                    storage_gaap[element.attrib['id']]['decimals'] = element.attrib.get('decimals', 'null')
                    if not element.text is None:
                        storage_gaap[element.attrib['id']]['StringValue'] = element.text
                    else:
                        storage_gaap[element.attrib['id']]['StringValue'] = ''
# There are a submissions where there is no ID attribute for a given fact data point.  This is
# a result of it not being a  XBRL submission
# These have to be ignored because of the lack of structure with these submissions.
                else :
#                    #Need to construct a unique id where companies did not insert an ID
                    id_string = CreateID()
                    storage_gaap[id_string] = {}
                    storage_gaap[id_string]['tag'] = element.tag
                    storage_gaap[id_string]['decimals'] = element.attrib.get('decimals', 'null')
                    storage_gaap[id_string]['contextRef'] = element.attrib.get('contextRef', 'null')
                    storage_gaap[id_string]['unitRef'] = element.attrib.get('unitRef', 'null')
                    storage_gaap[id_string]['decimals'] = element.attrib.get('decimals', 'null')
                    if not element.text is None:
                        storage_gaap[id_string]['StringValue'] = element.text
                    else:
                        storage_gaap[id_string]['StringValue'] = ''

    print(f"Number of Context Entries : {len(context_dictionary)}")
    print(f"Number of Table Values : {len(storage_gaap)}")
    return True

def Write_CSV(file_name, ticker) :
    # open the file in the current working directory
    with open(file_name, mode='w', newline='', encoding='utf-8') as sec_file:
        # create the writer.
        writer = csv.writer(sec_file, quoting=csv.QUOTE_ALL)

        # write the header.
        writer.writerow(['Ticker', 'Report_Date', 'Field_Date', 'Section', 'Sub_Section', 'Dimension', 'Member', 'FactDesc', 'Fact', 'Value_Rounding'])
        # Need DocumentPeriodEndDate field for the report date in each record
        # Open Presentation XML and create Presentation XML Dataframe objective
        Arc_DF = Pres_XML_Parse(file_name)
        num_arcs = len(Arc_DF)
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
            dimension_string = ''
            if 'contextRef' in storage_gaap[storage_1].keys() :
                id = storage_gaap[storage_1]['contextRef']
                if id != 'null' :
                    length = len(id)
                    if 'asofdate' in context_dictionary[id].keys() :
                        date_string = context_dictionary[id]['asofdate']
                        if '-' in date_string :
                            stripped_date_string = date_string.strip('\n')
                            if 'T' in stripped_date_string :
                                JustYYMMDD = stripped_date_string.split("T",1)
                                stripped_date_string = JustYYMMDD[0]
                            date_string = datetime.strptime(stripped_date_string, "%Y-%m-%d").strftime("%Y%m%d")
                        else:
                            date_string = Report_Date
                    else:
                        date_string = Report_Date
                    if 'dimension' in context_dictionary[id].keys() :
                        dimension_string = context_dictionary[id]['dimension']
                    else:
                        dimension_string = ''
                    CombinedText = context_dictionary[id]['text']
                    Row_String = CombinedText
                else:
                    date_string = Report_Date
                    dimension_string = ''
                    Row_String = ""

            RawTag = storage_gaap[storage_1]['tag']
            if '}' in RawTag :
                ColumnName = RawTag[RawTag.rindex('}') + 1:]
            else:
                ColumnName = RawTag
# The values should not be adjusted by the 'decimals' value.  This is just a number to indicate this value was rounded.
            if 'decimals' in storage_gaap[storage_1].keys() :
                dec_value = storage_gaap[storage_1]['decimals']
            else:
                dec_value = ""

            if storage_gaap[storage_1]['StringValue'].isnumeric() :
                original_xml_number = float(storage_gaap[storage_1]['StringValue'])
                converted_number = float(storage_gaap[storage_1]['StringValue'])
                output_converted_number = str(converted_number)
            elif storage_gaap[storage_1]['StringValue'].startswith('P') :
                #This might be a period type field, where we need to convert to a years number
                string_variable = storage_gaap[storage_1]['StringValue']
                if len(string_variable) > 1 :
                    if string_variable[1].isdigit() and ('Y' in string_variable or 'M' in string_variable or 'D' in string_variable) :
                        converted_number = Convert_Period_To_Years(string_variable)
                        output_converted_number= str(converted_number)
                    else :
                        output_converted_number = storage_gaap[storage_1]['StringValue']
                else :
                    output_converted_number = storage_gaap[storage_1]['StringValue']
            else:
                output_converted_number = storage_gaap[storage_1]['StringValue']

            if 'font-' in output_converted_number or 'FONT-' in output_converted_number or 'font:' in output_converted_number or 'Font:' in output_converted_number:
                output_converted_number = 'Suppressed'  # Textblock's have font definitions which cause havoc with the CVS file format
            if num_arcs > 1 :
                Section_Value, Sub_Section_Value = Search_ArcList_DF(Arc_DF, ColumnName, Row_String, dimension_string)
            else :
                Section_Value = ''
                Sub_Section_Value = ''
            output_line = [ticker, Report_Date, date_string, Section_Value, Sub_Section_Value, dimension_string, Row_String, ColumnName, output_converted_number, dec_value]
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
    # Read in input CSV file with all the tickers
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
        file_list = glob.glob('*.xml', )
        for i in range(len(file_list) - 1, -1, -1) :
            if '_pre.xml' in file_list[i].lower() :
                del file_list[i]
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
    for row in range(num_tickers):
        os.chdir(CurrentDirectory)
        os.chdir("OutputXML")
        row_data = Ticker_CIK.iloc[row]
        tck = row_data['Ticker']
        cik = row_data['CIK']
        os.chdir(tck)
        extension = 'csv'
        all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
        # Do not concatenate to the existing Combined*.csv file, just overwrite it.
        for i in range(len(all_filenames) - 1, -1, -1) :
            if '_combined.csv' in all_filenames[i].lower() :
                del all_filenames[i]
        # combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f, low_memory=False) for f in all_filenames])
        # export to csv
        Combined_File_Name = tck + '_Combined.csv'
        combined_csv.to_csv(Combined_File_Name, index=False, encoding='utf-8-sig')
        print("Combined CSV Files For ", tck)
    return


def main(argv):
   global ID_Counter
   inputfile = 'Ticker_CIK_List.csv'
   ID_Counter = 1
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

# Check to see if this file is being executed as the "Main" python
# script instead of being used as a module by some other python script
# This allows us to use the module which ever way we want.
if __name__ == "__main__":
   main(sys.argv[1:])
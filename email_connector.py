import re
import csv
import os
import sys

CITI_expr = "(\w+\s+\d+[-/\\,]\w+\s+\w+)\s+(\d+\s*\/\s*\d*)\s+(\w+[+-/*]?)"
DB_expr = "(\w+\s+\d+[-/\\,]\w+\s+\w+)\s+(\d+)\s*\/\s*(\d*)\s+(\d*\.?\d*)\s?[Xx/]\s?(\d*\.?\d*)"
JPM_expr = "(\w+\s+\d+[-/\\,]\w+\s+\w+)\s+(?:\d+\.\d+|NA)\s+(\d+\/\d*)\s+(\d\s?x\s?\w*)"
#ML_expr = "(\w+\s+\d+[-/\\,]\w+\s+\w+)\s+(?:\s+(\d+)(?:\s+(\d+)(?:\s+(\d+)(?:\s+(\d+))?)?)?)?"

index = 0
"""
def scrape_stuff(file_name):
    with open(file_name) as _input_:
        data = _input_.read()
    result = re.findall(JPM_expr, data)
    return result

def print_stuff(some_array):
    global index
    for each_array in some_array:
        index = index+1
        print "ID #",index,"Bond name",each_array
"""
def scrape_stuff_from_GS(file_name):

    total_results = []
    STACR_STR = []
    CAS_STR = []
    STACR_END = []
    CAS_END = []
    GS_bonds = []
    #input_stream = []

    with open(file_name) as _input_:
        input_stream = _input_.read()
        # for each_line in input_stream:
    if '                Cpn     Bid/Off  Size                    Cpn    Bid/Off  Size' in input_stream:
    #if 'HOORAY' in input_stream:
        GS_expr_with_bond_header = "(\w+\s+\d+[-/\\,]\w+\s+\w+)\s+[A-Za-z]?[+-/*]?\d+?\s+(\d+)\s*\/\s*(\d*)\s+(\d+)\s*[xX]\s*(\d*)"
        #for d in input_stream:
        GS_bonds = re.findall(GS_expr_with_bond_header, input_stream)
        #GS_bonds.append(result)

        GS_file_stream =  open('GS-CRT-MARKETS.csv', 'a')
        try:

            writer = csv.writer(GS_file_stream,delimiter=',', lineterminator='\n')
            writer.writerow( ('DATE','TIME','DEALER','SECURITY','SIZE BID','SIZE ASK','MKT BID','MKT ASK' ))
            for each_bond in GS_bonds:
                writer.writerow(('7/13/2015','8:30:58','GS',
                                  each_bond[0].strip(),each_bond[3].strip(),each_bond[4].strip(),each_bond[1].strip(),each_bond[2].strip()))
        finally:
            GS_file_stream.close()

    elif "STACR       Cpn    Bid  Offer  Size      CAS          Cpn   Bid  Offer Size" in input_stream:
        print "Bond"
        GS_expr_without_bond_header = '(\w+\s\d+[-/\\,]\w+\s+\w+)\s+[A-Za-z]?[+-/*]?\d+?\s+(\d+)\s*\/\s*(\d*)\s{3}(\d+)\s*[xX]\s*(\d*)'
        for d in input_stream.split('\n'):
            if len(re.findall('\d\sx\s?\d?\d?',d.strip())) > 0:
                temp = re.split('\d\sx\s?\d?\d?',d.strip())
                if d[:10] != "          " :
                    TEMP_STACR_STR = temp[0].strip()
                    TEMP_CAS_STR = temp[1].strip()
                    STACR_STR.append(TEMP_STACR_STR),CAS_STR.append(TEMP_CAS_STR)
                else:
                    CAS_STR.append(temp[0].strip())
            else:
                pass

            if len(re.findall('\d+\-\w+\s+\w+\s+[A-Za-z]\+\d+\s+\d+[\/\\\\]\s\d*\s+',d.strip())) > 0:
                temp = re.split('\d+\-\w+\s+\w+\s+[A-Za-z]\+\d+\s+\d+[\/\\\\]\s\d*\s+',d)
                if len(temp) == 3:
                    TEMP_STACR_END = temp[1].strip()
                    TEMP_CAS_END = temp[2].strip()
                    STACR_END.append(TEMP_STACR_END), CAS_END.append(TEMP_CAS_END)
                elif len(temp) == 2:
                    if temp[0] == "":
                        TEMP_STACR_END = temp[1].strip()
                        STACR_END.append(TEMP_STACR_END)
                    else:
                        TEMP_CAS_END = temp[1].strip()
                        CAS_END.append(TEMP_CAS_END)
            else:
                pass

        STACR = map('   '.join,zip(STACR_STR,STACR_END))
        STACR = ['STACR {}'.format(element[2:]) for element in STACR]
        #print STACR,"\n"

        CAS_STR = filter(None, CAS_STR)
        CAS_END = filter(None, CAS_END)
        CAS = map('   '.join,zip(CAS_STR,CAS_END))
        CAS = ['CAS {}'.format(element[2:]) for element in CAS]
        #print CAS, "\n"

        for S in STACR:
            GS_bonds += re.findall(GS_expr_without_bond_header,S)
        for C in CAS:
            GS_bonds += re.findall(GS_expr_without_bond_header,C)

    # total_results = re.findall('(\d+\-\w+\s+\w+)\s+[A-Za-z]\+\d+\s+(\d+)[\/\\\\]\s(\d*)\s+(\d\d?)\sx\s?(\d?\d?)',)
    #print len(STACR),len(CAS)
        GS_file_stream =  open('GS-CRT-MARKETS.csv', 'a')
        try:

            writer = csv.writer(GS_file_stream,delimiter=',', lineterminator='\n')
            writer.writerow( ('DATE','TIME','DEALER','SECURITY','SIZE BID','SIZE ASK','MKT BID','MKT ASK' ))
            for each_bond in GS_bonds:
                writer.writerow(('7/13/2015','8:30:58','GS',
                                  each_bond[0].strip(),each_bond[3].strip(),each_bond[4].strip(),each_bond[1].strip(),each_bond[2].strip()))
        finally:
            GS_file_stream.close()
    else:
        pass

    print"DONE"

scrape_shit_from_GS("GS-markets.txt")

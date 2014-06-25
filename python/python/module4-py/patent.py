#!/usr/bin/env python

# the mapper is to parse a patent database fiel of the followong format and 
# map year+country combination to count patents submitted by a country in one yeat

#"PATENT","GYEAR","GDATE","APPYEAR","COUNTRY","POSTATE","ASSIGNEE","ASSCODE","CLAIMS","NCLASS","CAT","SUBCAT","CMADE","CRECEIVE","RATIOCIT","GENERAL","ORIGINAL","FWDAPLAG","BCKGTLAG","SELFCTUB","SELFCTLB","SECDUPBD","SECDLWBD"
#3070801,1963,1096,,"BE","",,1,,269,6,69,,1,,0,,,,,,,
#3070802,1963,1096,,"US","TX",,1,,2,6,63,,0,,,,,,,,,
#3070803,1963,1096,,"US","IL",,1,,2,6,63,,9,,0.3704,,,,,,,
#3070804,1963,1096,,"US","OH",,1,,2,6,63,,3,,0.6667,,,,,,,
#3070805,1963,1096,,"US","CA",,1,,2,6,63,,1,,0,,,,,,,

import sys

# input comes from STDIN (standard input)
current_key = None
row_no=0
num=0
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    if row_no>0 : # skip header row
       cols = line.split(",")
       year = cols[1]
       country = cols[4].translate(None,'"')
       if ( current_key != None ):
          if ( current_key != year+'.'+country ):
             print '%s\t%s' % (current_key, num)
             num=0
       num=num+1
       current_key=year+'.'+country
    row_no+=1
# spit out whatever was collected but never reported
print '%s\t%s' % (current_key, num)
num=0


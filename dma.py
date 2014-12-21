__author__ = 'karenwu'

import subprocess
import string
import re
from datetime import date,datetime,timedelta
import time
import csv
from subprocess import check_output
import pandas as pd
import numpy as np

def perdelta(start,end,delta):
    curr=start
    while curr < end:
        yield curr
        curr += delta

####non-norm date###
for date_date in perdelta(date(2010,1,1),date(2010,1,2),timedelta(days=1)):
    date_brand= datetime.strptime(str(date_date),'%Y-%m-%d').strftime('%m/%d/%Y')
    print date_brand

    while True:
        query=' \'query:"agg:dma xvalues:all_in_country_usa date_range:('+date_brand+' 1d) compare:((entity:/m/0hr65bq) (entity:/m/01ncmt) (entity:/m/0hr65bq thermostat) (honeywell thermostat) ((entity:/m/01y3hg)|(entity:/m/01v1v alarm)|(smoke alarm)|(entity:/m/01v1v detect)) (entity:/m/0hr65bq entity:/m/01y3hg|entity:/m/08dckq|protect) (entity:/m/03gsxjt (alarm|detect|smoke)) (entity:/m/0c480v (alarm|detect|smoke) )(entity:/m/04z37g|(secure camera)) (entity:/m/0hr65bq dropcam) (dropcam) (entity:/m/0zmzvzg) (entity:/m/04q5hl)) privacy_threshold:0 survey_max_users:50000"\' '
        print "query---" + query
        proc=subprocess.Popen('stubby call blade:izeitgeist-api IZeitgeist.Query' +query+'--proto2',shell=True,stdout=subprocess.PIPE)
        output=str(proc.stdout.read())
        if output != "":
            print "==== BREAK!!"
            break
        print "==== check done"
        time.sleep(20)
    def find_between(string,start,end):
        try:
            start_idx = string.index(start)+len(start)
            end_idx = string.index(end)
            return string[start_idx:end_idx]
        except ValueError:
            print "Exception: find_between has ValueError()"
            return " "
    start = 'table {'
    end = 'column_user_data {'
    need_string = find_between(output,start,end)
    list_string = re.findall(r'\d*\.\d+|\d+|\d\d\d', need_string)
    print "start here ------"
    print list_string

    new_string = []
    for value in list_string:
        if len(value) == 3:
            new_string.append(date_brand)
            new_string.append(value)
        else:
            new_string.append(value)
    print new_string

    i = 1
    csv_list=[]
    csv_sub_list = None
    for i in range(len(new_string)):
        if ('/') in new_string[i]:
            if csv_sub_list is not None:
                csv_list.append(csv_sub_list)
            csv_sub_list = []
        csv_sub_list.append(new_string[i])
    csv_list.append(csv_sub_list)
    print csv_list

    with open (''+str(date_date)+'_dma.csv','w') as fp:
    #with open ('test_dma.csv','w') as fp:
    #with open('brand_dma_new_new.csv','a') as fp:
        a = csv.reader(fp)
        a = csv.writer(fp, delimiter=',')
        a.writerows(csv_list)

    time.sleep(20)

### non_norm_us ###
for date_date in perdelta(date(2010,1,1),date(2011,1,1),timedelta(days=1)):
    date_brand= datetime.strptime(str(date_date),'%Y-%m-%d').strftime('%m/%d/%Y')
    print date_brand

    while True:
        query=' \'query:"agg:dma xvalues:all_in_country_usa date_range:('+date_brand+' 1d) normalize:(country:us) compare:((entity:/m/0hr65bq) (entity:/m/01ncmt) (entity:/m/0hr65bq thermostat) (entity:/m/01gx66 thermostat) ((entity:/m/01y3hg)|(entity:/m/01v1v alarm)|(smoke alarm)|(entity:/m/01v1v detect)) (entity:/m/0hr65bq entity:/m/01y3hg|entity:/m/08dckq|protect) (entity:/m/03gsxjt (alarm|detect|smoke)) (entity:/m/0c480v (alarm|detect|smoke))(entity:/m/04z37g|(secure camera)) (entity:/m/0hr65bq dropcam) (dropcam) (entity:/m/0zmzvzg) (entity:/m/04q5hl)) privacy_threshold:0 survey_max_users:50000"\' '
        print "query---" + query
        proc=subprocess.Popen('stubby call blade:izeitgeist-api IZeitgeist.Query' +query+'--proto2',shell=True,stdout=subprocess.PIPE)
        output=str(proc.stdout.read())
        if output != "":
            print "==== BREAK!!"
            break
        print "==== check done"
        time.sleep(20)
    def find_between(string,start,end):
        try:
            start_idx = string.index(start)+len(start)
            end_idx = string.index(end)
            return string[start_idx:end_idx]
        except ValueError:
            print "Exception: find_between has ValueError()"
            return " "
    start = 'table {'
    end = 'column_user_data {'
    need_string = find_between(output,start,end)
    list_string = re.findall(r'\d*\.\d+|\d+|\d\d\d', need_string)
    print "start here ------"
    print list_string

    new_string = []
    for value in list_string:
        if len(value) == 3:
            new_string.append(date_brand)
            new_string.append(value)
        else:
            new_string.append(value)
    print new_string

    i = 1
    # csv_list = [['Date', 'State', 'Nest', 'Nest_Error', 'Nest_Norm', 'Nest_Error_Norm']]
    csv_list=[]
    csv_sub_list = None
    for i in range(len(new_string)):
        if ('/') in new_string[i]:
            if csv_sub_list is not None:
                csv_list.append(csv_sub_list)
            csv_sub_list = []
        csv_sub_list.append(new_string[i])
    csv_list.append(csv_sub_list)
    print csv_list

    #with open (''+str(date_date)+'_dma_us.csv','w') as fp:
    with open('brand_dma_us.csv','a') as fp:
        a = csv.reader(fp)
        a = csv.writer(fp, delimiter=',')
        a.writerows(csv_list)

    time.sleep(20)

#### norm data ###
for date_date in perdelta(date(2010,1,1),date(2010,1,2),timedelta(days=1)):
    date_brand= datetime.strptime(str(date_date),'%Y-%m-%d').strftime('%m/%d/%Y')
    print date_brand

    while True:
        query=' \'query:"agg:dma xvalues:all_in_country_usa date_range:('+date_brand+' 1d) normalize:(country:us) compare:((entity:/m/0hr65bq) (entity:/m/01ncmt) (entity:/m/0hr65bq thermostat) (entity:/m/01gx66 thermostat) ((entity:/m/01y3hg)|(entity:/m/01v1v alarm)|(smoke alarm)|(entity:/m/01v1v detect)) (entity:/m/0hr65bq entity:/m/01y3hg|entity:/m/08dckq|protect) (entity:/m/03gsxjt (alarm|detect|smoke)) (entity:/m/0c480v (alarm|detect|smoke))(entity:/m/04z37g|(secure camera)) (entity:/m/0hr65bq dropcam) (dropcam) (entity:/m/0zmzvzg) (entity:/m/04q5hl)) privacy_threshold:0 survey_max_users:50000 norm_by_agg_var:1"\' '
        print "query---" + query
        proc=subprocess.Popen('stubby call blade:izeitgeist-api IZeitgeist.Query' +query+'--proto2',shell=True,stdout=subprocess.PIPE)
        output=str(proc.stdout.read())
        if output != "":
            print "==== BREAK!!"
            break
        print "==== check done"
        time.sleep(20)
    def find_between(string,start,end):
        try:
            start_idx = string.index(start)+len(start)
            end_idx = string.index(end)
            return string[start_idx:end_idx]
        except ValueError:
            print "Exception: find_between has ValueError()"
            return " "
    start = 'table {'
    end = 'column_user_data {'
    need_string = find_between(output,start,end)
    list_string = re.findall(r'\d*\.\d+|\d+|\d\d\d', need_string)
    print "start here ------"
    print list_string

    new_string = []
    for value in list_string:
        if len(value) == 3:
            new_string.append(date_brand)
            new_string.append(value)
        else:
            new_string.append(value)
    print new_string

    i = 1
    # csv_list = [['Date', 'State', 'Nest', 'Nest_Error', 'Nest_Norm', 'Nest_Error_Norm']]
    csv_list=[]
    csv_sub_list = None
    for i in range(len(new_string)):
        if ('/') in new_string[i]:
            if csv_sub_list is not None:
                csv_list.append(csv_sub_list)
            csv_sub_list = []
        csv_sub_list.append(new_string[i])
    csv_list.append(csv_sub_list)
    print csv_list

    with open (''+str(date_date)+'_dma_norm.csv','w') as fp:
    #with open('brand_dma_norm_new_new.csv','a') as fp:
        a = csv.reader(fp)
        a = csv.writer(fp, delimiter=',')
        a.writerows(csv_list)

    time.sleep(20)

######start_appending_temp_file_into_final_file
open('brand_dma_temp.csv','w').close()
with open('brand_dma.csv','a') as destination,open('brand_dma_temp.csv','r') as source:
    source = csv.reader(source,delimiter=',',quotechar='"')
    for row in source:
        result = csv.writer(destination,delimiter=',')
        result.writerow(row)

open('brand_dma_norm_temp.csv','w').close()
with open('brand_dma_norm.csv','a') as destination,open('brand_dma_norm_temp.csv','r') as source:
    source = csv.reader(source,delimiter=',',quotechar='"')
    for row in source:
        result_norm = csv.writer(destination,delimiter=',')
        result_norm.writerow(row)

########start_adding_DMA#######
with open('AdWords API Cities-DMA Regions.csv', 'rb') as lookup_file:
    next(lookup_file)
    reader = csv.reader(lookup_file,delimiter=',',quotechar='"')
    lu_list = []
    for row in reader:
        lu_list.append(row)

lookupdict = csv.DictReader(open("AdWords API Cities-DMA Regions.csv",'rb'),delimiter=',',quotechar='"')
lu_dict = {}
for look in lu_list:
    print look
    lu_dict[look[4]]= look[3]

_KEY_LOCATION_ = 1

results = []
with open(''+str(date_date)+'_dma.csv', 'rb') as lookup_file,open('merged_'+str(date_date)+'_dma.csv','a') as merged_file:
#with open('brand_dma_new_new.csv', 'rb') as lookup_file,open('merged_brand_dma_new_new.csv','a') as merged_file:
    #next(lookup_file)
    lookup = csv.reader(lookup_file,delimiter=',',quotechar='"')
    for row in lookup:
        res =  lu_dict[row[_KEY_LOCATION_]]
        row.insert(1,res)
        results.append(row)

    merged = csv.writer(merged_file, delimiter=',')
    merged.writerows(results)

results_norm = []
#with open(''+str(date_date)+'_dma_us.csv', 'rb') as lookup_file_norm,open('merged_'+str(date_date)+'_dma_norm.csv','a') as merged_file_norm:
with open('brand_dma_us.csv', 'rb') as lookup_file_norm,open('merged_brand_dma_us.csv','a') as merged_file_norm:
    #next(lookup_file_norm)
    lookup = csv.reader(lookup_file_norm,delimiter=',',quotechar='"')
    for row in lookup:
        res =  lu_dict[row[_KEY_LOCATION_]]
        row.insert(1,res)
        results_norm.append(row)

    merged = csv.writer(merged_file_norm, delimiter=',')
    merged.writerows(results_norm)

results_norm = []
with open(''+str(date_date)+'_dma_norm.csv', 'rb') as lookup_file_norm,open('merged_'+str(date_date)+'_dma_norm.csv','a') as merged_file_norm:
#with open('brand_dma_norm_new_new.csv', 'rb') as lookup_file_norm,open('merged_brand_dma_norm_new_new.csv','a') as merged_file_norm:
    #next(lookup_file_norm)
    lookup = csv.reader(lookup_file_norm,delimiter=',',quotechar='"')
    for row in lookup:
        res =  lu_dict[row[_KEY_LOCATION_]]
        row.insert(1,res)
        results_norm.append(row)

    merged = csv.writer(merged_file_norm, delimiter=',')
    merged.writerows(results_norm)

import urllib2
import re
import sqlite3
import datetime


"""
1.access by type erf and findall resourcetypes
2.open each resourceid pages and findall resourceIDs, we also will need to persist these ids in DB, but might
 be part of record page
3. open each http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=[#]
and then parse html into sqlite3 db - need to model db. am assuming will create a dictionary
for this. will need to keep up what resId are already saved to db and use logic not to add
duplicates
"""
#parts of the ERF urls for global use
baseurl = 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?'
all_res_types = 'cmd=allResTypes'
search_res_types = 'cmd=searchResType&'
detail = 'cmd=detail'

#open each resource 'detail' page in erf & do something!
res_ids = get_resource_ids() 
erf_dict = {}
response = urllib2.urlopen('http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=1795')
html = response.read()
erf_tup = re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html)
sub_list = []
core_list = []
erf_dict = dict(erf_tup)

for i in erf_tup:
     if i[0] == 'Subject:':
          sub_list.append(i[1])
          erf_dict['subject'] = sub_list
     if i[0] == 'Core Subject:':
          core_list.append(i[1])
          erf_dcit['core_subject'] = core_list
     if i[0] == 'Resource type:':
          res_list.append(i[1])
          erf_dict['resource_type'] = res_list
     
        
#for id in res_ids:
if re.search('<B>(Title):</B>\s(.*?)<BR>', html):
    print('match title')
#if match <B>(URL):</B> <A HREF="(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)">
if re.search('<B>(Resource Type):</B>\s(.*?)<BR>', html):
    print('match resource type')
if re.search'<B>(Core subject):</B>\s(.*?)<BR>', html):
    print('match Core subject')
if re.search('<B>(Subject):</B>\s(.*?)<BR>', html):
    print('match subject')
if re.search('<B>(Access):</B>\s(.*?)<BR>', html):
    print('match access')
if re.search('<B>(Text):</B>\s(.*?)<BR>', html): 
    print('match full text')
#<B>(Brief description):</B> (.*?)<BR> 

def get_resource_ids():
    """function that returns a unique set of ERF resource ids open erfby 
    type page & pull out all resTypeId=\d+ as array"""
    response = urllib2.urlopen(baseurl+all_res_types)
    html = response.read()
    restypeid = re.findall('resTypeId=\d+', html)
    
    resids = []
    
    #Open each resTypeId page & capture the indiviual ERF resource ids (resIds)
    for id in restypeid:
        typeurl = baseurl + search_res_types + str(id)
        typeresponse = urllib2.urlopen(typeurl)
        typehtml = typeresponse.read()
        resid_part = re.findall('resId=\d+', typehtml)
        resids.extend(resid_part)

    unique_resids = set(resids)

    return(unique_resids)
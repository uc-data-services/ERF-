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
#creating a dict
# need to abstract the url handling and opening into a function
# need to 
res_ids = get_resource_ids() 
response = urllib2.urlopen('http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=1795')
html = response.read()
erf_list = list(re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html))
erf_list = [[i[0].lower().rstrip(':').replace(" ", "_"), i[1]] for i in erf_list]
erf_dict = dict(erf_list)
erf_dict['resource_id'] = int('resId=1795'.lstrip("resId=")) #need to pull out resId from res_ids
erf_dict['subject'] = [i[1] for i in erf_list if i[0] == "Subject:"]
erf_dict['core_subject'] = [i[1] for i in erf_list if i[0] == "Core subject:"]
erf_dict['resource_type'] = [i[1] for i in erf_list if i[0] == "Resource Type:"]


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
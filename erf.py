import urllib2
import re
import sqlite3
import datetime
import os

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

# change working dir for dreampie
# investigate putting connection object in a function or class
os.chdir('/home/tim/Dropbox/ERF-/')
db_filename = 'erf.sqlite'
connection = sqlite3.connect('erf.sqlite')
schema = 'erf_schema.sql'
cursor = connection.cursor()
res_ids = get_resource_ids() 

#need to wrap below in fucntion & for each resid, pull out urllib stuff into function call
response = urllib2.urlopen('http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=1795')
html = response.read()
erf_list = list(re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html))
erf_list = [[i[0].lower().rstrip(':').replace(" ", "_"), i[1]] for i in erf_list]
erf_dict = dict(erf_list)
erf_dict['resource_id'] = int('resId=1795'.lstrip("resId=")) #need to pull out resId from res_ids
erf_dict['subject'] = [i[1] for i in erf_list if i[0] == "subject"]
erf_dict['core_subject'] = [i[1] for i in erf_list if i[0] == "core_subject"]
erf_dict['resource_type'] = [i[1] for i in erf_list if i[0] == "resource_type"]
if 'text' not in erf_dict:
    erf_dict['text'] = 'NULL'
if 'publication_dates_covered' not in erf_dict:
    erf_dict['publication_dates_covered'] = 'NULL'
if 'alternative_title' not in erf_dict:
    erf_dict['alternative_title'] = 'NULL'
if 'licensing_restriction' not in erf_dict:
    erf_dict['licensing_restriction']='NULL'
add_to_db(erf_dict)

def create_db_tables():
    with sqlite3.connect(db_filename) as conn:
        print 'Creating schema'
        with open(schema, 'rt') as f:
            schema = f.read()
        conn.executescript(schema)

def add_to_db(erf_dict):
    '''takes a dictionary representation of an ERF record and inserts into a sqlite3 db'''
    with sqlite3.connect(db_filename) as conn:
        resource_stmt = """INSERT INTO resource 
                        (title, resource_id, text, description, coverage, 
                         licensing, last_modified, url, alternative_title) 
                         VALUES (?,?,?,?,?,?,?,?,?)"""
        conn.execute(resource_stmt, (erf_dict['title'], 
                                       erf_dict['resource_id'],
                                       erf_dict['text'],
                                       erf_dict['brief_description'], 
                                       erf_dict['publication_dates_covered'],
                                       erf_dict['licensing_restriction'],
                                       erf_dict['record_last_modified'],
                                       erf_dict['url'],
                                       erf_dict['alternative_title'])) # adding fields to the resource table in db
        
        conn.commit()
        #capture the lastrowid for use in bridge table b/t resource & subject
        rid = conn.lastrowid()
        erf_subj = erf_dict['subject'] # create a list out of subject terms
        erf_core = erf_dict['core_subject'] # create a list out of core subject terms
        erf_type = erf_dict['resource_type'] # create a list out of types
        '''for each term in subject do: insert into subject table, insert rid, sid into r_s_bridge, add logic to test is member of 
        core_subject and if is insert 1 into r_s_bridge core_subject bool
        need to handle if subj_term in table already, if so capture id so can add sid to r_s_bridge table'''
        subject_stmt = "INSERT INTO subject (term) VALUES (?)"
        type_stmt = "INSERT INTO type (type) VALUES (?)"
        rs_bridge_stmt = "INSERT INTO r_s_bridge (rid, sid, is_core) VALUES (?,?,?)"
        rt_bridge_stmt = "INSERT INTO r_t_bridge (rid, tid) VALUES (?,?)"
        is_core = 0 #initialize is_core to false
        for term in erf_subj:
            cursor.execute("SELECT sid FROM subject WHERE term=?", (term,))    
            is_term = cursor.fetchone()
            if is_term is not None:
                sid = is_term[0]
            else:    
                cursor.execute(subject_stmt, (term,))
                connection.commit()
                sid = cursor.lastrowid
            for erf_core_term in erf_core:
                if erf_core_term == term:
                    is_core = 1
            cursor.execute(rs_bridge_stmt, (rid,sid, is_core))
            connection.commit()
        for term in erf_type:
            cursor.execute("SELECT tid FROM type WHERE type=?", (term,))
            is_type = cursor.fetchone()
            if is_type is not None:
                tid = is_type[0]
            else:
                cursor.execute(type_stmt, (term,))
                connection.commit()
                tid = cursor.lastrowid
            cursor.execute(rt_bridge_stmt, (rid, tid))
            connection.commit()
        #decide whether or not to save alternate title

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
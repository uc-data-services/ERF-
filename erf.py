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
create_db_tables() #currently drops existing tables and creates them anew
#parts of the ERF urls for global use
baseurl = 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?'
all_res_types = 'cmd=allResTypes'
search_res_types = 'cmd=searchResType&'
detail = 'cmd=detail&'
resid = 'resId=3446' #need to remove when put in resid loop
os.chdir('/home/tim/Dropbox/ERF-/')
db_filename = 'erf.sqlite'
conn = sqlite3.connect(db_filename)
c = conn.cursor()
res_ids = get_resource_ids()

for id in resids: 
    response = urllib2.urlopen(baseurl+detail+id)
    html = response.read()
    erf_dict = parse_page(html)
    erf_dict['resource_id'] = int(resid.lstrip("resId=")) #need to pull out current resId from res_ids & add to dict
    resource_stmt = """INSERT INTO resource 
                    (title, resource_id, text, description, coverage, 
                     licensing, last_modified, url, alternative_title) 
                     VALUES (?,?,?,?,?,?,?,?,?)"""
    c.execute(resource_stmt, (erf_dict['title'], 
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
    rid = c.lastrowid
    erf_subj = erf_dict['subject'] # create a list out of subject terms
    erf_core = erf_dict['core_subject'] # create a list out of core subject terms
    erf_type = erf_dict['resource_type'] # create a list out of types
    
    subject_stmt = "INSERT INTO subject (term) VALUES (?)"
    rs_bridge_stmt = "INSERT INTO r_s_bridge (rid, sid, is_core) VALUES (?,?,?)"
    is_core = 0
    for term in erf_subj:
        c.execute("SELECT sid FROM subject WHERE term=?", (term,))    
        is_term = c.fetchone()
        if is_term is not None:
            sid = is_term[0]
        else:    
            c.execute(subject_stmt, (term,))
            conn.commit()
            sid = c.lastrowid
        for erf_core_term in erf_core:
            if erf_core_term == term:
                is_core = 1
        c.execute(rs_bridge_stmt, (rid,sid, is_core))
        conn.commit()
        
    type_stmt = "INSERT INTO type (type) VALUES (?)"
    rt_bridge_stmt = "INSERT INTO r_t_bridge (rid, tid) VALUES (?,?)"
    for term in erf_type:
        c.execute("SELECT tid FROM type WHERE type=?", (term,))
        is_type = c.fetchone()
        if is_type is not None:
            tid = is_type[0]
        else:
            c.execute(type_stmt, (term,))
            conn.commit()
            tid = c.lastrowid
        c.execute(rt_bridge_stmt, (rid, tid))
        conn.commit()

def parse_page(html):
    '''Takes in erf html page & resourceId, parses html and returns a dict representing an erf entry'''
    erf_list = list(re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html))
    erf_list = [[i[0].lower().rstrip(':').replace(" ", "_"), i[1]] for i in erf_list]
    erf_dict = dict(erf_list)
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
    
    return erf_dict


def create_db_tables():
    '''Creates tables for in erf.sqlite, if tables already exist, will drop them.'''
    schema = 'erf_schema.sql'
    with sqlite3.connect(db_filename) as conn:
        print 'Creating schema'
        with open(schema, 'rt') as f:
            schema = f.read()
        conn.executescript(schema)
        conn.close()

def get_resource_ids():
    """Returns a unique set of ERF resource ids open erf by type page & pull out all resTypeId=\d+ as array"""
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
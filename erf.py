#!/usr/bin/evn python

import urllib2
import re
import sqlite3
import datetime
import os
import time
import getopt
import sys


"""
1.access by type erf and findall resourcetypes
2.open each resourceid pages and findall resourceIDs, we also will need to persist these ids in DB, but might
 be part of record page
3. open each http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=[#]
and then parse html into sqlite3 db - need to model db. am assuming will create a dictionary
for this. will need to keep up what resId are already saved to db and use logic not to add
duplicates
"""
baseurl = 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?'
all_res_types = 'cmd=allResTypes'
search_res_types = 'cmd=searchResType&'
detail = 'cmd=detail&'
#resid = 'resId=3446 ' #need to remove when put in resid loop
#os.chdir('/home/tim/Dropbox/ERF-/')
db_filename = 'erf.sqlite'
RETRY_DELAY = 2

def parse_page(html):
    '''Takes in erf html page & parses html and returns a dict representing an erf entry'''
    if html.find('Centre\xc3\xa2\xc2\x80\xc2\x99s'):
        html = html.replace('Centre\xc3\xa2\xc2\x80\xc2\x99s',"Centre's")
    if html.find('Tageb\xc3\x83\xc2\xbccher'):
        html = html.replace('Tageb\xc3\x83\xc2\xbccher', 'Tageb&uuml;cher')
    erf_list = list(re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html))
    erf_list = [[i[0].lower().rstrip(':').replace(" ", "_"), i[1]] for i in erf_list]
    erf_dict = dict(erf_list)
    erf_dict['subject'] = [i[1] for i in erf_list if i[0] == "subject"]
    erf_dict['core_subject'] = [i[1] for i in erf_list if i[0] == "core_subject"]
    erf_dict['resource_type'] = [i[1] for i in erf_list if i[0] == "resource_type"]
    if [i[1] for i in erf_list if i[0] == "alternate_title"]:
        erf_dict['alternate_title'] = [i[1] for i in erf_list if i[0] == "alternate_title"]
    if 'text' not in erf_dict:
        erf_dict['text'] = 'NULL'
    if 'publication_dates_covered' not in erf_dict:
        erf_dict['publication_dates_covered'] = 'NULL'
    if 'licensing_restriction' not in erf_dict:
        erf_dict['licensing_restriction']='NULL'
    if 'brief_description' not in erf_dict:
        erf_dict['brief_description'] = 'NULL'    
    return erf_dict


def create_db_tables():
    '''Creates tables for in erf.sqlite, if tables already exist, will drop them.'''
    schema = 'erf_schema.sql'
    with sqlite3.connect(db_filename) as conn:
        print 'Creating schema'
        with open(schema, 'rt') as f:
            schema = f.read()
        conn.executescript(schema)

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
    unique_resids = natsort(set(resids))
    return(unique_resids)

def natsort(list_):
    '''a natural sort copied from pypi'''
    # decorate
    tmp = [ (int(re.search('\d+', i).group(0)), i) for i in list_]
    tmp.sort()
    # undecorate
    return [ i[1] for i in tmp ]

def get_local_resids_and_update_dates():
    '''Gets the resIds & last update dates from the local sqlite database'''
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    last_mod_stmt = "SELECT resource_id, last_modified FROM resource" #get the resource_id & last_modified date from local db
    c.execute(last_mod_stmt)
    local_resids_and_updates = c.fetchall() #make a 2-tuple from db query for resource_id & last_modified_date from local db
    conn.close()
    return local_resids_and_updates

def erf_resids_and_lastupdates(erf_res_ids):
    '''Returns a list of ERF resIds and last update dates.'''
    erf_res_ids_last_mod = []
    for ids in erf_res_ids:
        response = urllib2.urlopen(baseurl+detail+ids)
        html = response.read()
        last_update = re.search('<B>Record last modified:</B>\s(.*?)<BR>', html).group(1)
        erf_res_ids_last_mod.append((id, last_update)) #need to add as tuple
    return erf_res_ids_last_mod

def resids_needing_updating_and_adding(local_resids_and_dates, erf_res_ids_and_dates):
    '''Takes two 2-lists & returns a list resids that need updating or adding'''
    #for new, need to just find the diff b/t erf_resourceIds minus sqlite resource ids
    local_resids, local_dates_modified = zip(*local_resids_and_dates) #unzipping the 2-tuple list so we can get diff
    erf_resids, erf_dates_last_modified = zip(*erf_res_ids_and_dates) #unzipping the 2-tuple list so we can find diff
    new_resids = set(erf_resids)-set(local_resids) #should get back a list of new resource ids
    unpublish_resids = set(local_resids)-set(erf_resids)#should tell us what's has been removed from ERF & needs unpublishing
    update_resids = []
    for lids, ldate in local_resids_and_dates:
        #print ldate
        for rids, rdate in erf_res_ids_and_dates:
            #print rdate
            if lids == rids:
                if ldate != rdate:
                    update_resids.append(rids)
                    #print rids
    print "New resouces ", new_resids
    print "Unpublish resources ", unpublish_resids
    print "Updated resources ", update_resids
    #call add_new_resources_to_db for 'new' list
    #call update_resources_to_db for 'update' list
    #figure out what to do with delete
    #needs some print statements telling us what happened, which resources were updated, what's new, what's deleted

def add_new_resources_to_db(res_ids): 
    '''Takes a list of resource ids from the ERF, opens the ERF detail page for each, and then
    the resources to a local sqlite db.'''
    create_db_tables() #currently drops existing tables    
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    for id in res_ids:
        try:       
            response = urllib2.urlopen(baseurl+detail+id) # poss. move opening, reading and returning html of erf resource detail to own funciton
            html = response.read()
            erf_dict = parse_page(html)
            erf_dict['resource_id'] = int(id.lstrip("resId=")) #need to pull out current resId from res_ids & add to dict
            resource_stmt = "INSERT INTO resource (title, resource_id, text, description, coverage, licensing, last_modified, url) VALUES (?,?,?,?,?,?,?,?)"
            c.execute(resource_stmt, (erf_dict['title'], 
                                           erf_dict['resource_id'],
                                           erf_dict['text'],
                                           erf_dict['brief_description'], 
                                           erf_dict['publication_dates_covered'],
                                           erf_dict['licensing_restriction'],
                                           erf_dict['record_last_modified'],
                                           erf_dict['url'],)) # adding fields to the resource table in db
            
            conn.commit()
            rid = c.lastrowid #capture last row id of resource
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
            if "alternate_title" in erf_dict: 
                erf_alt = erf_dict['alternate_title']
                alt_title_stmt = "INSERT INTO alternate_title (title, rid) VALUES (?,?)"
                for term in erf_alt:
                    c.execute(alt_title_stmt, (term, rid))             
            print " Resource ID: ", erf_dict['resource_id'], "Title: ", erf_dict['title']
        except sqlite3.ProgrammingError as err:
            print ('Error: ' + str(err))
            print erf_dict['title']
        except urllib2.URLError as err:
            if err.reason[0] == 104: # Will throw TypeError if error is local, but we probably don't care
                print str(err)
                time.sleep(RETRY_DELAY)
    conn.close()

    
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "huc", ["help", "update", "create"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ("-u", "--update"):
           #need function that updates db 
            print "  ***update not implemented yet***"
            resids_needing_updating_and_adding(get_local_resids_and_update_dates(), erf_resids_and_lastupdates(get_resource_ids))
                                                               
            usage()
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--create"):
            add_new_resources_to_db(get_resource_ids())
        else:
            assert False, "unhandled option"

def usage():
    print """
    ERF Scrape Usage:
    
    1. Create a new local erf sqlite datatbase:
    
    >>>python erf.py --create 
    
    2. Update the local erf data base:
    
    >>>python erf.py --update
    
    """

if __name__ == '__main__':
    main()
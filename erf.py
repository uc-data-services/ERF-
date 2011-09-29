#!/usr/bin/evn python
# -*- coding: utf-8 -*-

import urllib2
import re
import sqlite3
import datetime
import os
import time
import getopt
import sys
import xmlwitch
from pubsubhubbub_publish import * 
from rfc3339 import rfc3339
import uuid

baseurl = 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?'
db_filename = 'erf.sqlite'
RETRY_DELAY = 2

def parse_page(rid):
    '''Takes a resource_id (rid), fetches erf detail page, parses html, & returns a dict representing an erf entry'''
    detail = 'cmd=detail&'
    resid_slug = 'resId='
    rid = str(rid)
    response = urllib2.urlopen(baseurl+detail+resid_slug+rid) # poss. move opening, reading and returning html of erf resource detail to own funciton
    html = response.read().decode('latin1')
    if html.find(u'Centre\xc3\xa2\xc2\x80\xc2\x99s'):
        html = html.replace(u'Centre\xc3\xa2\xc2\x80\xc2\x99s',"Centre's")
    if html.find(u'Tageb\xc3\x83\xc2\xbccher'):
        html = html.replace(u'Tageb\xc3\x83\xc2\xbccher', u'Tageb\xfccher')
    if html.find(u'R\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9pertoire International de Litt\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9rature Musicale'):
        html = html.replace(u'R\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9pertoire International de Litt\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9rature Musicale', u'R\xe9pertoire International de Litt\xe9rature Musicale')
    if html.find(u'Eric Weisstein\xc3\x82\xc2\x92s World of Mathematics'):
        html = html.replace(u'Eric Weisstein\xc3\x82\xc2\x92s World of Mathematics', "Eric Weisstein's World of Mathematics")
    erf_list = list(re.findall('<B>(.*?:)</B>\s(.*?)<BR>', html)) 
    erf_list = [[i[0].lower().rstrip(':').replace(" ", "_"), i[1]] for i in erf_list]
    erf_dict = dict(erf_list)
    url_str = erf_dict['url']
    regex_url = re.compile(">(.*?)</A>")
    erf_dict['url'] = re.search(regex_url, url_str).group(1).replace(" ", "")    
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
    '''Creates tables for in erf.sqlite, If tables already exist, will drop them.'''
    print "Creating database and tables..."
    schema = 'erf_schema.sql'
    with sqlite3.connect(db_filename) as conn:
        #print 'Creating schema'
        with open(schema, 'rt') as f:
            schema = f.read()
        conn.executescript(schema)

def get_resource_ids():
    """Returns a set() of ERF resource ids from the ERF."""
    all_res_types = 'cmd=allResTypes'
    search_res_types = 'cmd=searchResType&'
    response = urllib2.urlopen(baseurl+all_res_types)
    html = response.read()
    restypeid = re.findall('resTypeId=\d+', html) 
    resids = [] 
    #Open each resTypeId page & capture the indiviual ERF resource ids (resIds)
    for id in restypeid:
        typeurl = baseurl + search_res_types + str(id)
        typeresponse = urllib2.urlopen(typeurl)
        typehtml = typeresponse.read()
        resid_part = re.findall('resId=(\d+)', typehtml)
        resids.extend(resid_part)
    unique_resids = natsort(set(resids))
    print "Number of unique Ids: ", len(unique_resids)
    return(unique_resids)

def natsort(list_):
    '''a natural sort copied from pypi'''
    # decorate
    tmp = [(int(re.search('\d+', i).group(0)), i) for i in list_]
    tmp.sort()
    # undecorate
    return [i[1] for i in tmp]

def get_local_resids_and_update_dates():
    '''Gets the resIds & last update dates from the local sqlite database'''
    with sqlite3.connect(db_filename) as conn:
        c = conn.cursor()
        last_mod_stmt = "SELECT resource_id, last_modified FROM resource" #get the resource_id & last_modified date from local db
        c.execute(last_mod_stmt)
        local_resids_and_updates = c.fetchall() #make a 2-tuple from db query for resource_id & last_modified_date from local db
    return local_resids_and_updates

def get_erf_resids_and_lastupdates(erf_res_ids):
    '''Returns a list of ERF resIds and last update dates.'''
    detail = 'cmd=detail&'
    erf_res_ids_last_mod = []
    for rid in erf_res_ids:
        #print rid
        erf_dict = parse_page(rid)
        last_update = erf_dict['record_last_modified']
        erf_res_ids_last_mod.append((rid, last_update)) #need to add as tuple
    return erf_res_ids_last_mod

def resids_needing_updating_and_adding(local_resids_and_dates, erf_res_ids_and_dates):
    '''Takes two 2-lists of resids & update dates -- one local from sqlite db and the other from the ERF website-- 
    and determines what's new, what needs to be updated, what needs to be unpublished or removed. Then it calls the 
    appropriate functions to add, update or unpublish.'''
    local_resids, local_dates_modified = zip(*local_resids_and_dates) #unzipping the 2-tuple list so we can get diff
    erf_resids, erf_dates_last_modified = zip(*erf_res_ids_and_dates) #unzipping the 2-tuple list so we can find diff
    erf_resids = [int(i) for i in erf_resids]
    erf_res_ids_and_dates = zip(erf_resids, erf_dates_last_modified) #need to have erf as type ints for comparison
    new_resids = set(erf_resids)-set(local_resids) #should get back a list of new resource ids from ERF that aren't in local db
    if new_resids: #see if new_resids list has
        add_new_resources_to_db(new_resids)
    canceled_resources = set(local_resids)-set(erf_resids)#should tell us what's has been removed from ERF & needs unpublishing
    if canceled_resources: #see if there are any resources needing to be unpublished
        cancel_resource(canceled_resources)
    update_resids = []
    for lids, ldate in local_resids_and_dates: #since workign with 2-lists need to match id & then compare dates
        for rids, rdate in erf_res_ids_and_dates:
            if lids == rids:
                if ldate != rdate: #if the dates of local and erf are different
                    update_resids.append(rids)
    if update_resids:  #see if there are resources needing updating
        print update_resids
        update_resources_in_db(update_resids)
    if new_resids or canceled_resources or update_resids: #if any changes write out new atom feed
        write_to_atom()
    print "Number of new resouces: ", len(new_resids)
    print "Number of resources needing unpublishing: ", len(canceled_resources)
    print "Number of resources needing updating: ", len(update_resids)

def cancel_resource(canceled_resources):
    '''Takes a list of resources that are no longer in the ERF and flags them as canceled in db.'''
    with sqlite3.connect(db_filename) as conn:
        cancel_stmt = "UPDATE resource SET is_canceled = 1 WHERE resource_id = ?"
        for resid in canceled_resources:
            c.execute(cancel_stmt, (resid,))
        conn.close()

def add_new_resources_to_db(res_ids): 
    '''Takes a list of resource ids from the ERF, opens the ERF detail page for each, and then
    the resources to a local sqlite db. Calls other functions to add subjects & types.'''
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    print "Adding new resources to the database."
    for id in res_ids:
        try:       
            erf_dict = parse_page(id)
            erf_dict['resource_id'] = int(id) #need to pull out current resId from res_ids & add to dict
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
            add_or_update_subject(erf_subj, rid) #passing subject list, core list to add subject function            
            erf_core = erf_dict['core_subject'] # create a list out of core subject terms 
            add = True #set add to true so add_or_update_core() knows to add not remove 
            add_or_update_core(add, erf_core, rid)
            erf_type = erf_dict['resource_type'] # create a list out of types 
            if "resource_type" in erf_dict: #need to instead test to see if 'resource_type' is empty
                add_type_to_db(erf_type, rid)            
            if "alternate_title" in erf_dict: 
                erf_alt = erf_dict['alternate_title']
                add_alt_title(erf_alt, rid)          
            print " Resource ID: ", erf_dict['resource_id'], "  Title: ", erf_dict['title']
           
        except sqlite3.ProgrammingError as err:
            print ('Error: ' + str(err))
            print erf_dict['title']
        except urllib2.URLError as err:
            if err.reason[0] == 104: # Will throw TypeError if error is local, but we probably don't care
                print str(err)
                time.sleep(RETRY_DELAY)
    c.execute("select rid from resource")
    print "No added to DB:  ", len(c.fetchall()), "  ERF Resids; ",  len(res_ids)
    conn.close()


def update_resources_in_db(update_list):
    '''Takes a list of resource ids needing updating, gets the erf_dict of each rid from page_parse(), then updates the 
    local database directly and calls functions to also add new subject terms &/or remove terms.'''
    print "Updating resources..."
    with sqlite3.connect(db_filename) as conn:
        cursor = conn.cursor()
        for resid in update_list:
            resource_id = resid
            query = """UPDATE resource SET title=:title, text = :text, description = :description, coverage = :coverage, licensing = :licensing, last_modified = :last_modified,  url = :url WHERE resource_id = :resource_id
             """
            erf_dict = parse_page(resid)
            title, text, description, coverage, licensing, last_modified, url = erf_dict['title'], erf_dict['text'], erf_dict['brief_description'], erf_dict['publication_dates_covered'], erf_dict['licensing_restriction'], erf_dict['record_last_modified'], erf_dict['url']
            cursor.execute(query, {'title':title, 
                                           'text':text,
                                           'description':description, 
                                           'coverage':coverage,
                                           'licensing':licensing,
                                           'last_modified':last_modified,
                                           'url':url, 
                                           'resource_id':resource_id,}) # adding fields to the resource table in db
            
            conn.commit()
            rid = cursor.lastrowid #capture last row id of resource
            erf_subj = erf_dict['subject'] # create a list out of subject terms
            subjects = "SELECT term FROM subject JOIN r_s_bridge ON subject.sid = r_s_bridge.sid WHERE rid=?"
            cursor.execute(subjects, (rid,))
            subject_terms = cursor.fetchall()
            new_subjects = set(erf_subj)-set(subject_terms)#diff b/t erf_subjects and subject terms in DB to determine new subjects                        
            if new_subjects:
                add_or_update_subject(new_subjects, rid) #adds new subjects
            erf_core = erf_dict['core_subject'] # create a list out of core subject terms            
            core_terms_stmt = "SELECT term FROM subject JOIN r_s_bridge ON subject.sid = r_s_bridge.sid WHERE rid=? AND r_s_bridge.is_core=1"# pull out core terms
            cursor.execute(core_terms_stmt, (rid,))
            core_terms = cursor.fetchall()
            new_core = set(erf_core)-set(core_terms) #need to also check for removal            
            if new_core: #there's somethign in new_core, then call method
                add = True
                add_or_update_core(add, erf_core, rid)
            remove_subjects = set(subject_terms)-set(erf_subj)#dif b/t subj terms in db & erf to see what to remove from db
            remove_core = set(core_terms) - set(erf_core)
            if remove_core:
                add = False #set add to false so function will remove
                add_or_update_core(add, remove_core, rid)
            if remove_subjects:
                print remove_subjects #need to pass remove subjects list to a remove_subject(): function
            erf_type = erf_dict['resource_type'] # create a list out of types 
            if erf_type:
                print erf_type 
            # need sql queries for types and then a add type and remove type function
            print " Resource ID: ", erf_dict['resource_id'], "  Title: ", erf_dict['title']

def add_or_update_core(add, erf_core, rid):
    '''Takes an add boolean (true=add, false=remove), erf_core list & rid and adds or updates the database.'''
    print erf_core, rid
    add_stmt = "INSERT INTO r_s_bridge (rid, sid, is_core) VALUES (?,?,?)"
    remove_stmt = "UPDATE r_s_bridge SET is_core = '0' WHERE sid = ? AND rid = ?"
    is_core = 1
    with sqlite3.connect(db_filename) as conn:
        c = conn.cursor()
        for core_term in erf_core:
            c.execute("SELECT sid FROM subject WHERE term=?", (core_term,))    
            is_term = c.fetchone() # we can assume that subject term exists and has already been added to db
            sid = is_term[0]
            if add:
                c.execute(add_stmt, (rid, sid, is_core))
            else: #false means remove
                c.execute(remove_stmt, (sid, rid))
            conn.commit()

def add_or_update_subject(subj_list, rid):
    '''Takes a subject list, a core subject list and a resource id and adds those to the local db.'''
    subject_stmt = "INSERT INTO subject (term) VALUES (?)"
    with sqlite3.connect(db_filename) as conn:
        c = conn.cursor()
        for term in subj_list:
            c.execute("SELECT sid FROM subject WHERE term=?", (term,))    
            is_term = c.fetchone()
            if is_term is not None: #term exists in db & doesn't have rid
                sid = is_term[0]
            else:    
                c.execute(subject_stmt, (term,))
                conn.commit()
                sid = c.lastrowid
        conn.commit()
            
def add_type_to_db(type_list, rid):
    '''Takes a list of ERF types & resource ID and adds to the local sqlite db.'''
    type_stmt = "INSERT INTO type (type) VALUES (?)"
    rt_bridge_stmt = "INSERT INTO r_t_bridge (rid, tid) VALUES (?,?)"
    with sqlite3.connect(db_filename) as conn:
        c = conn.cursor()    
        for term in type_list:
            c.execute("SELECT tid FROM type WHERE type=?", (term,))
            is_type = c.fetchone()
            if is_type is not None:
                tid = is_type[0]
            else:
                c.execute(type_stmt, (term,))
                conn.commit()
                tid = c.lastrowid
            c.execute(rt_bridge_stmt, (rid, tid))
            conn.commit
            
def add_alt_title(alt_title_list, rid):
    '''Takes a alternate title list & resource id and adds it to the database.'''
    with sqlite3.connect(db_filename) as conn:
        c = conn.cursor()    
        alt_title_stmt = "INSERT INTO alternate_title (title, rid) VALUES (?,?)"
        for term in alt_title_list:
            c.execute(alt_title_stmt, (term, rid))  
                                
def write_to_atom():
    '''Writes out ERF data from local SQLite db into ATOM schema extended with Dublin Core. Notifies pubsubhubbub 
    service that a new update is ready for consuming.'''
    detail = 'cmd=detail&'
    atom_xml_write_directory = '/var/www/html/erf-atom/' #'/home/tim/'
    erf_atom_filename = 'erf-atom.xml'
    now = rfc3339(datetime.datetime.now())
    with sqlite3.connect(db_filename) as conn:
        with open(atom_xml_write_directory+erf_atom_filename, mode='w+') as atom:      
            cursor = conn.cursor()
            resids = "SELECT rid FROM resource"
            cursor.execute(resids)
            rids = cursor.fetchall()
            rids = [rid[0] for rid in rids]
            erf_url = 'library.berkeley.edu/find/types/electronic_resources.html'
            xml = xmlwitch.Builder(version='1.0', encoding='utf-8')
            with xml.feed(**{'xmlns':'http://www.w3.org/2005/Atom', 'xmlns:dc':'http://purl.org/dc/terms/'}):
                xml.title('Eelectronic Resources - UC Berkeley Library')
                xml.updated(now)
                xml.link(href="http://doemo.lib.berkeley.edu/erf-atom/erf-atom.xml", rel="self", type="application/atom+xml")
                xml.link(rel="hub", href="https://pubsubhubbub.appspot.com")
                xml.id(uuid.uuid3(uuid.NAMESPACE_DNS, 'library.berkeley.edu/find/types/electronic_resources.html'))
                with xml.author:
                    xml.name('UC Berkeley The Library')
                    xml.id(uuid.uuid3(uuid.NAMESPACE_DNS, 'http://www.lib.berkeley.edu'))
                for rid in rids:
                    #rid = str(rid)
                    resource_details_stmt = "SELECT title, resource_id, text, description, coverage, licensing, last_modified, url FROM resource WHERE rid = ?"
                    subjects = "SELECT term FROM subject JOIN r_s_bridge ON subject.sid = r_s_bridge.sid WHERE rid= ?"
                    #alternate_title_stmt = "SELECT title FROM alternate_title WHERE rid = ?"
                    types_stmt = "SELECT type FROM type JOIN r_t_bridge ON type.tid = r_t_bridge.tid WHERE rid= ?"      
                    cursor.execute(resource_details_stmt, (rid,))
                    resource_details_db = cursor.fetchone()
                    title, resource_id, text, description, coverage, licensing, last_modified, url = resource_details_db
                    cursor.execute(subjects, (rid,))
                    subjects_db = cursor.fetchall()
                    subjects_db = [subject[0] for subject in subjects_db]
                    cursor.execute("SELECT title from alternate_title WHERE rid=?", (rid,))
                    alt_title = cursor.fetchall()
                    alt_title = [a_title[0] for a_title in alt_title]
                    cursor.execute(types_stmt, (rid,))
                    types = cursor.fetchall()
                    types = [a_type[0] for a_type in types]
                    url_id = baseurl+detail+str(resource_id)
                    with xml.entry:
                        xml.title(title)
                        xml.id(url_id)
                        xml.updated(rfc3339(last_modified))
                        xml.dc__description(description)
                        if coverage != "NULL":
                            xml.dc__coverage(coverage)
                        if licensing != "NULL":
                            xml.dc__accessRights(licensing)
                        for subject in subjects_db:
                            ##need another test to see if is core & if so, add attribute
                            xml.dc__subject(subject)
                        for a_title in alt_title:
                            xml.dc__alternate(a_title)
                        for type in types:
                            xml.dc__type(type)
                        xml.url(url)              
            print(xml)
            atom.write(str(xml))
            publish_to_hub()

def publish_to_hub():
    try:     
        publish('https://pubsubhubbub.appspot.com', 
                'http://doemo.lib.berkeley.edu/erf-atom/erf-atom.xml')
        print "Publishing the atom feed to pubsubhubbub.appspot.com"

    except PublishError, e:
        print e

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "huca", ["help", "update", "create", "atom"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ("-u", "--update"):
            #need function that updates db 
            erf_resource_ids = get_resource_ids()
            erf_ids_and_updates = get_erf_resids_and_lastupdates(erf_resource_ids)
            local_resids_updates = get_local_resids_and_update_dates()
            resids_needing_updating_and_adding(local_resids_updates, erf_ids_and_updates)
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--create"):
            create_db_tables()
            add_new_resources_to_db(get_resource_ids())
            write_to_atom()
        elif o in ("-a", "--atom"):
            write_to_atom()
        else:
            assert False, "unhandled option"

def usage():
    print """
    ERF Scrape Usage:
    
    1. Create a new local erf sqlite datatbase:
    
    >>>python erf.py --create 
    
    2. Update the local erf data base:
    
    >>>python erf.py --update
    
    3. Write an ATOM representation of each resource to file: currently set to write to /var/www/html/erf-atom on doemo.lib
    
    >>>python erf.py --atom
    
    """

if __name__ == '__main__':
    main()
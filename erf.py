#!/usr/bin/evn python
# -*- coding: utf-8 -*-


"""This module web-scrapes the Electronic Resource
Finder (ERF) for the UC Berkeley Library. It saves the resources to a
local sqlite database and then writes the resources out into a atom feed.
"""

from urllib2 import Request, urlopen, URLError
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
import logging
from pprint import pprint

BASE_URL = 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?'
DB_FILENAME = 'erf.sqlite'
RETRY_DELAY = 2

#setting up logger below
logger = logging.getLogger('erf-scrape')
handler = logging.FileHandler('erf-scrape.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def get_page(url):
    """
    takes a url and opens, reads and returns html.
    """
    req = Request(url)
    try:
        response = urlopen(req)
    except URLError, e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
            time.sleep(RETRY_DELAY)
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
            time.sleep(RETRY_DELAY)
    else:
        html = response.read()
        return html


def parse_page(rid):
    """
    Takes a resource_id (rid), fetches erf detail page, parses html, & returns a dict representing an erf entry
    """
    detail = 'cmd=detail&'
    resid_slug = 'resId='
    rid = str(rid)
    url = BASE_URL+detail+resid_slug+rid
    html = get_page(url)
    if html.find(u'Centre\xc3\xa2\xc2\x80\xc2\x99s'):
        html = html.replace(u'Centre\xc3\xa2\xc2\x80\xc2\x99s',"Centre's")
    if html.find(u'Tageb\xc3\x83\xc2\xbccher'):
        html = html.replace(u'Tageb\xc3\x83\xc2\xbccher', u'Tageb\xfccher')
    if html.find(u'R\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9pertoire International de Litt\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9rature Musicale'):
        html = html.replace(u'R\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9pertoire International de Litt\xc3\x83\xc2\x83\xc3\x82\xc2\x83\xc3\x83\xc2\x82\xc3\x82\xc2\x83\xc3\x83\xc2\x83\xc3\x82\xc2\x82\xc3\x83\xc2\x82\xc3\x82\xc2\xa9rature Musicale', u'R\xe9pertoire International de Litt\xe9rature Musicale')
    if html.find(u'Eric Weisstein\xc3\x82\xc2\x92s World of Mathematics'):
        html = html.replace(u'Eric Weisstein\xc3\x82\xc2\x92s World of Mathematics',
                            "Eric Weisstein's World of Mathematics")
    if html.find('\r\n'):
        html = html.replace('\r\n', ' ') 
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
    """
    Creates tables for in erf.sqlite, If tables already exist, will drop them.
    """
    logger.info("Creating database and tables...")
    schema = 'erf_schema.sql'
    with sqlite3.connect(DB_FILENAME) as conn:
        #print 'Creating schema'
        with open(schema, 'rt') as f:
            schema = f.read()
        conn.executescript(schema)

def get_resource_ids():
    """
    Returns a set() of ERF resource ids from the ERF.
    """
    all_res_types = 'cmd=allResTypes'
    search_res_types = 'cmd=searchResType&'
    url = BASE_URL+all_res_types
    html = get_page(url)
    resource_type_id = re.findall('resTypeId=\d+', html)
    resource_ids = []
    #Open each resTypeId page & capture the individual ERF resource ids (resIds)
    for id in resource_type_id:
        type_url = BASE_URL + search_res_types + str(id)
        type_response = get_page(type_url)
        resid_part = re.findall("resId=(\d+)", type_response)
        resource_ids.extend(resid_part)
    unique_resource_ids = natsort(set(resource_ids))
    print("Number of unique Ids: ", len(unique_resource_ids))
    return unique_resource_ids

def natsort(list_):
    """'a natural sort copied from pypi"""
    # decorate
    tmp = [(int(re.search('\d+', i).group(0)), i) for i in list_]
    tmp.sort()
    # undecorate
    return [i[1] for i in tmp]

def set_all_to_canceled():
    """
    Flags all resources as canceled in db. Need to do this b/c we will switch back to uncanceled as we
    iterate thru resource ids -- leaving the removed ids canceled.
    """
    with sqlite3.connect(DB_FILENAME) as conn:
        c = conn.cursor()
        cancel_stmt = "UPDATE resource SET is_canceled = 1"
        c.execute(cancel_stmt, (resid,))
        conn.close()

def resource_needs_updating(id, update_date, c):
    """
    takes resource id, update_date and cursor object and returns
    """
    update_query_statement = "SELECT last_modified FROM resource WHERE resource_id=?"
    c.execute(update_query_statement, (id,))
    return update_date == c.fetchone()[0]


def add_or_update_resources_to_db(res_ids):
    """
    Takes a list of resource ids from the ERF, opens the ERF detail page for
    each, and then the resources to a local sqlite db. Calls other functions to
    add subjects & types.
    """
    with sqlite3.connect(DB_FILENAME) as conn:
        c = conn.cursor()
        for id in res_ids:
            try:
                erf_dict = parse_page(id) #get erf as dict
                title, text, description, coverage, licensing, last_modified, url = erf_dict['title'], erf_dict['text'], erf_dict['brief_description'], erf_dict['publication_dates_covered'], erf_dict['licensing_restriction'], erf_dict['record_last_modified'], erf_dict['url']
                #TODO: see if we can just pass the key:value of erf_dict to sql execute statement, removing above assignment
                if not resource_in_db(id,c): #then add
                    #print erf_dict['resource_id']
                    erf_dict['resource_id'] = int(id) #need to pull out current resId from res_ids & add to dict
                    pprint(erf_dict)
                    insert_stmt = """INSERT INTO resource (title=:title, text = :text, description = :description, 
                                    coverage = :coverage, licensing = :licensing, last_modified = :last_modified,  
                                    url = :url)"""
                    c.execute(insert_stmt, {'title':title,
                                             'text':text,
                                             'description':description,
                                             'coverage':coverage,
                                             'licensing':licensing,
                                             'last_modified':last_modified,
                                             'url':url,
                                             'resource_id':id,})

                    rid = c.lastrowid #capture row id of resource
                    add_or_update_subject(erf_dict['subject'], rid, c) #passing subject list, core list to add subject function
                    add_or_update_core(erf_dict['core_subject'], rid, c)
                    if "resource_type" in erf_dict:
                        add_or_update_type_to_db(erf_dict['resource_type'], rid, c)
                    if "alternate_title" in erf_dict:
                        add_alt_title(erf_dict['alternate_title'], rid, c)
                    print("Added Resource ID: ", erf_dict['resource_id'], "  Title: ", erf_dict['title'], "to database")
                if resource_needs_updating(id, erf_dict['record_last_modified'], c): #then update it
                    update_statement = """UPDATE resource SET title=:title, text = :text, description = :description, coverage = :coverage, licensing = :licensing, last_modified = :last_modified,  url = :url WHERE resource_id = :resource_id
                   """
                    c.execute(update_statement, {'title':title,
                                                      'text':text,
                                                      'description':description,
                                                      'coverage':coverage,
                                                      'licensing':licensing,
                                                      'last_modified':last_modified,
                                                      'url':url,
                                                      'resource_id':id,})
                    rid = c.lastrowid #capture last row id of resource
                    add_or_update_subject(erf_dict['subject'], rid, c) #adds new subjects
                    if 'core_subject' in erf_dict: #there's something in new_core, then call method
                        add_or_update_core(erf_dict['core_subject'], rid, c)
                    if 'resource_type'in erf_dict:
                        add_or_update_type_to_db(erf_dict['resource_type'], id, c)
                    print(" Resource ID: ", erf_dict['resource_id'], "  Title: ", erf_dict['title'])
            except sqlite3.ProgrammingError as err:
                print ('Error: ' + str(err))
                print(id)

        c.execute("select rid from resource")
        print("No added to DB:  ", len(c.fetchall()), "  ERF Resids; ",  len(res_ids))
        conn.close()

def add_or_update_core(erf_core, rid, c):
    """
    Takes an  erf_core list & rid,  finds sid, sets all existing core terms for rid to zero. then
    sets core to 1 for those in list.
    """
    set_core_to_default = "UPDATE r_s_bridge SET is_core = '0' WHERE is_core='1' AND rid = ?"
    c.execute(set_core_to_default,(rid,)) #sets all existing is_core for rid to zero, so we can set to 1 (so we can remove ones)
    add_term_as_core_stmt = "UPDATE r_s_bridge SET is_core = ? WHERE sid = ? AND rid = ?"
    is_core = 1
    for core_term in erf_core:
        c.execute("SELECT sid FROM subject WHERE term=?", (core_term,)) #finds subject id for term
        is_term = c.fetchone()
        sid = is_term[0]
        c.execute(add_term_as_core_stmt, (is_core, sid, rid))

def resource_in_db(id,c):
    """
    takes a resource id & a cursor object and checks if id exists in db.
    """
    resource_id_statement = 'SELECT rid FROM resource WHERE resource_id=?;'
    c.execute(resource_id_statement,(id,))
    return c.fetchone()

def add_or_update_subject(subj_list, rid, c):
    """
    Takes a subject list, a core subject list, a resource id, a cursor object and adds
    those to the local db.
    """
    add_subject_stmt = "INSERT INTO subject (term) VALUES (?)"
    link_subj_rid_stmt = "INSERT INTO r_s_bridge (rid,sid) VALUES (?,?)"
    for term in subj_list:
        c.execute("SELECT sid FROM subject WHERE term=?", (term,))
        has_term = c.fetchone()
        if has_term is not None: #term exists in subject table assign its sid ot sid variable
            sid = has_term[0]
            c.execute("SELECT rid FROM r_s_bridge WHERE sid=? AND rid=?", (sid,rid))
            has_rid = c.fetchone()
            if not has_rid: #if doesn't have
                c.execute(link_subj_rid_stmt, (rid, sid))
        else:
            c.execute(add_subject_stmt, (term,))
            sid = c.lastrowid
            c.execute(link_subj_rid_stmt, (rid, sid))

def add_or_update_type_to_db(type_list, rid, c):
    """
    Takes a list of ERF types & resource ID and adds to the local sqlite db.
    """
    type_stmt = "INSERT INTO type (type) VALUES (?)"
    rt_bridge_stmt = "INSERT INTO r_t_bridge (rid, tid) VALUES (?,?)"
    for term in type_list:
        c.execute("SELECT tid FROM type WHERE type=?", (term,))
        is_type_in_db = c.fetchone()
        if is_type_in_db is not None:
            tid = is_type_in_db[0] #assign tid
            #SELECT type.tid, type.type FROM type JOIN r_t_bridge ON type.tid=r_t_bridge.tid WHERE type.tid=3 AND r_t_bridge.rid=1077
            c.execute("SELECT type.tid FROM type JOIN r_t_bridge ON type.tid=r_t_bridge.tid WHERE type.tid=? AND type.rid=?", (tid,rid))
            term_rid_connected = c.fetchone()
            if term_rid_connected is None:
                c.execute(rt_bridge_stmt, (rid, tid))
            else: pass #already exists and linked via the r_t_bridge
        else:
            c.execute(type_stmt, (term,))
            tid = c.lastrowid
            c.execute(rt_bridge_stmt, (rid, tid))
            
def add_alt_title(alt_title_list, rid, c):
    """
    Takes a alternate title list & resource id and adds it to the database.
    """
    alt_title_stmt = "INSERT INTO alternate_title (title, rid) VALUES (?,?)"
    for term in alt_title_list:
        c.execute(alt_title_stmt, (term, rid))

def write_to_atom():
    """
    Writes out ERF data from local SQLite db into ATOM schema extended with
    Dublin Core. Notifies pubsubhubbub service that a new update is ready
    for consuming.
    """
    detail = 'cmd=detail&'
    atom_xml_write_directory = '/var/www/html/erf-atom/' #'/home/tim/'
    erf_atom_filename = 'erf-atom.xml'
    now = rfc3339(datetime.datetime.now())
    with sqlite3.connect(DB_FILENAME) as conn, open(atom_xml_write_directory+erf_atom_filename, mode='w+') as atom:
        cursor = conn.cursor()
        resids = "SELECT rid FROM resource"
        cursor.execute(resids)
        rids = cursor.fetchall()
        rids = [rid[0] for rid in rids]
        erf_uuid = 'urn:uuid:'+str(uuid.uuid3(uuid.NAMESPACE_DNS, 'library.berkeley.edu/find/types/electronic_resources.html'))
        library_uuid = 'urn:uuid:'+str(uuid.uuid3(uuid.NAMESPACE_DNS, 'http://www.lib.berkeley.edu'))
        xml = xmlwitch.Builder(version='1.0', encoding='utf-8')
        with xml.feed(**{'xmlns':'http://www.w3.org/2005/Atom', 'xmlns:dc':'http://purl.org/dc/terms/'}):
            xml.title('Electronic Resources - UC Berkeley Library')
            xml.updated(now)
            xml.link(None, href='http://doemo.lib.berkeley.edu/erf-atom/erf-atom.xml', rel='self', type='application/atom+xml')
            xml.link(None, rel='hub', href='https://pubsubhubbub.appspot.com')
            xml.id(erf_uuid)
            with xml.author:
                xml.name('UC Berkeley The Library')
                xml.id(library_uuid)
            for rid in rids:
                #rid = str(rid)
                resource_details_stmt = "SELECT title, resource_id, text, description, coverage, licensing, last_modified, url FROM resource WHERE rid = ?"
                subjects = "SELECT term FROM subject JOIN r_s_bridge ON subject.sid = r_s_bridge.sid WHERE rid= ?"
                #alternate_title_stmt = "SELECT title FROM alternate_title WHERE rid = ?"
                types_stmt = "SELECT type FROM type JOIN r_t_bridge ON type.tid = r_t_bridge.tid WHERE rid= ?"
                cursor.execute(resource_details_stmt, (rid,))
                resource_details_db = cursor.fetchone()
                title, resource_id, text, description, coverage, licensing, last_modified, url = resource_details_db
                last_modified += 'T12:00:00-07:00' #2011-09-29T19:20:26-07:00
                cursor.execute(subjects, (rid,))
                subjects_db = cursor.fetchall()
                subjects_db = [subject[0] for subject in subjects_db]
                cursor.execute("SELECT title from alternate_title WHERE rid=?", (rid,))
                alt_title = cursor.fetchall()
                alt_title = [a_title[0] for a_title in alt_title]
                cursor.execute(types_stmt, (rid,))
                types = cursor.fetchall()
                types = [a_type[0] for a_type in types]
                url_id = BASE_URL+detail+'resId='+str(resource_id)
                with xml.entry:
                    xml.title(title)
                    xml.id(url_id) #TODO:need to see if id needs to be more than just url, but some unique id, so date plus url
                    #TODO:add some url self item, preview in google
                    xml.updated(last_modified)
                    xml.dc__description(description)
                    if coverage != "NULL":
                        xml.dc__coverage(coverage)
                    if licensing != "NULL":
                        xml.dc__accessRights(licensing)
                    for subject in subjects_db:
                        #TODO need another test to see if is core & if so, add attribute
                        xml.dc__subject(subject)
                    for a_title in alt_title:
                        xml.dc__alternate(a_title)
                    for type in types:
                        xml.dc__type(type)
                    xml.link(None, href=url)
            print(xml)
            atom.write(str(xml))
            publish_to_hub()

def publish_to_hub():
    """
    Publishes atom feed to pubsubhubbub.appspot.com
    """
    try:
        publish('https://pubsubhubbub.appspot.com', 
                'http://doemo.lib.berkeley.edu/erf-atom/erf-atom.xml')
        print("Publishing the atom feed to pubsubhubbub.appspot.com")
    #TODO:need to capture the http response and print to log
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
            set_all_to_canceled()
            erf_resource_ids = get_resource_ids()
            add_or_update_resources_to_db(erf_resource_ids)

        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--create"):
            create_db_tables()
            add_or_update_resources_to_db(get_resource_ids())
            write_to_atom()
        elif o in ("-a", "--atom"):
            write_to_atom()
        else:
            assert False, "unhandled option"

def usage():
    """
    Prints out usage information to the stout
    """
    erf_scrape_usage = """
    ERF Scrape Usage:

    1. Create a new local erf sqlite database:

    >>>python erf.py --create

    2. Update the local erf data base:

    >>>python erf.py --update

    3. Write an ATOM representation of each resource to file: currently set to write to /var/www/html/erf-atom on doemo.lib

    >>>python erf.py --atom

    """
    print(erf_scrape_usage)

if __name__ == '__main__':
    main()
